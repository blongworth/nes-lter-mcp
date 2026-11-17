"""Efficient MCP access to the NES-LTER API (v3).

The v3 API does the filtering, variable resolution (including harmonizing
underway columns across vessels), station matching and per-cast profile
summaries server-side, so most tools are one or two requests. Surface
summaries, depth binning, underway aggregation, plots and maps are built here.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from mcp.server.fastmcp import FastMCP

API_BASE_URL = os.environ.get("NES_LTER_API_BASE_URL", "https://nes-lter-api.whoi.edu/api/v3").rstrip("/")
CACHE_TTL_SECONDS = float(os.environ.get("NES_LTER_CACHE_TTL", "900"))
MAX_CONCURRENCY = int(os.environ.get("NES_LTER_MAX_CONCURRENCY", "8"))

mcp = FastMCP(
    "nes-lter-mcp",
    instructions=(
        "Use compact summary tools first. Typical workflow: find_cruises, "
        "query_ctd, then request include_points=true only for selected casts. "
        "Use query_underway for meteorological/oceanographic underway data "
        "(wind, temperature, salinity, position, speed, etc.) with cruise-level "
        "aggregation; use list_dataset_rows for raw underway, bottle, nutrient, "
        "chlorophyll, HPLC and event rows. Variable names accept friendly names "
        "(oxygen, temperature), aliases and raw instrument column names; use "
        "resolve_variable to see units and what a name maps to. For plots use "
        "plot_ctd_profiles; for maps use make_station_map or get_cruise_track_data."
    ),
)

DATASETS = {"underway": "underway", "bottles": "bottles", "bottle_summary": "bottles", "nutrients": "nutrients",
            "hplc": "hplc", "chlorophyll": "chlorophyll", "events": "events", "casts": "casts", "ctd": "ctd"}
BOTTLE_SUMMARY_COLUMNS = ("cruise", "cast", "niskin", "time", "latitude", "longitude", "depth_m")

# Friendly underway names -> the API's harmonized underway variables. The API maps each
# vessel's own columns (and units, e.g. knots -> m/s) onto these names.
UNDERWAY_FRIENDLY_SYNONYMS = {
    "wind speed": "true_wind_speed", "windspeed": "true_wind_speed", "wind_speed": "true_wind_speed",
    "wind direction": "true_wind_direction", "winddir": "true_wind_direction", "wind_dir": "true_wind_direction",
    "wind_direction": "true_wind_direction",
    "temperature": "sea_surface_temperature", "temp": "sea_surface_temperature", "sst": "sea_surface_temperature",
    "sea surface temperature": "sea_surface_temperature", "water temperature": "sea_surface_temperature",
    "water_temperature": "sea_surface_temperature",
    "air temperature": "air_temperature", "airtemp": "air_temperature", "air_temp": "air_temperature",
    "salinity": "sea_surface_salinity", "sal": "sea_surface_salinity",
    "pressure": "barometric_pressure", "baro": "barometric_pressure", "barometric pressure": "barometric_pressure",
    "humidity": "relative_humidity", "rh": "relative_humidity", "relative humidity": "relative_humidity",
    "speed": "speed_over_ground", "sog": "speed_over_ground", "speed over ground": "speed_over_ground",
    "course": "course_over_ground", "cog": "course_over_ground", "course over ground": "course_over_ground",
    "lat": "latitude", "lon": "longitude", "lng": "longitude",
}
UNDERWAY_VARIABLE_KIND = {"true_wind_direction": "circular", "heading": "circular", "course_over_ground": "circular"}
# Physically valid ranges. Some source files use fill values (e.g. -99 on the Sharp) that
# the API passes through -- for wind speed already converted from knots, so -99 arrives as
# -50.93 m/s -- so values outside these ranges are dropped and counted in the notes.
UNDERWAY_VALID_RANGES = {
    "true_wind_speed": (0.0, 150.0), "true_wind_direction": (0.0, 360.0), "heading": (0.0, 360.0),
    "course_over_ground": (0.0, 360.0), "relative_humidity": (0.0, 100.0), "speed_over_ground": (0.0, 50.0),
    "latitude": (-90.0, 90.0), "longitude": (-180.0, 180.0),
}

UNDERWAY_PAGE_LIMIT = 50_000  # the API's maximum JSON page size

_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()
_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)


class APIError(ValueError):
    """The API rejected the request (4xx); the message is the API's explanation."""


async def _http_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        async with _client_lock:
            if _client is None or _client.is_closed:
                _client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=15.0), follow_redirects=True)
    return _client


def _url(path: str, params: dict[str, Any] | None = None) -> str:
    query = "&".join(f"{k}={quote(str(v), safe=',')}" for k, v in (params or {}).items() if v is not None)
    return f"{API_BASE_URL}{path}" + (f"?{query}" if query else "")


async def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET JSON. 4xx raises APIError with the API's detail; 5xx and network errors are retried."""
    client = await _http_client()
    url = _url(path, params)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            async with _semaphore:
                response = await client.get(url)
            if 400 <= response.status_code < 500:
                try:
                    detail = response.json().get("detail", response.text)
                except ValueError:
                    detail = response.text
                raise APIError(f"{response.status_code}: {detail}")
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"NES-LTER API request failed for {url}: {last_error}") from last_error


def _source(path: str, params: dict[str, Any] | None = None, build_id: str | None = None) -> dict[str, Any]:
    out = {"source_url": _url(path, params), "retrieved_at": datetime.now(timezone.utc).isoformat()}
    if build_id:
        out["build_id"] = build_id
    return out


class _TTLCache:
    def __init__(self) -> None:
        self.values: dict[str, tuple[float, Any]] = {}

    async def get(self, key: str, loader: Any, refresh: bool = False) -> Any:
        now = time.monotonic()
        if not refresh and key in self.values and now - self.values[key][0] < CACHE_TTL_SECONDS:
            return self.values[key][1]
        value = await loader()
        self.values[key] = (now, value)
        return value


_cache = _TTLCache()


async def _cached(path: str, params: dict[str, Any] | None = None, refresh: bool = False) -> Any:
    return await _cache.get(_url(path, params), lambda: _get(path, params), refresh)


async def _data_rows(dataset: str, params: dict[str, Any], refresh: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """All rows of /data/{dataset} for params (following next_cursor), and the last page's meta."""
    path = f"/data/{dataset}"
    body = await _cached(path, params, refresh)
    rows, meta = list(body["data"]), body["meta"]
    while meta.get("next_cursor") and len(rows) < meta.get("row_count", 0):
        body = await _get(path, {**params, "cursor": meta["next_cursor"]})
        rows.extend(body["data"])
        meta = body["meta"]
    return rows, meta


# Fill values used for missing data in source exports (e.g. -99 in Sharp SCS underway, HRS2303).
MISSING_SENTINELS = {-99.0, -999.0, -9999.0}


def _float(value: Any) -> float | None:
    if value in (None, "", "nan", "NaN"):
        return None
    try:
        number = float(value)
        return None if math.isnan(number) or number in MISSING_SENTINELS else number
    except (TypeError, ValueError):
        return None


POINT_COUNT_WARNING_THRESHOLD = 2000


def _truncation_note(shown: int, total: int, param: str = "limit") -> str | None:
    if shown >= total:
        return None
    return f"Showing {shown} of {total} available; increase {param} (or narrow the query) to see more."


def _point_count_note(point_count: int) -> str | None:
    if point_count <= POINT_COUNT_WARNING_THRESHOLD:
        return None
    return (
        f"{point_count} points returned; consider include_points=False, a surface_definition, "
        "or depth_bin_m to reduce response size."
    )


def _validate_surface_definition(value: str) -> str:
    valid = {"none", "shallowest_valid", "mean_0_5m", "mean_0_10m", "mean_0_20m"}
    if value not in valid:
        raise ValueError(f"Unknown surface_definition {value!r}; use {', '.join(sorted(valid))}")
    return value


def _surface_summary(points: list[dict[str, Any]], value_column: str, definition: str) -> dict[str, Any] | None:
    definition = _validate_surface_definition(definition)
    if not points or definition == "none":
        return None
    if definition == "shallowest_valid":
        point = min(points, key=lambda item: item["depth_m"])
        return {"value": point[value_column], "depth_m": point["depth_m"], "sample_count": 1}
    max_depth = int(definition.rsplit("_", 1)[-1].removesuffix("m"))
    selected = [point for point in points if point["depth_m"] <= max_depth]
    if not selected:
        return None
    return {
        "value": sum(point[value_column] for point in selected) / len(selected),
        "depth_m": [min(point["depth_m"] for point in selected), max(point["depth_m"] for point in selected)],
        "sample_count": len(selected),
    }


async def _gather_partial(items: list[Any], coro_factory: Any, label: Any) -> tuple[list[tuple[Any, Any]], list[str]]:
    """Run coro_factory(item) concurrently; one item's failure doesn't drop the rest.

    Returns (successes, errors) where successes pairs each item with its result and
    errors are human-readable "<label>: <exception>" strings for failed items.
    """
    results = await asyncio.gather(*[coro_factory(item) for item in items], return_exceptions=True)
    successes, errors = [], []
    for item, result in zip(items, results):
        if isinstance(result, Exception):
            errors.append(f"{label(item)}: {result}")
        else:
            successes.append((item, result))
    return successes, errors


def _bin_points(points: list[dict[str, Any]], value_column: str, bin_size_m: float | None) -> list[dict[str, Any]]:
    if not bin_size_m:
        return points
    if bin_size_m <= 0:
        raise ValueError("depth_bin_m must be greater than zero")
    bins: dict[int, list[dict[str, Any]]] = {}
    for point in points:
        index = int(point["depth_m"] // bin_size_m)
        bins.setdefault(index, []).append(point)
    return [
        {"depth_m": (index + 0.5) * bin_size_m, value_column: sum(p[value_column] for p in values) / len(values), "sample_count": len(values)}
        for index, values in sorted(bins.items())
    ]


def _circular_mean(degrees: list[float]) -> float:
    sin_sum = sum(math.sin(math.radians(d)) for d in degrees)
    cos_sum = sum(math.cos(math.radians(d)) for d in degrees)
    return math.degrees(math.atan2(sin_sum, cos_sum)) % 360


async def _cruise_names(year: int | None, season: str | None, cruise_type: str | None, refresh: bool = False) -> list[str]:
    params = {"year": year, "season": season, "type": cruise_type}
    body = await _cached("/cruises", params, refresh)
    return [c["cruise"] for c in body["cruises"]]


@mcp.tool()
async def list_stations(refresh: bool = False) -> dict[str, Any]:
    """List standard NES-LTER stations with coordinates and the periods each position applies."""
    body = await _cached("/stations", refresh=refresh)
    return {**_source("/stations"), "count": body["count"], "stations": body["stations"]}


@mcp.tool()
async def find_cruises(year: int | None = None, season: str | None = None, cruise_type: str | None = None, refresh: bool = False) -> dict[str, Any]:
    """Return the full cruise catalog or filter it by year, meteorological season, and type.

    With no filters this returns cruises of all types, with fields cruise, vessel,
    cruise_type, start_time, end_time and per-dataset row counts. Winter is Dec of the
    previous year to Feb. If season is supplied without year, it matches that season
    across all years.
    """
    params = {"year": year, "season": season, "type": cruise_type}
    body = await _cached("/cruises", params, refresh)
    return {"year": year, "season": season, "cruise_type": cruise_type, "count": body["count"],
            "cruises": body["cruises"], **_source("/cruises", params)}


@mcp.tool()
async def list_casts(cruise_name: str, station: str | None = None, max_distance_km: float = 3.0, offset: int = 0, limit: int = 100, refresh: bool = False) -> dict[str, Any]:
    """List casts of a cruise, optionally only those near a standard station."""
    limit = max(1, min(limit, 10_000))
    params = {"cruise": cruise_name, "station": station, "max_distance_km": max_distance_km if station else None,
              "limit": offset + limit}
    body = await _cached("/data/casts", params, refresh)
    rows = body["data"][offset:offset + limit]
    total = body["meta"]["row_count"]
    notes = [note for note in (_truncation_note(offset + len(rows), total, "limit/offset"),) if note]
    return {"cruise": cruise_name, "offset": offset, "limit": limit, "has_more": total > offset + len(rows),
            "count": len(rows), "notes": notes, "casts": rows, **_source("/data/casts", params, body["meta"]["build_id"])}


def _summary_to_profile(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "cruise": row["cruise"], "cast": row["cast"], "time": row["time"], "latitude": row["latitude"],
        "longitude": row["longitude"], "nearest_station": row["nearest_station"], "distance_km": row["distance_km"],
        "variable": row["variable"], "value_column": row["variable"], "units": row["unit"], "point_count": row["n"],
        "depth_range_m": [row["depth_min"], row["depth_max"]], "value_range": [row["min"], row["max"]],
        "mean": row["mean"], "surface_value": row["surface"], "bottom_value": row["bottom"],
    }


async def _cast_points(profile: dict[str, Any], depth_min: float | None, depth_max: float | None) -> list[dict[str, Any]]:
    variable = profile["variable"]
    body = await _get(f"/casts/{quote(str(profile['cruise']), safe='')}/{quote(str(profile['cast']), safe='')}/profile",
                      {"variables": variable, "depth_min": depth_min, "depth_max": depth_max})
    points = []
    for row in body["data"]:
        depth, value = _float(row.get("depth_m")), _float(row.get(variable))
        if depth is not None and value is not None:
            points.append({"depth_m": depth, variable: value, "time": row.get("time")})
    return points


async def _profiles(params: dict[str, Any], variable: str, include_points: bool, limit: int, surface_definition: str = "none", depth_bin_m: float | None = None) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    """(per-cast summaries for one variable, response meta, per-cast errors).

    Points are fetched only when needed: for include_points, a surface_definition, or
    depth binning. A cast whose points fail to load is skipped and reported in errors.
    """
    _validate_surface_definition(surface_definition)
    if depth_bin_m is not None and depth_bin_m <= 0:
        raise ValueError("depth_bin_m must be greater than zero")
    summary = await _get("/profiles", {**params, "variables": variable, "summary": "true"})
    profiles = [_summary_to_profile(r) for r in summary["data"]][:limit]
    for p in profiles:
        p["surface_summary"] = None
        p["points"] = None
    errors: list[str] = []
    if profiles and (include_points or surface_definition != "none" or depth_bin_m):
        results, errors = await _gather_partial(
            profiles,
            lambda p: _cast_points(p, params.get("depth_min"), params.get("depth_max")),
            lambda p: f"{p['cruise']} cast {p['cast']}",
        )
        profiles = []
        for p, points in results:
            points = _bin_points(points, p["variable"], depth_bin_m)
            p["surface_summary"] = _surface_summary(points, p["variable"], surface_definition)
            if include_points:
                p["points"], p["point_count"] = points, len(points)
            profiles.append(p)
    return profiles, summary["meta"] | {"total": len(summary["data"])}, errors


@mcp.tool()
async def get_cast_profile(cruise_name: str, cast_number: str, variable: str = "oxygen", include_points: bool = True, depth_min: float | None = None, depth_max: float | None = None, surface_definition: str = "none", depth_bin_m: float | None = None) -> dict[str, Any]:
    """Get one CTD profile: units, depth and value ranges, surface/bottom values, and optionally the points.

    surface_definition: none, shallowest_valid, mean_0_5m, mean_0_10m or mean_0_20m.
    depth_bin_m averages points into depth bins of that size.
    """
    params = {"cruise": cruise_name, "cast": cast_number, "depth_min": depth_min, "depth_max": depth_max}
    profiles, _, errors = await _profiles(params, variable, include_points, 1, surface_definition, depth_bin_m)
    if errors:
        raise RuntimeError(errors[0])
    if not profiles:
        return {"cruise": cruise_name, "cast": cast_number, "variable_requested": variable, "point_count": 0,
                "points": [] if include_points else None, **_source("/profiles", params)}
    return {"variable_requested": variable, "surface_definition": surface_definition, "depth_bin_m": depth_bin_m,
            **profiles[0], **_source("/profiles", params)}


async def _query_ctd(
    station: str | None = None,
    variable: str = "oxygen",
    year: int | None = None,
    season: str | None = None,
    cruise_name: str | None = None,
    cruise_type: str | None = "NES-LTER",
    max_distance_km: float = 3.0,
    include_points: bool = False,
    depth_min: float | None = None,
    depth_max: float | None = None,
    limit: int = 100,
    refresh: bool = False,
    surface_definition: str = "none",
    depth_bin_m: float | None = None,
) -> dict[str, Any]:
    if not cruise_name and year is None:
        raise ValueError("Provide cruise_name or year")
    _validate_surface_definition(surface_definition)
    base = {"station": station.upper() if station else None, "variable": variable, "year": year, "season": season,
            "surface_definition": surface_definition, "depth_bin_m": depth_bin_m}
    cruises = [cruise_name] if cruise_name else await _cruise_names(year, season, cruise_type, refresh)
    if not cruises:
        return {**base, "profile_count": 0, "point_count": 0, "truncated": False,
                "notes": ["No cruises matched the given filters."], "errors": [], "profiles": [],
                **_source("/cruises", {"year": year, "season": season, "type": cruise_type})}
    params = {"cruise": ",".join(cruises), "station": station, "max_distance_km": max_distance_km if station else None,
              "year": year, "season": season, "depth_min": depth_min, "depth_max": depth_max}
    limit = max(1, min(limit, 1000))
    profiles, meta, errors = await _profiles(params, variable, include_points, limit, surface_definition, depth_bin_m)
    point_count = sum(len(p["points"] or []) for p in profiles) if include_points else sum(p.get("point_count") or 0 for p in profiles)
    notes = [note for note in (
        _truncation_note(min(limit, meta["total"]), meta["total"], "limit"),
        _point_count_note(point_count) if include_points else None,
        f"{len(errors)} cast(s) failed to load and were skipped; see errors." if errors else None,
    ) if note]
    return {**base, "profile_count": len(profiles), "point_count": point_count,
            "truncated": meta["total"] > limit or bool(errors), "notes": notes, "errors": errors, "profiles": profiles,
            **_source("/profiles", {**params, "variables": variable, "summary": "true"}, meta.get("build_id"))}


@mcp.tool()
async def query_ctd(station: str | None = None, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = False, depth_min: float | None = None, depth_max: float | None = None, limit: int = 100, refresh: bool = False, surface_definition: str = "none", depth_bin_m: float | None = None) -> dict[str, Any]:
    """Batch CTD query across cruises. Returns compact per-cast summaries unless include_points is true.

    surface_definition (none, shallowest_valid, mean_0_5m, mean_0_10m, mean_0_20m) adds a
    per-cast surface_summary; depth_bin_m averages points into depth bins.
    """
    return await _query_ctd(station, variable, year, season, cruise_name, cruise_type, max_distance_km, include_points, depth_min, depth_max, limit, refresh, surface_definition, depth_bin_m)


@mcp.tool()
async def get_station_profile(station: str, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = True, limit: int = 100) -> dict[str, Any]:
    """Compatibility wrapper for station profile lookup; use query_ctd for compact results."""
    return await query_ctd(station, variable, year, season, cruise_name, cruise_type, max_distance_km, include_points, limit=limit)


def _plot_rows(profiles: list[dict[str, Any]], aggregation: str, surface_definition: str) -> list[dict[str, Any]]:
    valid_aggregations = {"none", "cruise_mean", "period_mean"}
    if aggregation not in valid_aggregations:
        raise ValueError(f"Unknown aggregation {aggregation!r}; use {', '.join(sorted(valid_aggregations))}")
    rows: list[dict[str, Any]] = []
    for profile in profiles:
        value_column = profile.get("value_column")
        points = profile.get("points") or []
        if not value_column:
            continue
        surface = _surface_summary(points, value_column, surface_definition)
        if surface is not None:
            depth = surface["depth_m"]
            depth = sum(depth) / len(depth) if isinstance(depth, list) else depth
            rows.append({"cruise": profile["cruise"], "cast": profile["cast"], "depth_m": depth, "value": surface["value"], "sample_count": surface["sample_count"]})
        elif surface_definition == "none":
            rows.extend({"cruise": profile["cruise"], "cast": profile["cast"], "depth_m": point["depth_m"], "value": point[value_column], "sample_count": point.get("sample_count", 1)} for point in points)
    if aggregation == "none":
        return sorted(rows, key=lambda row: (str(row["cruise"]), str(row["cast"]), row["depth_m"]))
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["cruise"],) if aggregation == "cruise_mean" and surface_definition != "none" else ((row["cruise"], row["depth_m"]) if aggregation == "cruise_mean" else (row["depth_m"],) if surface_definition == "none" else ("period",))
        groups.setdefault(key, []).append(row)
    output = []
    for key, values in groups.items():
        output.append({"cruise": key[0] if aggregation == "cruise_mean" else "period_mean", "depth_m": values[0]["depth_m"], "value": sum(item["value"] for item in values) / len(values), "sample_count": len(values)})
    return sorted(output, key=lambda row: (str(row["cruise"]), row["depth_m"]))


@mcp.tool()
async def plot_ctd_profiles(
    station: str | None,
    variable: str = "oxygen",
    variables: list[str] | None = None,
    year_start: int = 2017,
    year_end: int | None = None,
    season: str | None = None,
    cruise_type: str | None = "NES-LTER",
    aggregation: str = "none",
    surface_definition: str = "none",
    max_distance_km: float = 3.0,
    depth_min: float | None = None,
    depth_max: float | None = None,
    depth_bin_m: float | None = None,
    limit_per_year: int = 100,
    width: int = 450,
    height: int = 325,
) -> dict[str, Any]:
    """Return one or more plotting-ready CTD variables and a Vega-Lite profile specification."""
    year_end = year_start if year_end is None else year_end
    if year_end < year_start:
        raise ValueError("year_end must be greater than or equal to year_start")
    if width < 300 or height < 300:
        raise ValueError("width and height must both be at least 300 pixels")
    _validate_surface_definition(surface_definition)
    requested_variables = list(dict.fromkeys(variables or [variable]))
    results = await asyncio.gather(*[_query_ctd(station, requested_variable, year, season, None, cruise_type, max_distance_km, True, depth_min, depth_max, limit_per_year, False, surface_definition, depth_bin_m) for requested_variable in requested_variables for year in range(year_start, year_end + 1)])
    rows = []
    for requested_variable, result_year, result in zip(
        (requested_variable for requested_variable in requested_variables for _ in range(year_start, year_end + 1)),
        (result_year for _ in requested_variables for result_year in range(year_start, year_end + 1)),
        results,
    ):
        variable_rows = _plot_rows(result["profiles"], aggregation, surface_definition)
        for row in variable_rows:
            row["variable"] = requested_variable
            row["cruise_year"] = result_year
            row["cruise_label"] = f"{row['cruise']} ({result_year})"
        rows.extend(variable_rows)
    # Units come from the API; only report one when a single variable has a single unit.
    found_units = {p.get("units") for result in results for p in result["profiles"]} - {None}
    units = next(iter(found_units)) if len(requested_variables) == 1 and len(found_units) == 1 else None
    cruise_order = list(dict.fromkeys(
        row["cruise_label"]
        for row in sorted(rows, key=lambda item: (item.get("cruise_year", 0), str(item.get("cruise", ""))))
    ))
    shared_encoding = {
        "y": {"field": "depth_m", "type": "quantitative", "title": "Depth (m)", "scale": {"reverse": True}},
        "color": {"field": "cruise_label", "type": "nominal", "title": "Cruise (year)", "sort": cruise_order},
        "detail": {"field": "cast", "type": "nominal"},
        "order": {"field": "depth_m", "type": "quantitative", "sort": "ascending"},
        "tooltip": [{"field": "cruise_label", "title": "Cruise"}, {"field": "cruise_year", "title": "Year"}, {"field": "cast"}, {"field": "variable"}, {"field": "depth_m"}, {"field": "value"}, {"field": "sample_count"}],
    }
    layers = []
    colors = ["#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00", "#56B4E9"]
    for index, requested_variable in enumerate(requested_variables):
        layers.append({
            "transform": [{"filter": {"field": "variable", "equal": requested_variable}}],
            "mark": {"type": "line"},
            "encoding": {
                "x": {"field": "value", "type": "quantitative", "title": requested_variable},
                **shared_encoding,
                **({"color": {"value": colors[index % len(colors)]}} if len(requested_variables) > 1 else {}),
            },
        })
    notes = list(dict.fromkeys(note for result in results for note in result.get("notes", [])))
    errors = list(dict.fromkeys(error for result in results for error in result.get("errors", [])))
    return {
        "chart_type": "vertical_profile",
        "station": station.upper() if station else None,
        "variable": requested_variables[0] if len(requested_variables) == 1 else None,
        "variables": requested_variables,
        "units": units,
        "aggregation": aggregation,
        "surface_definition": surface_definition,
        "row_count": len(rows),
        "notes": notes,
        "errors": errors,
        "data": rows,
        "vega_lite": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": f"{', '.join(requested_variables)} CTD profiles at {station or 'NES-LTER stations'}",
            "width": width,
            "height": height,
            "data": {"values": rows},
            "layer": layers,
            "resolve": {"scale": {"x": "independent"}},
        },
        "provenance": {"years": [year_start, year_end], "source": f"{API_BASE_URL}/profiles"},
    }


@mcp.tool()
async def list_dataset_rows(dataset: str, cruise_name: str, station: str | None = None, variable: str | None = None, start_time: str | None = None, end_time: str | None = None, offset: int = 0, limit: int = 500, refresh: bool = False) -> dict[str, Any]:
    """Fetch rows of underway, bottles, bottle_summary, nutrients, HPLC, chlorophyll, events, casts or ctd for a cruise.

    `variable` selects measurement columns (comma-separated names or aliases); key columns are always included.
    `station` keeps rows nearest that station (within 3 km); not every dataset supports it.
    """
    name = dataset.lower()
    if name == "event_history":
        raise ValueError("event_history is not available from API v3: event corrections are made in files, "
                         "so there is no edit history to serve")
    if name not in DATASETS:
        raise ValueError(f"Unknown dataset {dataset!r}; choose from {', '.join(sorted(DATASETS))}")
    limit = max(1, min(limit, 10_000))
    params = {"cruise": cruise_name, "station": station, "variables": variable, "start": start_time, "end": end_time,
              "limit": offset + limit}
    body = await _cached(f"/data/{DATASETS[name]}", params, refresh)
    rows = body["data"][offset:offset + limit]
    if name == "bottle_summary":
        rows = [{k: r.get(k) for k in BOTTLE_SUMMARY_COLUMNS} for r in rows]
    units = {c["name"]: c["units"] for c in body["meta"]["columns"] if c.get("units")}
    total = body["meta"]["row_count"]
    notes = [note for note in (_truncation_note(offset + len(rows), total, "limit/offset"),) if note]
    return {"dataset": name, "cruise": cruise_name, "offset": offset, "limit": limit,
            "has_more": total > offset + len(rows), "count": len(rows), "notes": notes, "units": units,
            "rows": rows, **_source(f"/data/{DATASETS[name]}", params, body["meta"]["build_id"])}


async def _underway_variable(variable: str) -> dict[str, Any]:
    """Catalog entry of the harmonized underway variable a friendly name, alias or raw column maps to."""
    name = UNDERWAY_FRIENDLY_SYNONYMS.get(variable.lower(), variable)
    body = await _cached(f"/variables/{quote(name, safe='')}")
    match = next((v for v in body["variables"] if "underway" in v.get("datasets", [])), None)
    if match is None:
        raise ValueError(f"{variable!r} is not an underway variable; try e.g. wind speed, wind direction, "
                         "temperature, salinity, air temperature, pressure, humidity, heading or speed")
    return match


async def _underway_cruise_values(cruise_name: str, variable: str, start_time: str | None, end_time: str | None, refresh: bool) -> dict[str, Any]:
    # The API pages at 1,000 rows by default; a cruise has up to ~30k underway rows, so ask for
    # them in one page (_data_rows still follows next_cursor if the API caps it).
    params = {"cruise": cruise_name, "variables": variable, "start": start_time, "end": end_time, "limit": UNDERWAY_PAGE_LIMIT}
    (rows, meta), cruise = await asyncio.gather(
        _data_rows("underway", params, refresh), _cached(f"/cruises/{quote(cruise_name.lower(), safe='')}"))
    unit = next((c.get("units") for c in meta["columns"] if c["name"] == variable), None)
    low, high = UNDERWAY_VALID_RANGES.get(variable, (-math.inf, math.inf))
    values, dropped = [], 0
    for row in rows:
        raw = row.get(variable)
        value = _float(raw)
        if value is None or not low <= value <= high:
            dropped += raw not in (None, "")
            continue
        values.append({"time": row.get("time"), "value": value,
                       "latitude": _float(row.get("latitude")), "longitude": _float(row.get("longitude"))})
    return {"cruise": cruise_name, "vessel": cruise.get("vessel"), "variable": variable, "unit": unit,
            "kind": UNDERWAY_VARIABLE_KIND.get(variable, "linear"), "values": values, "dropped": dropped,
            "build_id": meta.get("build_id")}


@mcp.tool()
async def query_underway(
    variable: str,
    cruise_name: str | None = None,
    year: int | None = None,
    season: str | None = None,
    cruise_type: str | None = "NES-LTER",
    aggregation: str = "cruise_mean",
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 500,
    refresh: bool = False,
) -> dict[str, Any]:
    """Query underway meteorological/oceanographic data, aggregated across one or many cruises.

    Accepts friendly names (wind speed, wind direction, water temperature, air temperature,
    salinity, pressure, humidity, heading, course over ground, speed over ground, latitude,
    longitude, plus synonyms like temp, sal, sog, cog), the API's variable names, or a
    vessel's raw column name. The API maps each vessel's columns onto common variables and
    units (wind speed in m/s). Fill values (-99) and physically impossible values are
    dropped and counted in the notes. Directional variables use a circular mean.

    aggregation: "cruise_mean" (default, one row per cruise), "period_mean" (one
    overall value, equal-weighted across matched cruises), or "none" (per-reading rows
    with position, up to limit, for e.g. plotting a track colored by variable).
    """
    valid_aggregations = {"none", "cruise_mean", "period_mean"}
    if aggregation not in valid_aggregations:
        raise ValueError(f"Unknown aggregation {aggregation!r}; use {', '.join(sorted(valid_aggregations))}")
    if not cruise_name and year is None and season is None:
        raise ValueError("Provide cruise_name, year, or season")
    catalog = await _underway_variable(variable)
    api_variable = catalog["name"]
    # Season alone matches that season across all years, as in find_cruises.
    cruises = [cruise_name] if cruise_name else await _cruise_names(year, season, cruise_type, refresh)
    params = {"cruise": ",".join(cruises), "variables": api_variable, "start": start_time, "end": end_time}
    if not cruises:
        return {"variable": api_variable, "requested": variable, "unit": catalog.get("units"), "aggregation": aggregation,
                "cruise_count": 0, "notes": ["No cruises matched the given filters."], "errors": [], "data": [],
                **_source("/cruises", {"year": year, "season": season, "type": cruise_type})}

    results, errors = await _gather_partial(
        cruises,
        lambda c: _underway_cruise_values(c, api_variable, start_time, end_time, refresh),
        lambda c: c,
    )
    cruise_results = [result for _, result in results]
    unit = next((r["unit"] for r in cruise_results if r["unit"]), catalog.get("units"))
    is_circular = UNDERWAY_VARIABLE_KIND.get(api_variable) == "circular"
    notes = [
        f"{r['cruise']}: dropped {r['dropped']} fill or out-of-range value(s) (e.g. -99)."
        for r in cruise_results if r["dropped"]
    ]

    if aggregation == "none":
        flat = [
            {"cruise": r["cruise"], "vessel": r["vessel"], "time": v["time"], "value": v["value"], "latitude": v["latitude"], "longitude": v["longitude"]}
            for r in cruise_results for v in r["values"]
        ]
        data = flat[:max(1, min(limit, 10_000))]
        notes.extend(note for note in (_truncation_note(len(data), len(flat), "limit"),) if note)
    elif aggregation == "cruise_mean":
        data = []
        for r in cruise_results:
            if not r["values"]:
                continue
            vals = [v["value"] for v in r["values"]]
            mean_value = _circular_mean(vals) if is_circular else sum(vals) / len(vals)
            data.append({"cruise": r["cruise"], "vessel": r["vessel"], "value": mean_value, "sample_count": len(vals)})
    else:
        cruise_means = []
        for r in cruise_results:
            if r["values"]:
                vals = [v["value"] for v in r["values"]]
                cruise_means.append(_circular_mean(vals) if is_circular else sum(vals) / len(vals))
        if cruise_means:
            overall = _circular_mean(cruise_means) if is_circular else sum(cruise_means) / len(cruise_means)
            data = [{"cruise": "period_mean", "value": overall, "cruise_count": len(cruise_means)}]
        else:
            data = []

    if errors:
        notes.append(f"{len(errors)} cruise(s) failed to load and were skipped; see errors.")

    build_id = next((r["build_id"] for r in cruise_results if r["build_id"]), None)
    return {
        "variable": api_variable,
        "requested": variable,
        "unit": unit,
        "kind": "circular" if is_circular else "linear",
        "aggregation": aggregation,
        "cruise_count": len(cruise_results),
        "notes": notes,
        "errors": errors,
        "data": data,
        **_source("/data/underway", params, build_id),
    }


@mcp.tool()
async def get_cruise_track(cruise_name: str) -> dict[str, Any]:
    """URLs for the cruise's underway track (time, latitude, longitude, ...) as CSV or Parquet."""
    c = quote(cruise_name.lower(), safe="")
    path = f"/files/{c}/underway.csv"
    return {"cruise": cruise_name, "track_url": _url(path), "track_parquet_url": _url(f"/files/{c}/underway.parquet"),
            **_source(path)}


@mcp.tool()
async def get_cruise_track_data(cruise_name: str, include_points: bool = True) -> dict[str, Any]:
    """Build GeoJSON cruise track data from the cruise's cast positions."""
    params = {"cruise": cruise_name}
    rows, meta = await _data_rows("casts", params)
    points = []
    for row in sorted(rows, key=lambda r: str(r.get("time") or "")):
        latitude, longitude = _float(row.get("latitude")), _float(row.get("longitude"))
        if latitude is None or longitude is None:
            continue
        points.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
            "properties": {"cruise": cruise_name, "cast": row.get("cast"), "time": row.get("time"),
                           "nearest_station": row.get("nearest_station"), "distance_km": _float(row.get("distance_km"))},
        })
    coordinates = [feature["geometry"]["coordinates"] for feature in points]
    features = []
    if len(coordinates) >= 2:
        features.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": coordinates}, "properties": {"cruise": cruise_name, "feature": "cast_track"}})
    if include_points:
        features.extend(points)
    return {"cruise": cruise_name, "format": "GeoJSON", "feature_count": len(features),
            "geojson": {"type": "FeatureCollection", "features": features}, "point_count": len(points),
            **_source("/data/casts", params, meta.get("build_id"))}


@mcp.tool()
async def make_station_map(stations: list[str] | None = None, cruises: list[str] | None = None, include_tracks: bool = True) -> dict[str, Any]:
    """Return station GeoJSON (current positions) plus an optional Vega-Lite point-map specification."""
    station_catalog = (await list_stations())["stations"]
    wanted = {station.upper() for station in stations} if stations else None
    station_features = []
    map_rows = []
    for row in station_catalog:
        name = row.get("station")
        latitude, longitude = _float(row.get("latitude")), _float(row.get("longitude"))
        # Stations that moved have one row per position; map the current one.
        if row.get("valid_to") or not name or latitude is None or longitude is None or (wanted and str(name).upper() not in wanted):
            continue
        properties = {"station": name, "depth_m": _float(row.get("depth_m")), "comment": row.get("comment")}
        station_features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [longitude, latitude]}, "properties": properties})
        map_rows.append({"station": name, "latitude": latitude, "longitude": longitude, "depth_m": properties["depth_m"]})
    track_results, track_errors = await _gather_partial(cruises or [], get_cruise_track_data, lambda cruise: cruise) if include_tracks and cruises else ([], [])
    track_features = [feature for _, result in track_results for feature in result["geojson"]["features"]]
    notes = [f"{len(track_errors)} cruise track(s) failed to load and were skipped; see errors."] if track_errors else []
    return {
        "format": "GeoJSON",
        "geojson": {"type": "FeatureCollection", "features": track_features + station_features},
        "station_count": len(station_features),
        "cruise_count": len(cruises or []),
        "notes": notes,
        "errors": track_errors,
        "vega_lite": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": "NES-LTER standard stations",
            "projection": {"type": "mercator"},
            "data": {"values": map_rows},
            "mark": {"type": "circle", "size": 100},
            "encoding": {
                "longitude": {"field": "longitude", "type": "quantitative"},
                "latitude": {"field": "latitude", "type": "quantitative"},
                "color": {"field": "station", "type": "nominal", "legend": None},
                "tooltip": [{"field": "station"}, {"field": "latitude"}, {"field": "longitude"}, {"field": "depth_m"}],
            },
        },
        **_source("/stations"),
    }


@mcp.tool()
async def get_dataset_schema(dataset: str, cruise_name: str) -> dict[str, Any]:
    """Columns (with units) of a dataset, and how many rows the cruise has."""
    name = DATASETS.get(dataset.lower(), dataset.lower())
    datasets = {d["name"]: d for d in (await _cached("/datasets"))["datasets"]}
    if name not in datasets:
        raise ValueError(f"Unknown dataset {dataset!r}; choose from {', '.join(sorted(datasets))}")
    cruise = await _cached(f"/cruises/{quote(cruise_name.lower(), safe='')}")
    d = datasets[name]
    return {"dataset": name, "cruise": cruise_name, "rows_for_cruise": cruise["datasets"].get(name, 0),
            "keys": d["keys"], "filters": d["filters"], "columns": d["columns"], **_source("/datasets")}


@mcp.tool()
async def resolve_variable(name: str, cruise_name: str | None = None, dataset: str = "ctd") -> dict[str, Any]:
    """Resolve a friendly variable name or alias to the API's variable, with units and which cruises have data."""
    lookup = UNDERWAY_FRIENDLY_SYNONYMS.get(name.lower(), name) if dataset == "underway" else name
    try:
        body = await _cached(f"/variables/{quote(lookup, safe='')}")
    except APIError as e:
        return {"requested": name, "matches": [], "error": str(e), "dataset": dataset}
    matches = [v for v in body["variables"] if dataset is None or dataset in v["datasets"]] or body["variables"]
    out = []
    for v in matches:
        cruises = v.get("cruises", {}).get(dataset, [])
        rec = {k: v.get(k) for k in ("name", "units", "description", "aliases", "datasets")}
        rec["has_data_for_cruise"] = (cruise_name.lower() in cruises) if cruise_name else None
        out.append(rec)
    return {"requested": name, "dataset": dataset, "matches": out, "candidates": [m["name"] for m in out]}


@mcp.resource("nes-lter://stations")
async def stations_resource() -> str:
    """Station catalog as JSON for model reference."""
    return json.dumps(await list_stations())


@mcp.resource("nes-lter://variable-aliases")
async def variable_aliases_resource() -> str:
    """CTD and underway variables: name -> units and accepted aliases, plus friendly underway synonyms."""
    ctd, underway = await asyncio.gather(_cached("/variables", {"dataset": "ctd"}), _cached("/variables", {"dataset": "underway"}))
    harmonized = set(UNDERWAY_FRIENDLY_SYNONYMS.values()) | set(UNDERWAY_VALID_RANGES)
    return json.dumps({
        "ctd": {v["name"]: {"units": v["units"], "aliases": v["aliases"]} for v in ctd["variables"]},
        "underway": {v["name"]: {"units": v["units"], "aliases": v["aliases"]} for v in underway["variables"] if v["name"] in harmonized},
        "underway_synonyms": UNDERWAY_FRIENDLY_SYNONYMS,
    })


@mcp.resource("nes-lter://vega-lite-viewer")
async def vega_lite_viewer_resource() -> str:
    """Reusable browser custom element for rendering Vega-Lite results."""
    return (Path(__file__).resolve().parent / "web" / "nes-lter-vega-plot.js").read_text()


@mcp.prompt()
def station_profile_prompt(station: str, variable: str, season: str, year: int) -> str:
    return f"Use query_ctd(station={station!r}, variable={variable!r}, season={season!r}, year={year}, include_points=false), then summarize the selected profiles and units."


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
