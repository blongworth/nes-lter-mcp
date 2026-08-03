"""Efficient MCP access to the NES-LTER API.

The tools intentionally return compact, JSON-serializable records.  Large
profiles and cruise tables can be filtered, limited, or requested in batches.
"""

from __future__ import annotations

import asyncio
import csv
import io
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from mcp.server.fastmcp import FastMCP

API_BASE_URL = os.environ.get("NES_LTER_API_BASE_URL", "https://nes-lter-api.whoi.edu").rstrip("/")
CACHE_TTL_SECONDS = float(os.environ.get("NES_LTER_CACHE_TTL", "900"))
MAX_CONCURRENCY = int(os.environ.get("NES_LTER_MAX_CONCURRENCY", "8"))

mcp = FastMCP(
    "nes-lter-mcp",
    instructions=(
        "Use compact summary tools first. Typical workflow: find_cruises, "
        "query_ctd, then request include_points=true only for selected casts. "
        "Use list_dataset_rows for underway, bottle, nutrient, chlorophyll, "
        "HPLC, event, and event-history data. For plots use plot_ctd_profiles; "
        "for maps use make_station_map or get_cruise_track_data."
    ),
)

SEASONS = {"winter": {12, 1, 2}, "spring": {3, 4, 5}, "summer": {6, 7, 8}, "fall": {9, 10, 11}, "autumn": {9, 10, 11}}
VARIABLE_ALIASES = {
    "oxygen": ["sbox0mm_kg", "sbox1mm_kg"], "dissolved oxygen": ["sbox0mm_kg", "sbox1mm_kg"], "o2": ["sbox0mm_kg", "sbox1mm_kg"],
    "temperature": ["t090c", "t190c", "potemp090c", "potemp190c"], "temp": ["t090c", "t190c", "potemp090c", "potemp190c"],
    "salinity": ["sal00", "sal11"], "sal": ["sal00", "sal11"], "density": ["sigma_00", "sigma_11"],
    "fluorescence": ["fleco_afl"], "par": ["par"], "transmissometer": ["cstartr0", "cstarat0"],
}
DATASET_PATHS = {
    "underway": "/api/underway/{cruise}.csv", "bottles": "/api/ctd/bottles/{cruise}.csv",
    "bottle_summary": "/api/ctd/bottle_summary/{cruise}.csv", "nutrients": "/api/nut/{cruise}.csv",
    "hplc": "/api/hplc/{cruise}.csv", "chlorophyll": "/api/chl/{cruise}.csv",
    "events": "/api/events/{cruise}.csv", "event_history": "/api/events/history/{cruise}",
}

_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()
_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)


async def _http_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        async with _client_lock:
            if _client is None or _client.is_closed:
                _client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=15.0), follow_redirects=True)
    return _client


async def _get(path: str) -> httpx.Response:
    client = await _http_client()
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            async with _semaphore:
                response = await client.get(f"{API_BASE_URL}{path}")
            if response.status_code in {429, 500, 502, 503, 504}:
                response.raise_for_status()
            response.raise_for_status()
            return response
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"NES-LTER API request failed for {path}: {last_error}") from last_error


def _source(path: str) -> dict[str, str]:
    return {"source_url": f"{API_BASE_URL}{path}", "retrieved_at": datetime.now(timezone.utc).isoformat()}


async def _json(path: str) -> Any:
    return (await _get(path)).json()


async def _json_or_text(path: str) -> Any:
    response = await _get(path)
    try:
        return response.json()
    except ValueError:
        return {"raw": response.text, "columns": [line.strip() for line in response.text.splitlines() if line.strip()]}


async def _csv(path: str) -> list[dict[str, str]]:
    response = await _get(path)
    return list(csv.DictReader(io.StringIO(response.text)))


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


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _in_season(dt: datetime, year: int, season: str | None) -> bool:
    if not season:
        return dt.year == year
    months = SEASONS.get(season.lower())
    if months is None:
        raise ValueError(f"Unknown season {season!r}; use winter, spring, summer, or fall")
    if season.lower() == "winter":
        return (dt.year == year and dt.month in {1, 2}) or (dt.year == year - 1 and dt.month == 12)
    return dt.year == year and dt.month in months


def _float(value: Any) -> float | None:
    if value in (None, "", "nan", "NaN"):
        return None
    try:
        number = float(value)
        return None if math.isnan(number) else number
    except (TypeError, ValueError):
        return None


def _paginate(rows: list[dict[str, Any]], offset: int, limit: int) -> tuple[list[dict[str, Any]], bool]:
    limit = max(1, min(limit, 10_000))
    selected = rows[max(0, offset):max(0, offset) + limit]
    return selected, len(rows) > max(0, offset) + len(selected)


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


async def _all_cruises(refresh: bool = False) -> tuple[dict[str, Any], ...]:
    value = await _cache.get("cruises", lambda: _json("/api/ctd/cruises/all"), refresh)
    return tuple(c for c in value if c)


async def _metadata(cruise_name: str, refresh: bool = False) -> list[dict[str, str]]:
    name = quote(cruise_name, safe="")
    return await _cache.get(f"metadata:{name}", lambda: _csv(f"/api/ctd/metadata/{name}.csv"), refresh)


@mcp.tool()
async def list_stations(refresh: bool = False) -> dict[str, Any]:
    """List standard NES-LTER stations with coordinates and station metadata."""
    path = "/api/stations/file.csv"
    rows = await _cache.get("stations", lambda: _csv(path), refresh)
    return {**_source(path), "count": len(rows), "stations": rows}


def _cruise_matches(cruise: dict[str, Any], year: int | None, season: str | None, cruise_type: str | None) -> bool:
    start = _parse_time(cruise.get("start_time"))
    if year is not None:
        if start is None or not _in_season(start, year, season):
            return False
    elif season:
        months = SEASONS.get(season.lower())
        if months is None:
            raise ValueError(f"Unknown season {season!r}; use winter, spring, summer, or fall")
        if start is None or start.month not in months:
            return False
    return not cruise_type or cruise.get("type", "").lower() == cruise_type.lower()


@mcp.tool()
async def find_cruises(year: int | None = None, season: str | None = None, cruise_type: str | None = None, refresh: bool = False) -> dict[str, Any]:
    """Return the full CTD cruise catalog or filter it by year, season, and type.

    With no filters this mirrors /api/ctd/cruises/all, including cruises of all
    types and the API fields name, vessel_name, type, start_time, and end_time.
    If season is supplied without year, it matches that season across all years.
    """
    cruises = [cruise for cruise in await _all_cruises(refresh) if _cruise_matches(cruise, year, season, cruise_type)]
    return {"year": year, "season": season, "cruise_type": cruise_type, "count": len(cruises), "cruises": cruises, **_source("/api/ctd/cruises/all")}


async def _matching_casts(cruise_name: str, station: str | None, max_distance_km: float) -> list[dict[str, Any]]:
    rows = await _metadata(cruise_name)
    station_upper = station.upper() if station else None
    output = []
    for row in rows:
        distance = _float(row.get("distance_km"))
        nearest = (row.get("nearest_station") or "").upper()
        if station_upper and (nearest != station_upper or (distance is not None and distance > max_distance_km)):
            continue
        output.append({**row, "distance_km": distance})
    return output


@mcp.tool()
async def list_casts(cruise_name: str, station: str | None = None, max_distance_km: float = 3.0, offset: int = 0, limit: int = 100, refresh: bool = False) -> dict[str, Any]:
    """List casts, optionally restricted to the nearest standard station."""
    rows = await _matching_casts(cruise_name, station, max_distance_km)
    selected, has_more = _paginate(rows, offset, limit)
    return {"cruise": cruise_name, "offset": offset, "limit": limit, "has_more": has_more, "count": len(selected), "casts": selected, **_source(f"/api/ctd/metadata/{quote(cruise_name, safe='')}.csv")}


async def _cast_profile(
    cruise_name: str,
    cast_number: str,
    variable: str,
    include_points: bool = True,
    depth_min: float | None = None,
    depth_max: float | None = None,
    surface_definition: str = "none",
    depth_bin_m: float | None = None,
) -> dict[str, Any]:
    name, cast = quote(cruise_name, safe=""), quote(cast_number, safe="")
    path = f"/api/ctd/cast/{name}/{cast}.csv"
    rows = await _csv(path)
    if not rows:
        return {"cruise": cruise_name, "cast": cast_number, "point_count": 0, "points": [], **_source(path)}
    headers = list(rows[0])
    candidates = VARIABLE_ALIASES.get(variable.lower(), [variable])
    value_column = next((column for column in candidates if column in headers), None)
    if value_column is None:
        raise ValueError(f"Variable {variable!r} was not found; available columns include: {', '.join(headers[:80])}")
    depth_column = "depsm" if "depsm" in headers else "prdm"
    points = []
    for row in rows:
        depth, value = _float(row.get(depth_column)), _float(row.get(value_column))
        if depth is not None and value is not None and (depth_min is None or depth >= depth_min) and (depth_max is None or depth <= depth_max):
            points.append({"depth_m": depth, value_column: value, "date": row.get("date")})
    plot_points = _bin_points(points, value_column, depth_bin_m)
    depths, values = [p["depth_m"] for p in plot_points], [p[value_column] for p in plot_points]
    result = {"cruise": cruise_name, "cast": cast_number, "variable_requested": variable, "value_column": value_column, "depth_column": depth_column, "units_note": "Confirm units from the cruise variable schema; oxygen sbox*mm_kg is commonly mmol/kg and temperature t*c is deg C.", "surface_definition": surface_definition, "depth_bin_m": depth_bin_m, "point_count": len(plot_points), "depth_range_m": [min(depths), max(depths)] if depths else None, "value_range": [min(values), max(values)] if values else None, **_source(path)}
    result["surface_summary"] = _surface_summary(plot_points, value_column, surface_definition)
    result["points"] = plot_points if include_points else None
    return result


@mcp.tool()
async def get_cast_profile(cruise_name: str, cast_number: str, variable: str = "oxygen", include_points: bool = True, depth_min: float | None = None, depth_max: float | None = None, surface_definition: str = "none", depth_bin_m: float | None = None) -> dict[str, Any]:
    """Get one CTD profile, optionally compact and depth-filtered."""
    return await _cast_profile(cruise_name, cast_number, variable, include_points, depth_min, depth_max, surface_definition, depth_bin_m)


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
    cruises = [{"name": cruise_name}] if cruise_name else (await find_cruises(year or datetime.now(timezone.utc).year, season, cruise_type, refresh))["cruises"]
    cast_groups = await asyncio.gather(*[_matching_casts(c["name"], station, max_distance_km) for c in cruises])
    targets = [(c["name"], cast) for c, casts in zip(cruises, cast_groups) for cast in casts]
    targets = targets[:max(1, min(limit, 1000))]
    profiles = await asyncio.gather(*[_cast_profile(name, str(cast["cast"]), variable, include_points, depth_min, depth_max, surface_definition, depth_bin_m) for name, cast in targets])
    for profile, (_, cast) in zip(profiles, targets):
        profile["station_match"] = cast
    return {"station": station.upper() if station else None, "variable": variable, "year": year, "season": season, "surface_definition": surface_definition, "depth_bin_m": depth_bin_m, "profile_count": len(profiles), "truncated": len(targets) < sum(len(group) for group in cast_groups), "profiles": profiles, **_source("/api/ctd/cast/{cruise}/{cast}.csv")}


@mcp.tool()
async def query_ctd(station: str | None = None, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = False, depth_min: float | None = None, depth_max: float | None = None, limit: int = 100, refresh: bool = False, surface_definition: str = "none", depth_bin_m: float | None = None) -> dict[str, Any]:
    """Batch CTD query with explicit surface definitions and optional depth binning."""
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
    units = "deg C" if variable.lower() in {"temperature", "temp"} else None
    if len(requested_variables) > 1:
        units = None
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
    return {
        "chart_type": "vertical_profile",
        "station": station.upper() if station else None,
        "variable": requested_variables[0] if len(requested_variables) == 1 else None,
        "variables": requested_variables,
        "units": units,
        "aggregation": aggregation,
        "surface_definition": surface_definition,
        "row_count": len(rows),
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
        "provenance": {"years": [year_start, year_end], "source": "NES-LTER CTD cast API"},
    }


@mcp.tool()
async def list_dataset_rows(dataset: str, cruise_name: str, station: str | None = None, variable: str | None = None, start_time: str | None = None, end_time: str | None = None, offset: int = 0, limit: int = 500, refresh: bool = False) -> dict[str, Any]:
    """Fetch and filter underway, bottles, nutrients, HPLC, chlorophyll, or event rows."""
    dataset = dataset.lower()
    if dataset not in DATASET_PATHS:
        raise ValueError(f"Unknown dataset {dataset!r}; choose from {', '.join(sorted(DATASET_PATHS))}")
    path = DATASET_PATHS[dataset].format(cruise=quote(cruise_name, safe=""))
    async def load_rows() -> list[dict[str, Any]]:
        if dataset == "event_history":
            value = await _json(path)
            return value if isinstance(value, list) else [value]
        return await _csv(path)

    rows = await _cache.get(f"dataset:{dataset}:{cruise_name}", load_rows, refresh)
    filtered = []
    for row in rows:
        haystack = " ".join(str(value) for value in row.values()).lower()
        timestamp = next((_parse_time(row.get(key)) for key in ("date", "datetime", "timestamp", "time") if row.get(key)), None)
        if station and station.lower() not in haystack:
            continue
        if variable and variable.lower() not in haystack:
            continue
        if start_time and timestamp and timestamp < (_parse_time(start_time) or timestamp):
            continue
        if end_time and timestamp and timestamp > (_parse_time(end_time) or timestamp):
            continue
        filtered.append(row)
    selected, has_more = _paginate(filtered, offset, limit)
    return {"dataset": dataset, "cruise": cruise_name, "offset": offset, "limit": limit, "has_more": has_more, "count": len(selected), "rows": selected, **_source(path)}


@mcp.tool()
async def get_cruise_track(cruise_name: str) -> dict[str, Any]:
    """Return the API's cruise track resource URL for client-side map use."""
    path = f"/cruise/{quote(cruise_name, safe='')}/track/"
    return {"cruise": cruise_name, "track_url": f"{API_BASE_URL}{path}", **_source(path)}


def _row_value(row: dict[str, Any], *names: str) -> Any:
    normalized = {str(key).lower().replace(" ", "_"): value for key, value in row.items()}
    for name in names:
        if name in normalized:
            return normalized[name]
    return None


@mcp.tool()
async def get_cruise_track_data(cruise_name: str, include_points: bool = True) -> dict[str, Any]:
    """Build GeoJSON cruise track data from the cruise's cast positions."""
    rows = await _metadata(cruise_name)
    points = []
    for row in rows:
        latitude = _float(_row_value(row, "latitude", "lat"))
        longitude = _float(_row_value(row, "longitude", "lon", "lng"))
        if latitude is None or longitude is None:
            continue
        points.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
            "properties": {"cruise": cruise_name, "cast": _row_value(row, "cast", "cast_number"), "date": _row_value(row, "date"), "nearest_station": _row_value(row, "nearest_station"), "distance_km": _float(_row_value(row, "distance_km"))},
        })
    coordinates = [feature["geometry"]["coordinates"] for feature in points]
    features = []
    if len(coordinates) >= 2:
        features.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": coordinates}, "properties": {"cruise": cruise_name, "feature": "cast_track"}})
    if include_points:
        features.extend(points)
    path = f"/api/ctd/metadata/{quote(cruise_name, safe='')}.csv"
    return {"cruise": cruise_name, "format": "GeoJSON", "feature_count": len(features), "geojson": {"type": "FeatureCollection", "features": features}, "point_count": len(points), **_source(path)}


@mcp.tool()
async def make_station_map(stations: list[str] | None = None, cruises: list[str] | None = None, include_tracks: bool = True) -> dict[str, Any]:
    """Return station GeoJSON plus an optional Vega-Lite point-map specification."""
    station_catalog = (await list_stations())["stations"]
    wanted = {station.upper() for station in stations} if stations else None
    station_features = []
    map_rows = []
    for row in station_catalog:
        name = _row_value(row, "station", "name", "station_name")
        latitude = _float(_row_value(row, "latitude", "lat"))
        longitude = _float(_row_value(row, "longitude", "lon", "lng"))
        if not name or latitude is None or longitude is None or (wanted and str(name).upper() not in wanted):
            continue
        properties = {"station": name, "depth_m": _float(_row_value(row, "depth_m", "depth")), "comment": _row_value(row, "comment", "comments")}
        station_features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [longitude, latitude]}, "properties": properties})
        map_rows.append({"station": name, "latitude": latitude, "longitude": longitude, "depth_m": properties["depth_m"]})
    track_results = await asyncio.gather(*[get_cruise_track_data(cruise) for cruise in (cruises or [])]) if include_tracks and cruises else []
    track_features = [feature for result in track_results for feature in result["geojson"]["features"]]
    return {
        "format": "GeoJSON",
        "geojson": {"type": "FeatureCollection", "features": track_features + station_features},
        "station_count": len(station_features),
        "cruise_count": len(cruises or []),
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
        **_source("/api/stations/file.csv"),
    }


@mcp.tool()
async def get_dataset_schema(dataset: str, cruise_name: str) -> dict[str, Any]:
    """Infer available columns and example values for a cruise dataset."""
    if dataset == "ctd":
        path = f"/api/ctd/casts/{quote(cruise_name, safe='')}.csv"
    elif dataset == "underway":
        path = f"/api/underway/get_column_headers/{quote(cruise_name, safe='')}"
    else:
        if dataset not in DATASET_PATHS:
            raise ValueError(f"Unknown dataset {dataset!r}")
        path = DATASET_PATHS[dataset].format(cruise=quote(cruise_name, safe=""))
    rows = await (_json_or_text(path) if not path.endswith(".csv") else _csv(path))
    if isinstance(rows, dict):
        return {"dataset": dataset, "cruise": cruise_name, "schema": rows, **_source(path)}
    headers = list(rows[0]) if rows else []
    return {"dataset": dataset, "cruise": cruise_name, "columns": headers, "example": rows[0] if rows else {}, **_source(path)}


@mcp.tool()
async def resolve_variable(name: str, cruise_name: str | None = None, dataset: str = "ctd") -> dict[str, Any]:
    """Resolve a friendly variable name to candidate API columns."""
    candidates = VARIABLE_ALIASES.get(name.lower(), [name])
    available = None
    if cruise_name:
        schema = await get_dataset_schema(dataset, cruise_name)
        available = schema.get("columns", [])
        candidates = [column for column in candidates if column in available] or candidates
    return {"requested": name, "candidates": candidates, "available_matches": [c for c in candidates if available and c in available], "dataset": dataset}


@mcp.resource("nes-lter://stations")
async def stations_resource() -> str:
    """Station catalog as JSON for model reference."""
    import json
    return json.dumps(await list_stations())


@mcp.resource("nes-lter://variable-aliases")
async def variable_aliases_resource() -> str:
    """Friendly CTD variable aliases."""
    import json
    return json.dumps(VARIABLE_ALIASES)


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
