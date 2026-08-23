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
        "Use query_underway for meteorological/oceanographic underway data "
        "(wind, temperature, salinity, position, speed, etc.) with vessel-aware "
        "variable resolution and cruise-level aggregation; use list_dataset_rows "
        "for raw underway, bottle, nutrient, chlorophyll, HPLC, event, and "
        "event-history rows. For plots use plot_ctd_profiles; for maps use "
        "make_station_map or get_cruise_track_data."
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

KT_TO_MS = 0.514444

# Underway instrumentation and column naming differ by vessel. Atlantis shares the
# Neil Armstrong's WXT weather-station convention, so both map to the "armstrong" family.
UNDERWAY_VESSEL_FAMILIES = {
    "endeavor": "endeavor",
    "neil armstrong": "armstrong",
    "atlantis": "armstrong",
    "hugh r. sharp": "sharp",
    "atlantic explorer": "atlantic_explorer",
}

UNDERWAY_FRIENDLY_SYNONYMS = {
    "wind speed": "wind_speed", "windspeed": "wind_speed", "wind_speed": "wind_speed",
    "wind direction": "wind_direction", "winddir": "wind_direction", "wind_dir": "wind_direction", "wind_direction": "wind_direction",
    "temperature": "water_temperature", "temp": "water_temperature", "sst": "water_temperature",
    "sea surface temperature": "water_temperature", "water temperature": "water_temperature", "water_temperature": "water_temperature",
    "air temperature": "air_temperature", "airtemp": "air_temperature", "air_temp": "air_temperature", "air_temperature": "air_temperature",
    "salinity": "salinity", "sal": "salinity",
    "pressure": "barometric_pressure", "baro": "barometric_pressure", "barometric pressure": "barometric_pressure", "barometric_pressure": "barometric_pressure",
    "humidity": "relative_humidity", "rh": "relative_humidity", "relative humidity": "relative_humidity", "relative_humidity": "relative_humidity",
    "speed": "speed_over_ground", "sog": "speed_over_ground", "speed over ground": "speed_over_ground", "speed_over_ground": "speed_over_ground",
    "heading": "heading",
    "course": "course_over_ground", "cog": "course_over_ground", "course over ground": "course_over_ground", "course_over_ground": "course_over_ground",
    "lat": "latitude", "latitude": "latitude",
    "lon": "longitude", "lng": "longitude", "longitude": "longitude",
}

UNDERWAY_VARIABLE_KIND = {"wind_direction": "circular", "heading": "circular", "course_over_ground": "circular"}
UNDERWAY_VARIABLE_UNITS = {
    "wind_speed": "m/s", "wind_direction": "deg", "heading": "deg", "course_over_ground": "deg",
    "speed_over_ground": "kt", "air_temperature": "deg C", "water_temperature": "deg C",
    "salinity": "psu", "barometric_pressure": "hPa", "relative_humidity": "%",
    "latitude": "deg", "longitude": "deg",
}

# Conversion factors into each canonical unit above, keyed by the various ways the API's
# /api/underway/column_definition/{cruise} endpoint spells a raw unit (lowercased, periods
# stripped). Used to correct the static per-vessel factors below against the API's own
# declared units, since instrumentation units can vary or change per vessel/cruise.
UNDERWAY_UNIT_CONVERSION_FACTORS = {
    "m/s": {"knots": KT_TO_MS, "knot": KT_TO_MS, "kt": KT_TO_MS, "kts": KT_TO_MS, "m/s": 1.0, "meters/second": 1.0},
    "kt": {"knots": 1.0, "knot": 1.0, "kt": 1.0, "kts": 1.0},
    "deg": {"deg": 1.0, "deg true": 1.0, "deg relative": 1.0, "degree": 1.0, "degrees": 1.0},
    "deg C": {"deg c": 1.0, "degree c": 1.0, "degrees c": 1.0, "c": 1.0},
    "psu": {"psu": 1.0},
    "hPa": {"hpa": 1.0, "mbar": 1.0, "millibar": 1.0, "millibars": 1.0},
    "%": {"%": 1.0, "percent": 1.0},
}

# Per vessel family: friendly name -> [(column, factor_to_canonical_unit), ...] in priority order.
UNDERWAY_VARIABLE_ALIASES = {
    "endeavor": {
        "wind_speed": [("wind_truewindbow_speed", KT_TO_MS)],
        "wind_direction": [("wind_truewindbow_direction", 1.0)],
        "heading": [("gyro1_heading", 1.0), ("gnss_adu2_heading", 1.0)],
        "course_over_ground": [("gps_furuno_tmg", 1.0)],
        "speed_over_ground": [("gps_furuno_smg", 1.0)],
        "air_temperature": [("met_rmy_bow_airtemp", 1.0), ("met_rmy_trans_airtemp", 1.0)],
        "water_temperature": [("tsg1_temperature", 1.0), ("tsg2_temperature", 1.0), ("met_rmy_trans_sst1", 1.0)],
        "salinity": [("tsg1_salinity", 1.0), ("tsg2_salinity", 1.0)],
        "barometric_pressure": [("met_rmy_bow_baropressure", 1.0), ("met_rmy_trans_baropressure", 1.0)],
        "relative_humidity": [("met_rmy_bow_relhumidity", 1.0), ("met_rmy_trans_relhumidity", 1.0)],
        "latitude": [("gps_furuno_latitude", 1.0)],
        "longitude": [("gps_furuno_longitude", 1.0)],
    },
    "armstrong": {
        "wind_speed": [("wxtp_sm", 1.0), ("wxts_sm", 1.0)],
        "wind_direction": [("wxtp_dm", 1.0), ("wxts_dm", 1.0)],
        "heading": [("hdt", 1.0)],
        "course_over_ground": [("cog", 1.0), ("cnav_cog", 1.0)],
        "speed_over_ground": [("sog", 1.0)],
        "air_temperature": [("wxtp_ta", 1.0), ("wxts_ta", 1.0)],
        "water_temperature": [("sbe48t", 1.0)],
        "salinity": [("sbe45s", 1.0)],
        "barometric_pressure": [("barom_p", 1.0), ("barom_s", 1.0), ("wxtp_pa", 1.0)],
        "relative_humidity": [("wxtp_ua", 1.0), ("wxts_ua", 1.0)],
        "latitude": [("dec_lat", 1.0)],
        "longitude": [("dec_lon", 1.0)],
    },
    # Sharp's SCS column naming changed at some point before HRS2601; older cruises like
    # HRS2303 use a distinct legacy naming scheme (with only one true-wind sensor instead
    # of two), listed second in each candidate list so current cruises still match first.
    "sharp": {
        "wind_speed": [("wind1_true_speed_kt", KT_TO_MS), ("wind2_true_speed_kt", KT_TO_MS), ("true_wind_speed__knots", KT_TO_MS)],
        "wind_direction": [("wind1_true_dir_deg", 1.0), ("wind2_true_dir_deg", 1.0), ("true_wind_direction_deg", 1.0)],
        "heading": [("heading", 1.0)],
        "course_over_ground": [("course_true_deg", 1.0), ("cog_deg", 1.0)],
        "speed_over_ground": [("sog_kts", 1.0)],
        "air_temperature": [("air_temperature_degree_c", 1.0), ("air_temperature_c", 1.0)],
        "water_temperature": [("water_temperature_degree_c", 1.0)],
        "salinity": [("salinity_psu", 1.0)],
        # HRS2303's Barometer_Decibars column is mislabeled: its declared unit is "decibars"
        # but the actual values (~1020) are already hPa, so this is left unconverted.
        "barometric_pressure": [("barometric_pressure_hpa", 1.0), ("barometer_decibars", 1.0)],
        "relative_humidity": [("relative_humidity_percent", 1.0), ("humidity_", 1.0)],
        "latitude": [("dec_lat", 1.0), ("latitude_deg", 1.0)],
        "longitude": [("dec_lon", 1.0), ("longitude_deg", 1.0)],
    },
    "atlantic_explorer": {
        "wind_speed": [("twindspdpri_kts", KT_TO_MS), ("twindspdsec_kts", KT_TO_MS), ("twindspdter_kts", KT_TO_MS)],
        "wind_direction": [("twinddirpri_deg", 1.0), ("twinddirsec_deg", 1.0), ("twinddirter_deg", 1.0)],
        "heading": [("gps_hdt_deg", 1.0)],
        "course_over_ground": [("cog_deg", 1.0)],
        "speed_over_ground": [("sog_kts", 1.0)],
        "air_temperature": [("airtemp_c", 1.0), ("rmy_airtemp_c", 1.0)],
        "water_temperature": [("sbe38_remotetemp_c", 1.0), ("sbe45pri_temp_c", 1.0)],
        "salinity": [("sbe45pri_sal_psu", 1.0), ("sbe45sec_sal_psu", 1.0)],
        "barometric_pressure": [("bp_corr_mbar", 1.0), ("rmy_bp_mbar", 1.0)],
        "relative_humidity": [("rh_percent", 1.0), ("rmy_rh_percent", 1.0)],
        "latitude": [("latitude", 1.0)],
        "longitude": [("longitude", 1.0)],
    },
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
    notes = [note for note in (_truncation_note(len(selected), len(rows), "limit/offset"),) if note]
    return {"cruise": cruise_name, "offset": offset, "limit": limit, "has_more": has_more, "count": len(selected), "notes": notes, "casts": selected, **_source(f"/api/ctd/metadata/{quote(cruise_name, safe='')}.csv")}


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
    cast_results, cast_errors = await _gather_partial(cruises, lambda c: _matching_casts(c["name"], station, max_distance_km), lambda c: c["name"])
    targets = [(cruise["name"], cast) for cruise, casts in cast_results for cast in casts]
    total_casts = len(targets)
    targets = targets[:max(1, min(limit, 1000))]
    profile_results, profile_errors = await _gather_partial(
        targets,
        lambda target: _cast_profile(target[0], str(target[1]["cast"]), variable, include_points, depth_min, depth_max, surface_definition, depth_bin_m),
        lambda target: f"{target[0]} cast {target[1]['cast']}",
    )
    profiles = []
    for (_, cast), profile in profile_results:
        profile["station_match"] = cast
        profiles.append(profile)
    errors = cast_errors + profile_errors
    point_count = sum(profile.get("point_count", 0) for profile in profiles)
    notes = [note for note in (
        _truncation_note(len(targets), total_casts, "limit"),
        _point_count_note(point_count) if include_points else None,
        f"{len(errors)} cast(s) failed to load and were skipped; see errors." if errors else None,
    ) if note]
    return {"station": station.upper() if station else None, "variable": variable, "year": year, "season": season, "surface_definition": surface_definition, "depth_bin_m": depth_bin_m, "profile_count": len(profiles), "point_count": point_count, "truncated": len(targets) < total_casts or bool(errors), "notes": notes, "errors": errors, "profiles": profiles, **_source("/api/ctd/cast/{cruise}/{cast}.csv")}


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
        "provenance": {"years": [year_start, year_end], "source": "NES-LTER CTD cast API"},
    }


async def _dataset_rows(dataset: str, cruise_name: str, refresh: bool = False) -> tuple[list[dict[str, Any]], str]:
    path = DATASET_PATHS[dataset].format(cruise=quote(cruise_name, safe=""))
    async def load_rows() -> list[dict[str, Any]]:
        if dataset == "event_history":
            value = await _json(path)
            return value if isinstance(value, list) else [value]
        return await _csv(path)

    rows = await _cache.get(f"dataset:{dataset}:{cruise_name}", load_rows, refresh)
    return rows, path


@mcp.tool()
async def list_dataset_rows(dataset: str, cruise_name: str, station: str | None = None, variable: str | None = None, start_time: str | None = None, end_time: str | None = None, offset: int = 0, limit: int = 500, refresh: bool = False) -> dict[str, Any]:
    """Fetch and filter underway, bottles, nutrients, HPLC, chlorophyll, or event rows."""
    dataset = dataset.lower()
    if dataset not in DATASET_PATHS:
        raise ValueError(f"Unknown dataset {dataset!r}; choose from {', '.join(sorted(DATASET_PATHS))}")
    rows, path = await _dataset_rows(dataset, cruise_name, refresh)
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
    notes = [note for note in (_truncation_note(len(selected), len(filtered), "limit/offset"),) if note]
    return {"dataset": dataset, "cruise": cruise_name, "offset": offset, "limit": limit, "has_more": has_more, "count": len(selected), "notes": notes, "rows": selected, **_source(path)}


def _vessel_family(vessel_name: str | None) -> str | None:
    if not vessel_name:
        return None
    name = vessel_name.lower()
    return next((family for key, family in UNDERWAY_VESSEL_FAMILIES.items() if key in name), None)


async def _vessel_for_cruise(cruise_name: str, refresh: bool = False) -> str | None:
    cruise = next((c for c in await _all_cruises(refresh) if c.get("name") == cruise_name), None)
    return _vessel_family(cruise.get("vessel_name")) if cruise else None


async def _underway_column_units(cruise_name: str, refresh: bool = False) -> dict[str, str]:
    """Map lowercased column name -> the raw unit string declared by the API for this cruise.

    The endpoint's response shape (and completeness) varies by vessel/cruise, so this
    tolerates missing units, missing names, and both the "Name"/"Units" and "name"/"units"
    key-casing conventions observed across vessels; any failure yields an empty map so
    callers fall back to the static UNDERWAY_VARIABLE_ALIASES factors.
    """
    async def _load() -> dict[str, str]:
        try:
            entries = await _json(f"/api/underway/column_definition/{quote(cruise_name, safe='')}")
        except Exception:
            return {}
        if not isinstance(entries, list):
            return {}
        units: dict[str, str] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("Name") or entry.get("name")
            unit = entry.get("Units") if entry.get("Units") is not None else entry.get("units")
            if name and unit:
                # CSV column headers use underscores; column_definition names use hyphens.
                units[str(name).strip().lower().replace("-", "_")] = str(unit).strip()
        return units

    return await _cache.get(f"underway_units:{cruise_name}", _load, refresh)


def _declared_unit_factor(friendly_variable: str, declared_unit: str | None) -> float | None:
    """Return the factor to convert `declared_unit` into the friendly variable's canonical
    unit, or None if the unit is missing or not a recognized spelling."""
    if not declared_unit:
        return None
    canonical = UNDERWAY_VARIABLE_UNITS.get(friendly_variable)
    table = UNDERWAY_UNIT_CONVERSION_FACTORS.get(canonical)
    if not table:
        return None
    normalized = declared_unit.strip().lower().replace(".", "")
    normalized = " ".join(normalized.split())
    return table.get(normalized)


def _circular_mean(degrees: list[float]) -> float:
    sin_sum = sum(math.sin(math.radians(d)) for d in degrees)
    cos_sum = sum(math.cos(math.radians(d)) for d in degrees)
    return math.degrees(math.atan2(sin_sum, cos_sum)) % 360


def _resolve_underway_variable(variable: str, family: str, headers: list[str]) -> tuple[str, str, float]:
    """Return (resolved_column, friendly_name, unit_factor) for the first candidate present in headers."""
    key = UNDERWAY_FRIENDLY_SYNONYMS.get(variable.lower(), variable.lower())
    candidates = UNDERWAY_VARIABLE_ALIASES.get(family, {}).get(key) or [(variable, 1.0)]
    for column, factor in candidates:
        if column in headers:
            return column, key, factor
    raise ValueError(f"Variable {variable!r} was not found for vessel family {family!r}; available columns include: {', '.join(headers[:80])}")


async def _underway_cruise_values(cruise_name: str, variable: str, start_time: str | None, end_time: str | None, refresh: bool) -> dict[str, Any]:
    vessel = await _vessel_for_cruise(cruise_name, refresh)
    if vessel is None:
        raise ValueError(f"Could not determine vessel for cruise {cruise_name!r}; underway variable resolution requires a known vessel.")
    rows, _ = await _dataset_rows("underway", cruise_name, refresh)
    if not rows:
        return {"cruise": cruise_name, "vessel": vessel, "column": None, "friendly_variable": None, "unit": None, "kind": "linear", "values": [], "declared_unit": None, "unit_source": None}
    headers = list(rows[0])
    column, friendly, default_factor = _resolve_underway_variable(variable, vessel, headers)
    column_units = await _underway_column_units(cruise_name, refresh)
    declared_unit = column_units.get(column.lower())
    declared_factor = _declared_unit_factor(friendly, declared_unit)
    factor = declared_factor if declared_factor is not None else default_factor
    unit_source = "api" if declared_factor is not None else "default_table"
    lat_col = next((c for c, _ in UNDERWAY_VARIABLE_ALIASES.get(vessel, {}).get("latitude", []) if c in headers), None)
    lon_col = next((c for c, _ in UNDERWAY_VARIABLE_ALIASES.get(vessel, {}).get("longitude", []) if c in headers), None)
    start_dt, end_dt = _parse_time(start_time), _parse_time(end_time)
    values = []
    for row in rows:
        value = _float(row.get(column))
        if value is None:
            continue
        timestamp = next((_parse_time(row.get(key)) for key in ("date", "datetimeiso", "datetime", "timestamp") if row.get(key)), None)
        if start_dt and timestamp and timestamp < start_dt:
            continue
        if end_dt and timestamp and timestamp > end_dt:
            continue
        values.append({
            "date": row.get("date") or row.get("datetimeiso"),
            "value": value * factor,
            "latitude": _float(row.get(lat_col)) if lat_col else None,
            "longitude": _float(row.get(lon_col)) if lon_col else None,
        })
    return {
        "cruise": cruise_name, "vessel": vessel, "column": column, "friendly_variable": friendly,
        "unit": UNDERWAY_VARIABLE_UNITS.get(friendly), "kind": UNDERWAY_VARIABLE_KIND.get(friendly, "linear"),
        "values": values, "declared_unit": declared_unit, "unit_source": unit_source,
    }


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
    """Query underway meteorological/oceanographic data with vessel-aware variable resolution.

    Resolves friendly names (wind_speed, wind_direction, water_temperature,
    air_temperature, salinity, barometric_pressure, relative_humidity, heading,
    course_over_ground, speed_over_ground, latitude, longitude — plus common synonyms
    like temp, sal, sog, cog) to the correct column for the cruise's vessel (Endeavor,
    Neil Armstrong, Atlantis, Hugh R. Sharp, or Atlantic Explorer), converts to a
    common unit, and aggregates across one or many cruises. Directional variables
    (wind_direction, heading, course_over_ground) use a circular mean.

    aggregation: "cruise_mean" (default, one row per cruise), "period_mean" (one
    overall value, equal-weighted across matched cruises), or "none" (paginated
    per-reading rows with position, for e.g. plotting a track colored by variable).
    """
    valid_aggregations = {"none", "cruise_mean", "period_mean"}
    if aggregation not in valid_aggregations:
        raise ValueError(f"Unknown aggregation {aggregation!r}; use {', '.join(sorted(valid_aggregations))}")
    if not cruise_name and year is None and season is None:
        raise ValueError("Provide cruise_name, year, or season")
    # Pass year through as-is (rather than defaulting to the current year) so that
    # season alone matches that season across all years, per find_cruises' documented behavior.
    cruises = [{"name": cruise_name}] if cruise_name else (await find_cruises(year, season, cruise_type, refresh))["cruises"]
    if not cruises:
        return {"variable": variable, "aggregation": aggregation, "cruise_count": 0, "notes": ["No cruises matched the given filters."], "errors": [], "data": [], **_source("/api/underway/{cruise}.csv")}

    results, errors = await _gather_partial(
        cruises,
        lambda c: _underway_cruise_values(c["name"], variable, start_time, end_time, refresh),
        lambda c: c["name"],
    )
    cruise_results = [result for _, result in results]

    friendly_variable = next((r["friendly_variable"] for r in cruise_results if r["values"]), None)
    unit = next((r["unit"] for r in cruise_results if r["values"]), None)
    is_circular = next((r["kind"] == "circular" for r in cruise_results if r["values"]), False)
    notes = []
    fallback_cases = sorted({(r["vessel"], r["column"]) for r in cruise_results if r["values"] and r["unit_source"] == "default_table"})
    notes.extend(
        f"Vessel {vessel!r} column {column!r} had no API-declared unit recognized for {friendly_variable!r}; used the built-in default conversion factor instead."
        for vessel, column in fallback_cases
    )

    if aggregation == "none":
        flat = [
            {"cruise": r["cruise"], "vessel": r["vessel"], "date": v["date"], "value": v["value"], "latitude": v["latitude"], "longitude": v["longitude"]}
            for r in cruise_results for v in r["values"]
        ]
        data, has_more = _paginate(flat, 0, limit)
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

    return {
        "variable": friendly_variable or variable,
        "unit": unit,
        "kind": "circular" if is_circular else "linear",
        "aggregation": aggregation,
        "cruise_count": len(cruise_results),
        "notes": notes,
        "errors": errors,
        "data": data,
        **_source("/api/underway/{cruise}.csv"),
    }


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
    if dataset == "underway":
        key = UNDERWAY_FRIENDLY_SYNONYMS.get(name.lower(), name.lower())
        if cruise_name:
            vessel = await _vessel_for_cruise(cruise_name)
            candidates = [column for column, _ in UNDERWAY_VARIABLE_ALIASES.get(vessel, {}).get(key, [(name, 1.0)])]
            schema = await get_dataset_schema(dataset, cruise_name)
            available = schema.get("columns") or schema.get("schema", {}).get("metadata", {}).get("columns", [])
            return {"requested": name, "resolved_variable": key, "vessel": vessel, "candidates": candidates, "available_matches": [c for c in candidates if c in available], "dataset": dataset}
        candidates = sorted({column for family in UNDERWAY_VARIABLE_ALIASES.values() for column, _ in family.get(key, [])}) or [name]
        return {"requested": name, "resolved_variable": key, "candidates": candidates, "available_matches": [], "dataset": dataset}
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
    """Friendly CTD and underway variable aliases."""
    import json
    return json.dumps({"ctd": VARIABLE_ALIASES, "underway": UNDERWAY_VARIABLE_ALIASES})


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
