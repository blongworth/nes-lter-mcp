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
        "HPLC, event, and event-history data."
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


@mcp.tool()
async def find_cruises(year: int, season: str | None = None, cruise_type: str | None = "NES-LTER", refresh: bool = False) -> dict[str, Any]:
    """Find cruises by year, meteorological season, and type."""
    cruises = []
    for cruise in await _all_cruises(refresh):
        start = _parse_time(cruise.get("start_time"))
        if start and _in_season(start, year, season) and (not cruise_type or cruise.get("type", "").lower() == cruise_type.lower()):
            cruises.append(cruise)
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


async def _cast_profile(cruise_name: str, cast_number: str, variable: str, include_points: bool = True, depth_min: float | None = None, depth_max: float | None = None) -> dict[str, Any]:
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
    depths, values = [p["depth_m"] for p in points], [p[value_column] for p in points]
    result = {"cruise": cruise_name, "cast": cast_number, "variable_requested": variable, "value_column": value_column, "depth_column": depth_column, "units_note": "Confirm units from the cruise variable schema; oxygen sbox*mm_kg is commonly mmol/kg and temperature t*c is deg C.", "point_count": len(points), "depth_range_m": [min(depths), max(depths)] if depths else None, "value_range": [min(values), max(values)] if values else None, **_source(path)}
    result["points"] = points if include_points else None
    return result


@mcp.tool()
async def get_cast_profile(cruise_name: str, cast_number: str, variable: str = "oxygen", include_points: bool = True, depth_min: float | None = None, depth_max: float | None = None) -> dict[str, Any]:
    """Get one CTD profile, optionally compact and depth-filtered."""
    return await _cast_profile(cruise_name, cast_number, variable, include_points, depth_min, depth_max)


@mcp.tool()
async def query_ctd(station: str | None = None, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = False, depth_min: float | None = None, depth_max: float | None = None, limit: int = 100, refresh: bool = False) -> dict[str, Any]:
    """Batch CTD query. Returns compact cast summaries unless include_points is true."""
    if not cruise_name and year is None:
        raise ValueError("Provide cruise_name or year")
    cruises = [{"name": cruise_name}] if cruise_name else (await find_cruises(year or datetime.now(timezone.utc).year, season, cruise_type, refresh))["cruises"]
    cast_groups = await asyncio.gather(*[_matching_casts(c["name"], station, max_distance_km) for c in cruises])
    targets = [(c["name"], cast) for c, casts in zip(cruises, cast_groups) for cast in casts]
    targets = targets[:max(1, min(limit, 1000))]
    profiles = await asyncio.gather(*[_cast_profile(name, str(cast["cast"]), variable, include_points, depth_min, depth_max) for name, cast in targets])
    for profile, (_, cast) in zip(profiles, targets):
        profile["station_match"] = cast
    return {"station": station.upper() if station else None, "variable": variable, "year": year, "season": season, "profile_count": len(profiles), "truncated": len(targets) < sum(len(group) for group in cast_groups), "profiles": profiles, **_source("/api/ctd/cast/{cruise}/{cast}.csv")}


@mcp.tool()
async def get_station_profile(station: str, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = True, limit: int = 100) -> dict[str, Any]:
    """Compatibility wrapper for station profile lookup; use query_ctd for compact results."""
    return await query_ctd(station, variable, year, season, cruise_name, cruise_type, max_distance_km, include_points, limit=limit)


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


@mcp.prompt()
def station_profile_prompt(station: str, variable: str, season: str, year: int) -> str:
    return f"Use query_ctd(station={station!r}, variable={variable!r}, season={season!r}, year={year}, include_points=false), then summarize the selected profiles and units."


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
