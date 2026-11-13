"""Efficient MCP access to the NES-LTER API (v3).

The v3 API does the filtering, variable resolution, station matching and
profile summaries server-side, so each tool is one or two requests. Tool
names and parameters are unchanged from the v2 version of this server.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
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
        "Use list_dataset_rows for underway, bottle, nutrient, chlorophyll, "
        "HPLC and event data. Variable names accept friendly names (oxygen, "
        "temperature), aliases and raw instrument column names; use "
        "resolve_variable to see units and what a name maps to."
    ),
)

DATASETS = {"underway": "underway", "bottles": "bottles", "bottle_summary": "bottles", "nutrients": "nutrients",
            "hplc": "hplc", "chlorophyll": "chlorophyll", "events": "events", "casts": "casts", "ctd": "ctd"}
BOTTLE_SUMMARY_COLUMNS = ("cruise", "cast", "niskin", "time", "latitude", "longitude", "depth_m")

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
async def find_cruises(year: int, season: str | None = None, cruise_type: str | None = "NES-LTER", refresh: bool = False) -> dict[str, Any]:
    """Find cruises by year, meteorological season (winter = Dec of the previous year to Feb), and type."""
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
    return {"cruise": cruise_name, "offset": offset, "limit": limit,
            "has_more": body["meta"]["row_count"] > offset + len(rows), "count": len(rows), "casts": rows,
            **_source("/data/casts", params, body["meta"]["build_id"])}


def _summary_to_profile(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "cruise": row["cruise"], "cast": row["cast"], "time": row["time"], "latitude": row["latitude"],
        "longitude": row["longitude"], "nearest_station": row["nearest_station"], "distance_km": row["distance_km"],
        "variable": row["variable"], "units": row["unit"], "point_count": row["n"],
        "depth_range_m": [row["depth_min"], row["depth_max"]], "value_range": [row["min"], row["max"]],
        "mean": row["mean"], "surface_value": row["surface"], "bottom_value": row["bottom"],
    }


async def _profiles(params: dict[str, Any], variable: str, include_points: bool, limit: int) -> tuple[list[dict[str, Any]], dict]:
    """(per-cast summaries for one variable, response meta), with points attached when asked for."""
    summary = await _get("/profiles", {**params, "variables": variable, "summary": "true"})
    profiles = [_summary_to_profile(r) for r in summary["data"]][:limit]
    if include_points and profiles:
        async def points(p):
            body = await _get(f"/casts/{quote(p['cruise'])}/{quote(p['cast'])}/profile",
                              {"variables": p["variable"], "depth_min": params.get("depth_min"),
                               "depth_max": params.get("depth_max")})
            return [{"depth_m": r["depth_m"], p["variable"]: r[p["variable"]], "time": r["time"]}
                    for r in body["data"] if r[p["variable"]] is not None]
        for p, pts in zip(profiles, await asyncio.gather(*[points(p) for p in profiles])):
            p["points"] = pts
    return profiles, summary["meta"] | {"total": len(summary["data"])}


@mcp.tool()
async def get_cast_profile(cruise_name: str, cast_number: str, variable: str = "oxygen", include_points: bool = True, depth_min: float | None = None, depth_max: float | None = None) -> dict[str, Any]:
    """Get one CTD profile: units, depth and value ranges, surface/bottom values, and optionally the points."""
    params = {"cruise": cruise_name, "cast": cast_number, "depth_min": depth_min, "depth_max": depth_max}
    profiles, _ = await _profiles(params, variable, include_points, 1)
    if not profiles:
        return {"cruise": cruise_name, "cast": cast_number, "variable_requested": variable, "point_count": 0,
                "points": [] if include_points else None, **_source("/profiles", params)}
    return {"variable_requested": variable, **profiles[0], **_source("/profiles", params)}


@mcp.tool()
async def query_ctd(station: str | None = None, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = False, depth_min: float | None = None, depth_max: float | None = None, limit: int = 100, refresh: bool = False) -> dict[str, Any]:
    """Batch CTD query across cruises. Returns compact per-cast summaries unless include_points is true."""
    if not cruise_name and year is None:
        raise ValueError("Provide cruise_name or year")
    cruises = [cruise_name] if cruise_name else await _cruise_names(year, season, cruise_type, refresh)
    if not cruises:
        return {"station": station, "variable": variable, "year": year, "season": season, "profile_count": 0,
                "truncated": False, "profiles": [], **_source("/cruises", {"year": year, "season": season})}
    params = {"cruise": ",".join(cruises), "station": station, "max_distance_km": max_distance_km if station else None,
              "year": year, "season": season, "depth_min": depth_min, "depth_max": depth_max}
    limit = max(1, min(limit, 1000))
    profiles, meta = await _profiles(params, variable, include_points, limit)
    return {"station": station.upper() if station else None, "variable": variable, "year": year, "season": season,
            "profile_count": len(profiles), "truncated": meta["total"] > limit, "profiles": profiles,
            **_source("/profiles", {**params, "variables": variable, "summary": "true"}, meta["build_id"])}


@mcp.tool()
async def get_station_profile(station: str, variable: str = "oxygen", year: int | None = None, season: str | None = None, cruise_name: str | None = None, cruise_type: str | None = "NES-LTER", max_distance_km: float = 3.0, include_points: bool = True, limit: int = 100) -> dict[str, Any]:
    """Compatibility wrapper for station profile lookup; use query_ctd for compact results."""
    return await query_ctd(station, variable, year, season, cruise_name, cruise_type, max_distance_km, include_points, limit=limit)


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
    return {"dataset": name, "cruise": cruise_name, "offset": offset, "limit": limit,
            "has_more": body["meta"]["row_count"] > offset + len(rows), "count": len(rows), "units": units,
            "rows": rows, **_source(f"/data/{DATASETS[name]}", params, body["meta"]["build_id"])}


@mcp.tool()
async def get_cruise_track(cruise_name: str) -> dict[str, Any]:
    """URLs for the cruise's underway track (time, latitude, longitude, ...) as CSV or Parquet."""
    c = quote(cruise_name.lower(), safe="")
    path = f"/files/{c}/underway.csv"
    return {"cruise": cruise_name, "track_url": _url(path), "track_parquet_url": _url(f"/files/{c}/underway.parquet"),
            **_source(path)}


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
    try:
        body = await _cached(f"/variables/{quote(name, safe='')}")
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
    """CTD variables: name -> units and accepted aliases."""
    body = await _cached("/variables", {"dataset": "ctd"})
    return json.dumps({v["name"]: {"units": v["units"], "aliases": v["aliases"]} for v in body["variables"]})


@mcp.prompt()
def station_profile_prompt(station: str, variable: str, season: str, year: int) -> str:
    return f"Use query_ctd(station={station!r}, variable={variable!r}, season={season!r}, year={year}, include_points=false), then summarize the selected profiles and units."


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
