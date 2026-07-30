# nes-lter-mcp

An MCP server for the [NES-LTER API](https://nes-lter-api.whoi.edu/). It exposes efficient discovery, batch CTD queries, cruise datasets, schema discovery, and provenance-rich results.

Example prompt to an MCP client:

> show me the profile for oxygen for station L8 for winter 2025

The server can satisfy this with `query_ctd(station="L8", variable="oxygen", season="winter", year=2025)`.

## Install / run

```bash
uv sync
uv run nes-lter-mcp
```

For MCP clients, configure the command as:

```bash
uv --directory /path/to/nes-lter-mcp run nes-lter-mcp
```

## Tools

- `list_stations()` — station catalog with coordinates, depth, and provenance.
- `find_cruises(year, season?, cruise_type?)` — CTD cruises by date/season/type.
- `list_casts(cruise_name, station?, max_distance_km?, offset?, limit?)` — bounded cast discovery.
- `get_cast_profile(cruise_name, cast_number, variable, include_points?, depth_min?, depth_max?)` — one profile.
- `query_ctd(...)` — batch CTD query; compact summaries by default, with bounded concurrency.
- `get_station_profile(...)` — compatibility wrapper around `query_ctd`.
- `list_dataset_rows(dataset, cruise_name, ...)` — underway, bottles, bottle summaries, nutrients, HPLC, chlorophyll, events, and event history.
- `get_cruise_track(cruise_name)` — track URL and provenance.
- `get_dataset_schema(dataset, cruise_name)` — available columns or API schema.
- `resolve_variable(name, cruise_name?, dataset?)` — friendly-name resolution.

The `nes-lter://stations` and `nes-lter://variable-aliases` MCP resources provide stable reference context.

Common variables include `oxygen`, `temperature`, `salinity`, `density`, `fluorescence`, and `par`. Raw CTD column names like `sbox0mm_kg` are also accepted.

By default, `winter 2025` is interpreted meteorologically as Dec 2024 through Feb 2025.

## Configuration

Set `NES_LTER_API_BASE_URL` to override the API base URL. Defaults to `https://nes-lter-api.whoi.edu`.

`NES_LTER_CACHE_TTL` controls the in-memory cache duration in seconds (default 900), and `NES_LTER_MAX_CONCURRENCY` controls concurrent API requests (default 8).

For efficient interaction, use `query_ctd(..., include_points=false)` first, then request full points for selected casts. All data responses include `source_url` and `retrieved_at`.
