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
- `find_cruises(year?, season?, cruise_type?)` — full CTD cruise catalog when unfiltered, or cruises filtered by year/season/type. Results preserve the API fields from `/api/ctd/cruises/all`.
- `list_casts(cruise_name, station?, max_distance_km?, offset?, limit?)` — bounded cast discovery.
- `get_cast_profile(cruise_name, cast_number, variable, include_points?, depth_min?, depth_max?)` — one profile.
- `query_ctd(...)` — batch CTD query with `surface_definition` (`shallowest_valid`, `mean_0_5m`, `mean_0_10m`, or `mean_0_20m`) and optional `depth_bin_m`.
- `get_station_profile(...)` — compatibility wrapper around `query_ctd`.
- `plot_ctd_profiles(...)` — plotting-ready CTD records plus a Vega-Lite vertical-profile specification. Supports one or multiple `variables`, and `aggregation=none`, `cruise_mean`, or `period_mean`.
- `list_dataset_rows(dataset, cruise_name, ...)` — underway, bottles, bottle summaries, nutrients, HPLC, chlorophyll, events, and event history.
- `get_cruise_track(cruise_name)` — track URL and provenance.
- `get_cruise_track_data(cruise_name)` — GeoJSON cruise track built from cast positions.
- `make_station_map(stations?, cruises?, include_tracks?)` — station GeoJSON and a Vega-Lite map specification.
- `get_dataset_schema(dataset, cruise_name)` — available columns or API schema.
- `resolve_variable(name, cruise_name?, dataset?)` — friendly-name resolution.

The `nes-lter://stations` and `nes-lter://variable-aliases` MCP resources provide stable reference context.

## Plot viewer

`web/nes-lter-vega-plot.js` defines a reusable `<nes-lter-vega-plot>` browser component. The component accepts the `vega_lite` object returned by `plot_ctd_profiles`:

```html
<script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
<script type="module" src="./nes-lter-vega-plot.js"></script>
<nes-lter-vega-plot id="plot"></nes-lter-vega-plot>
<script>
  document.querySelector("#plot").spec = plot_result.vega_lite;
</script>
```

To view the included Spring 2025 L8 oxygen demonstration:

```bash
cd web
python3 -m http.server 8080
```

Open <http://localhost:8080/demo.html>. The `nes-lter://vega-lite-viewer` MCP resource exposes the component source to compatible clients.

Common variables include `oxygen`, `temperature`, `salinity`, `density`, `fluorescence`, and `par`. Raw CTD column names like `sbox0mm_kg` are also accepted.

By default, `winter 2025` is interpreted meteorologically as Dec 2024 through Feb 2025.

## Configuration

Set `NES_LTER_API_BASE_URL` to override the API base URL. Defaults to `https://nes-lter-api.whoi.edu`.

`NES_LTER_CACHE_TTL` controls the in-memory cache duration in seconds (default 900), and `NES_LTER_MAX_CONCURRENCY` controls concurrent API requests (default 8).

For efficient interaction, use `query_ctd(..., include_points=false)` first, then request full points for selected casts. For plots, prefer `plot_ctd_profiles` because it returns compact plotting data and a Vega-Lite spec. For maps, use GeoJSON from `make_station_map` or `get_cruise_track_data`. All data responses include `source_url` and `retrieved_at`.

## Query benchmarks

`benchmarks/bench_queries.py` compares MCP tool latency with an equivalent direct NES-LTER API workflow. It records elapsed time, response size, row count, errors, median, and p95 summaries as JSON Lines:

```bash
uv run python benchmarks/bench_queries.py --warmups 2 --repetitions 10 --mode both --output benchmark-results.jsonl
```

Use `--mode mcp` or `--mode direct` to run one side only. Add `--mode all` for the optional bare-LLM comparison:

```bash
export LLM_API_KEY=...
export LLM_MODEL=gpt-4o-mini
export LLM_INPUT_USD_PER_1M=0
export LLM_OUTPUT_USD_PER_1M=0
uv run python benchmarks/bench_queries.py --mode all --repetitions 10 --output benchmark-results.jsonl
```

The bare-LLM condition receives only the API base URL and a few endpoint paths. The model produces an API plan without MCP tools, then the harness executes the allowlisted direct API workflow and returns the same profile rows. Its elapsed time therefore includes model planning plus API retrieval. LLM rows include total latency, separate model/API timings, token usage, and estimated cost when both pricing environment variables are set.

`--mode all` also includes a `codex` condition. It launches `codex exec --json` in an isolated temporary directory with user configuration, rules, and MCP settings excluded. Codex must retrieve the same structured data using read-only API requests. Use `--codex-sandbox read-only` by default; the Codex CLI must be installed and authenticated. Offline checks are in `tests/test_bench_queries.py`.

To plot multiple variables together, pass `variables=["sbeox0ml_l", "t090c"]`; the Vega-Lite result overlays them on one chart with a shared depth axis and independent x-scales, which avoids conflating their units. Use `width` and `height` to control the returned chart dimensions.
