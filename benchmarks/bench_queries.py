"""Benchmark MCP tool calls against equivalent direct NES-LTER API calls.

The benchmark measures tool/API execution, not model inference.  Use an
instrumented model client separately when measuring prompt-to-answer latency
or token cost.

Examples:
    uv run python benchmarks/bench_queries.py --repetitions 10
    uv run python benchmarks/bench_queries.py --mode mcp --output results.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

API_BASE_URL = os.environ.get("NES_LTER_API_BASE_URL", "https://nes-lter-api.whoi.edu").rstrip("/")
SERVER_COMMAND = [sys.executable, "-m", "main"]
SEASON_MONTHS = {"winter": {12, 1, 2}, "spring": {3, 4, 5}, "summer": {6, 7, 8}, "fall": {9, 10, 11}}

CASES: dict[str, dict[str, Any]] = {
    "l8_winter_temperature": {
        "tool": "plot_ctd_profiles",
        "arguments": {
            "station": "L8",
            "variable": "temperature",
            "year_start": 2018,
            "year_end": 2026,
            "season": "winter",
            "aggregation": "none",
            "depth_bin_m": 5,
            "limit_per_year": 100,
            "width": 450,
            "height": 325,
        },
    },
}


def bare_llm_prompt(case: dict[str, Any]) -> str:
    """Prompt a model without tools, giving it only API discovery context."""
    return (
        "You have no tools and must not claim to have fetched data. "
        "The NES-LTER API base URL is https://nes-lter-api.whoi.edu. "
        "The CTD cruise catalog is available at /api/ctd/cruises/all; "
        "cruise metadata is /api/ctd/metadata/{cruise}.csv; and a cast profile "
        "is /api/ctd/cast/{cruise}/{cast}.csv.\n\n"
        "Return a concise plan for the API client that will execute after your response. "
        "Do not invent measurements or claim to have fetched data. Explain the exact API "
        "requests and processing needed to answer this user request, including how you "
        "would identify winter cruises and station L8 casts:\n"
        "show me the temperature profiles at station L8 for all winter transect cruises. "
        "Order the plots by year."
    )


def codex_agent_prompt(case: dict[str, Any]) -> str:
    """Prompt for an isolated Codex run that must retrieve structured data."""
    return (
        "Use only read-only HTTP requests to the NES-LTER API at "
        f"{API_BASE_URL}. Do not use MCP tools, access the repository, or invent values. "
        "The CTD cruise catalog is /api/ctd/cruises/all; cruise metadata is "
        "/api/ctd/metadata/{cruise}.csv; and cast profiles are "
        "/api/ctd/cast/{cruise}/{cast}.csv. Identify all winter cruises from 2018 through "
        "2026, find casts nearest station L8, retrieve temperature (t090c), bin values "
        "into 5 m depth bins, and order cruises by year. Return only the requested JSON "
        "schema with row_count and data rows containing cruise, cast, depth_m, value, "
        "and sample_count."
    )


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def in_season(value: str | None, year: int, season: str | None) -> bool:
    start = _parse_time(value)
    if start is None:
        return False
    if not season:
        return start.year == year
    if season == "winter":
        return (start.year == year and start.month in {1, 2}) or (start.year == year - 1 and start.month == 12)
    return start.year == year and start.month in SEASON_MONTHS[season]


def bin_profile_rows(rows: list[dict[str, Any]], depth_bin_m: float | None) -> list[dict[str, Any]]:
    if not depth_bin_m:
        return rows
    bins: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        index = int(row["depth_m"] // depth_bin_m)
        bins.setdefault(index, []).append(row)
    return [
        {
            "depth_m": (index + 0.5) * depth_bin_m,
            "value": sum(row["value"] for row in values) / len(values),
            "sample_count": len(values),
        }
        for index, values in sorted(bins.items())
    ]


class MCPProcess:
    def __init__(self, command: list[str], cwd: Path) -> None:
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        self.request_id = 0
        self.request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "nes-lter-benchmark", "version": "1.0"},
        })
        self.notify("notifications/initialized", {})

    def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("MCP process pipes are unavailable")
        self.request_id += 1
        request = {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params}
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError("MCP server exited before returning a response")
            response = json.loads(line)
            if response.get("id") == self.request_id:
                return response

    def notify(self, method: str, params: dict[str, Any]) -> None:
        if self.process.stdin is None:
            raise RuntimeError("MCP process stdin is unavailable")
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        self.process.stdin.flush()

    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.request("tools/call", {"name": tool, "arguments": arguments})

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()


async def direct_l8_winter_temperature(client: httpx.AsyncClient, arguments: dict[str, Any]) -> dict[str, Any]:
    async def get_json(path: str) -> Any:
        response = await client.get(f"{API_BASE_URL}{path}")
        response.raise_for_status()
        return response.json()

    async def get_csv(path: str) -> list[dict[str, str]]:
        response = await client.get(f"{API_BASE_URL}{path}")
        response.raise_for_status()
        return list(csv.DictReader(io.StringIO(response.text)))

    cruises = await get_json("/api/ctd/cruises/all")
    selected = [
        cruise for cruise in cruises
        if cruise.get("type", "").lower() == "nes-lter"
        and any(in_season(cruise.get("start_time"), year, arguments["season"]) for year in range(arguments["year_start"], arguments["year_end"] + 1))
    ]

    async def fetch_cruise(cruise: dict[str, Any]) -> list[dict[str, Any]]:
        name = cruise["name"]
        metadata = await get_csv(f"/api/ctd/metadata/{name}.csv")
        casts = [
            row for row in metadata
            if row.get("nearest_station", "").upper() == arguments["station"].upper()
            and (not row.get("distance_km") or float(row["distance_km"]) <= 3.0)
        ]
        output = []
        for cast in casts:
            cast_number = cast["cast"]
            data = await get_csv(f"/api/ctd/cast/{name}/{cast_number}.csv")
            points = []
            for row in data:
                try:
                    depth = float(row.get("depsm", ""))
                    value = float(row.get("t090c", ""))
                except (TypeError, ValueError):
                    continue
                points.append({"depth_m": depth, "value": value})
            for point in bin_profile_rows(points, arguments["depth_bin_m"]):
                output.append({"cruise": name, "cast": cast_number, **point})
        return output

    rows = [row for group in await asyncio.gather(*(fetch_cruise(cruise) for cruise in selected)) for row in group]
    return {"chart_type": "vertical_profile", "station": arguments["station"], "variable": "temperature", "row_count": len(rows), "data": rows}


async def direct_call(client: httpx.AsyncClient, case: dict[str, Any]) -> dict[str, Any]:
    if case["tool"] == "plot_ctd_profiles":
        return await direct_l8_winter_temperature(client, case["arguments"])
    raise ValueError(f"No direct baseline implemented for {case['tool']!r}")


async def bare_llm_call(client: httpx.AsyncClient, case: dict[str, Any], model: str) -> dict[str, Any]:
    """Measure LLM planning plus direct API retrieval without MCP tools.

    The harness executes the allowlisted direct API workflow after the model
    responds, so this condition returns comparable data rather than prose only.
    """
    api_key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Set LLM_API_KEY or OPENAI_API_KEY to run --mode llm")
    base_url = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    llm_started = time.perf_counter()
    response = await client.post(
        f"{base_url}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "temperature": 0,
            "messages": [{"role": "user", "content": bare_llm_prompt(case)}],
        },
    )
    response.raise_for_status()
    payload = response.json()
    usage = payload.get("usage", {})
    llm_seconds = time.perf_counter() - llm_started
    data_started = time.perf_counter()
    data = await direct_l8_winter_temperature(client, case["arguments"])
    return {
        "answer": payload.get("choices", [{}])[0].get("message", {}).get("content", ""),
        "usage": usage,
        "data": data["data"],
        "row_count": data["row_count"],
        "llm_seconds": llm_seconds,
        "api_seconds": time.perf_counter() - data_started,
    }


def _normalized_usage(usage: dict[str, Any]) -> dict[str, int]:
    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0))
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))
    return {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": usage.get("total_tokens", input_tokens + output_tokens),
    }


def codex_agent_call(command: list[str], case: dict[str, Any], sandbox: str) -> dict[str, Any]:
    """Run Codex non-interactively with MCP/user config excluded."""
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["row_count", "data"],
        "properties": {
            "row_count": {"type": "integer"},
            "data": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["cruise", "cast", "depth_m", "value", "sample_count"],
                    "properties": {
                        "cruise": {"type": "string"},
                        "cast": {"type": "string"},
                        "depth_m": {"type": "number"},
                        "value": {"type": "number"},
                        "sample_count": {"type": "integer"},
                    },
                },
            },
        },
    }
    with tempfile.TemporaryDirectory(prefix="nes-lter-codex-bench-") as directory:
        workdir = Path(directory)
        schema_path = workdir / "schema.json"
        output_path = workdir / "result.json"
        schema_path.write_text(json.dumps(schema))
        completed = subprocess.run(
            [
                *command,
                "--ephemeral",
                "--json",
                "--ignore-user-config",
                "--ignore-rules",
                "--skip-git-repo-check",
                "--sandbox",
                sandbox,
                "--output-schema",
                str(schema_path),
                "-o",
                str(output_path),
                codex_agent_prompt(case),
            ],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"codex exec failed ({completed.returncode}): {completed.stderr[-1000:]}")
        usage: dict[str, Any] = {}
        for line in completed.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "turn.completed":
                usage = _normalized_usage(event.get("usage", {}))
        result = json.loads(output_path.read_text())
        return {**result, "usage": usage}


def mcp_call(server: MCPProcess, case: dict[str, Any]) -> dict[str, Any]:
    return server.call(case["tool"], case["arguments"])


def mcp_payload(response: dict[str, Any]) -> dict[str, Any]:
    """Decode the JSON text content returned by FastMCP tools."""
    content = response.get("result", {}).get("content", [])
    if content and isinstance(content[0], dict) and content[0].get("text"):
        try:
            payload = json.loads(content[0]["text"])
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass
    return {}


def estimate_llm_cost(usage: dict[str, Any]) -> float | None:
    input_rate = os.environ.get("LLM_INPUT_USD_PER_1M")
    output_rate = os.environ.get("LLM_OUTPUT_USD_PER_1M")
    if input_rate is None or output_rate is None:
        return None
    return (
        (usage.get("prompt_tokens", 0) / 1_000_000) * float(input_rate)
        + (usage.get("completion_tokens", 0) / 1_000_000) * float(output_rate)
    )


async def benchmark(args: argparse.Namespace) -> list[dict[str, Any]]:
    root = Path(__file__).resolve().parents[1]
    selected_cases = {name: CASES[name] for name in args.case} if args.case else CASES
    results: list[dict[str, Any]] = []
    server = MCPProcess(args.server_command, root) if args.mode in {"mcp", "both", "all"} else None
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            for name, case in selected_cases.items():
                for _ in range(args.warmups):
                    if args.mode in {"mcp", "both", "all"}:
                        mcp_call(server, case)
                    if args.mode in {"direct", "both", "all"}:
                        await direct_call(client, case)
                    if args.mode in {"llm", "all"}:
                        await bare_llm_call(client, case, args.llm_model)
                    if args.mode in {"codex", "all"}:
                        await asyncio.to_thread(codex_agent_call, args.codex_command, case, args.codex_sandbox)
                for repetition in range(args.repetitions):
                    modes = {"both": ["mcp", "direct"], "all": ["mcp", "direct", "llm", "codex"]}.get(args.mode, [args.mode])
                    for mode in modes:
                        started = time.perf_counter()
                        try:
                            if mode == "mcp":
                                response = mcp_call(server, case)
                            elif mode == "direct":
                                response = await direct_call(client, case)
                            elif mode == "llm":
                                response = await bare_llm_call(client, case, args.llm_model)
                            else:
                                response = await asyncio.to_thread(codex_agent_call, args.codex_command, case, args.codex_sandbox)
                            error = None
                        except Exception as exc:  # benchmark rows should preserve failures
                            response = {"error": str(exc)}
                            error = repr(exc)
                        elapsed = time.perf_counter() - started
                        encoded = json.dumps(response, separators=(",", ":"))
                        payload = mcp_payload(response) if mode == "mcp" else response
                        usage = response.get("usage", {}) if mode == "llm" else {}
                        result = {
                            "case": name,
                            "mode": mode,
                            "repetition": repetition,
                            "elapsed_seconds": elapsed,
                            "response_bytes": len(encoded.encode()),
                            "row_count": payload.get("row_count"),
                            "input_tokens": response.get("usage", {}).get("prompt_tokens") if mode in {"llm", "codex"} else None,
                            "output_tokens": response.get("usage", {}).get("completion_tokens") if mode in {"llm", "codex"} else None,
                            "total_tokens": response.get("usage", {}).get("total_tokens") if mode in {"llm", "codex"} else None,
                            "estimated_cost_usd": estimate_llm_cost(usage),
                            "llm_seconds": response.get("llm_seconds") if mode == "llm" else None,
                            "api_seconds": response.get("api_seconds") if mode == "llm" else None,
                            "error": error,
                        }
                        results.append(result)
                        print(json.dumps(result), flush=True)
    finally:
        if server:
            server.close()
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES), help="Case to run; repeat for multiple cases")
    parser.add_argument("--mode", choices=("mcp", "direct", "llm", "codex", "both", "all"), default="both")
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--output", type=Path, help="Optional JSONL output path")
    parser.add_argument("--server-command", nargs="+", default=SERVER_COMMAND)
    parser.add_argument("--llm-model", default=os.environ.get("LLM_MODEL", "gpt-4o-mini"), help="OpenAI-compatible model for --mode llm/all")
    parser.add_argument("--codex-command", nargs="+", default=["codex", "exec"], help="Codex command for --mode codex/all")
    parser.add_argument("--codex-sandbox", choices=("read-only", "workspace-write", "danger-full-access"), default="read-only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results = asyncio.run(benchmark(args))
    if args.output:
        args.output.write_text("\n".join(json.dumps(row) for row in results) + "\n")
    grouped: dict[str, list[float]] = {}
    for row in results:
        if not row["error"]:
            grouped.setdefault(f"{row['case']}:{row['mode']}", []).append(row["elapsed_seconds"])
    for key, values in grouped.items():
        print(json.dumps({"summary": key, "median_seconds": statistics.median(values), "p95_seconds": sorted(values)[max(0, int(len(values) * 0.95) - 1)]}))


if __name__ == "__main__":
    main()
