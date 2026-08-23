from benchmarks.bench_queries import bin_profile_rows, bare_llm_prompt, codex_agent_prompt, in_season


def test_winter_includes_january_and_prior_december():
    assert in_season("2026-01-13T15:15:41Z", 2026, "winter")
    assert in_season("2025-12-20T15:15:41Z", 2026, "winter")
    assert not in_season("2025-02-20T15:15:41Z", 2026, "winter")


def test_bin_profile_rows_orders_bins_and_counts_samples():
    rows = [
        {"depth_m": 8.0, "value": 10.0},
        {"depth_m": 3.0, "value": 6.0},
        {"depth_m": 9.0, "value": 14.0},
    ]

    assert bin_profile_rows(rows, 5) == [
        {"depth_m": 2.5, "value": 6.0, "sample_count": 1},
        {"depth_m": 7.5, "value": 12.0, "sample_count": 2},
    ]


def test_bare_llm_prompt_contains_only_api_discovery_context():
    prompt = bare_llm_prompt({})
    assert "https://nes-lter-api.whoi.edu" in prompt
    assert "/api/ctd/cruises/all" in prompt
    assert "must not claim to have fetched data" in prompt
    assert "plan for the API client" in prompt


def test_codex_prompt_requires_structured_api_data_without_mcp():
    prompt = codex_agent_prompt({})
    assert "Do not use MCP tools" in prompt
    assert "row_count and data rows" in prompt
    assert "t090c" in prompt
