import asyncio

import pytest

import main
from main import KT_TO_MS, _circular_mean, _declared_unit_factor, _resolve_underway_variable, _underway_column_units, _vessel_family


def test_vessel_family_matches_known_vessels():
    assert _vessel_family("Endeavor") == "endeavor"
    assert _vessel_family("Neil Armstrong") == "armstrong"
    assert _vessel_family("Atlantis") == "armstrong"
    assert _vessel_family("Hugh R. Sharp") == "sharp"
    assert _vessel_family("Atlantic Explorer") == "atlantic_explorer"


def test_vessel_family_is_case_insensitive_and_unknown_is_none():
    assert _vessel_family("ENDEAVOR") == "endeavor"
    assert _vessel_family("R/V Some Other Ship") is None
    assert _vessel_family(None) is None


def test_circular_mean_wraps_around_north():
    result = _circular_mean([350.0, 10.0]) % 360
    assert result == pytest.approx(0.0, abs=1e-6) or result == pytest.approx(360.0, abs=1e-6)


def test_circular_mean_of_identical_headings_is_unchanged():
    assert _circular_mean([90.0, 90.0, 90.0]) == pytest.approx(90.0, abs=1e-6)


def test_resolve_underway_variable_picks_first_available_candidate():
    headers = ["wxts_sm", "wxts_dm", "dec_lat", "dec_lon"]
    column, friendly, factor = _resolve_underway_variable("wind speed", "armstrong", headers)
    assert column == "wxts_sm"  # wxtp_sm is preferred but absent from headers
    assert friendly == "wind_speed"
    assert factor == 1.0


def test_resolve_underway_variable_raises_for_missing_column():
    with pytest.raises(ValueError, match="was not found for vessel family"):
        _resolve_underway_variable("wind speed", "endeavor", ["dec_lat", "dec_lon"])


@pytest.mark.parametrize("family", ["sharp", "atlantic_explorer", "endeavor"])
def test_wind_speed_converted_from_knots_to_ms(family):
    headers = {
        "sharp": ["wind1_true_speed_kt"],
        "atlantic_explorer": ["twindspdpri_kts"],
        "endeavor": ["wind_truewindbow_speed"],
    }[family]
    _column, _friendly, factor = _resolve_underway_variable("wind speed", family, headers)
    assert factor == pytest.approx(KT_TO_MS)


@pytest.mark.parametrize("declared_unit", ["Knots", "kts", "KT", "kt."])
def test_declared_unit_factor_recognizes_knots_variants_for_wind_speed(declared_unit):
    assert _declared_unit_factor("wind_speed", declared_unit) == pytest.approx(KT_TO_MS)


def test_declared_unit_factor_recognizes_ms_for_wind_speed():
    assert _declared_unit_factor("wind_speed", "m/s") == pytest.approx(1.0)


def test_declared_unit_factor_returns_none_when_missing_or_unrecognized():
    assert _declared_unit_factor("wind_speed", None) is None
    assert _declared_unit_factor("wind_speed", "") is None
    assert _declared_unit_factor("wind_speed", "furlongs/fortnight") is None


def test_underway_column_units_handles_both_key_casings(monkeypatch):
    async def fake_json(path):
        return [
            {"Name": "Wind-TrueWindBow-Speed", "Units": "Knots"},
            {"name": "wxtp_sm", "units": "m/s"},
            {"Name": "NoUnits", "Units": None},
        ]

    monkeypatch.setattr(main, "_json", fake_json)
    units = asyncio.run(_underway_column_units("EN608"))
    assert units["wind_truewindbow_speed"] == "Knots"
    assert units["wxtp_sm"] == "m/s"
    assert "nounits" not in units


def test_underway_column_units_returns_empty_on_api_failure(monkeypatch):
    async def failing_json(path):
        raise RuntimeError("boom")

    monkeypatch.setattr(main, "_json", failing_json)
    assert asyncio.run(_underway_column_units("EN608", refresh=True)) == {}
