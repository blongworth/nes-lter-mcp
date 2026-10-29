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
    headers = ["wxts_ts", "wxts_td", "dec_lat", "dec_lon"]
    column, friendly, factor = _resolve_underway_variable("wind speed", "armstrong", headers)
    assert column == "wxts_ts"  # wxtp_ts is preferred but absent from headers
    assert friendly == "wind_speed"
    assert factor == 1.0


def test_armstrong_wind_uses_true_not_relative_columns():
    headers = ["wxtp_dm", "wxtp_td", "wxtp_sm", "wxtp_ts"]
    column, _friendly, _factor = _resolve_underway_variable("wind direction", "armstrong", headers)
    assert column == "wxtp_td"
    column, _friendly, _factor = _resolve_underway_variable("wind speed", "armstrong", headers)
    assert column == "wxtp_ts"


def test_underway_cruise_values_drops_fill_values(monkeypatch):
    async def fake_vessel(cruise_name, refresh=False):
        return "sharp"

    async def fake_rows(dataset, cruise_name, refresh=False):
        rows = [
            {"date": "2023-05-01T00:00:00Z", "true_wind_speed__knots": "10", "latitude_deg": "41", "longitude_deg": "-70"},
            {"date": "2023-05-01T00:01:00Z", "true_wind_speed__knots": "-99", "latitude_deg": "-99", "longitude_deg": "-99"},
        ]
        return rows, "url"

    async def fake_units(cruise_name, refresh=False):
        return {"true_wind_speed__knots": "knots"}

    monkeypatch.setattr(main, "_vessel_for_cruise", fake_vessel)
    monkeypatch.setattr(main, "_dataset_rows", fake_rows)
    monkeypatch.setattr(main, "_underway_column_units", fake_units)
    result = asyncio.run(main._underway_cruise_values("HRS2303", "wind speed", None, None, False))
    assert [v["value"] for v in result["values"]] == [pytest.approx(10 * KT_TO_MS)]


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


# HRS2303 predates Sharp's current SCS column naming (e.g. HRS2606+) and uses this
# older, distinct set of headers with only one true-wind sensor instead of two.
LEGACY_SHARP_HEADERS = [
    "date", "latitude_deg", "longitude_deg", "cog_deg", "sog_knots", "sog_kts",
    "depth_meters", "relative_wind_speed_1_knots", "relative_wind_direction_1_deg",
    "true_wind_speed__knots", "true_wind_direction_deg", "air_temperature_c",
    "humidity_", "barometer_decibars", "water_temperature_degree_c", "salinity_psu",
]


@pytest.mark.parametrize(
    "variable,expected_column,expected_factor",
    [
        ("wind speed", "true_wind_speed__knots", KT_TO_MS),
        ("wind direction", "true_wind_direction_deg", 1.0),
        ("course over ground", "cog_deg", 1.0),
        ("air_temperature", "air_temperature_c", 1.0),
        ("relative_humidity", "humidity_", 1.0),
        ("barometric_pressure", "barometer_decibars", 1.0),
        ("latitude", "latitude_deg", 1.0),
        ("longitude", "longitude_deg", 1.0),
    ],
)
def test_resolve_underway_variable_falls_back_to_legacy_sharp_columns(variable, expected_column, expected_factor):
    column, _friendly, factor = _resolve_underway_variable(variable, "sharp", LEGACY_SHARP_HEADERS)
    assert column == expected_column
    assert factor == pytest.approx(expected_factor)


def test_resolve_underway_variable_prefers_current_sharp_columns_when_present():
    headers = ["wind1_true_speed_kt", "true_wind_speed__knots"]
    column, _friendly, _factor = _resolve_underway_variable("wind speed", "sharp", headers)
    assert column == "wind1_true_speed_kt"
