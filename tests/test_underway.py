import asyncio

import pytest

import main
from main import UNDERWAY_FRIENDLY_SYNONYMS, _circular_mean


def test_circular_mean_wraps_around_north():
    result = _circular_mean([350.0, 10.0]) % 360
    assert result == pytest.approx(0.0, abs=1e-6) or result == pytest.approx(360.0, abs=1e-6)


def test_circular_mean_of_identical_headings_is_unchanged():
    assert _circular_mean([90.0, 90.0, 90.0]) == pytest.approx(90.0, abs=1e-6)


@pytest.mark.parametrize(
    "friendly,api_name",
    [
        ("wind speed", "true_wind_speed"),
        ("wind_direction", "true_wind_direction"),
        ("sst", "sea_surface_temperature"),
        ("temperature", "sea_surface_temperature"),
        ("sal", "sea_surface_salinity"),
        ("rh", "relative_humidity"),
        ("cog", "course_over_ground"),
        ("sog", "speed_over_ground"),
    ],
)
def test_friendly_underway_names_map_to_api_variables(friendly, api_name):
    assert UNDERWAY_FRIENDLY_SYNONYMS[friendly] == api_name


@pytest.mark.parametrize("raw", ["-99", "-99.0", -99, -99.0, "-999", -999.0, "-9999", "-9999.0"])
def test_float_treats_missing_sentinel_as_none(raw):
    assert main._float(raw) is None


def test_float_keeps_values_near_sentinel():
    assert main._float("-99.5") == -99.5
    assert main._float("99") == 99.0
    assert main._float("-998") == -998.0


def _fake_underway(monkeypatch, rows, units="m/s"):
    async def fake_data_rows(dataset, params, refresh=False):
        variable = params["variables"]
        return rows, {"build_id": "b1", "columns": [{"name": variable, "units": units}]}

    async def fake_cached(path, params=None, refresh=False):
        return {"cruise": "hrs2303", "vessel": "hrs"}

    monkeypatch.setattr(main, "_data_rows", fake_data_rows)
    monkeypatch.setattr(main, "_cached", fake_cached)


def test_underway_cruise_values_drops_fill_values(monkeypatch):
    # HRS2303 records missing wind as -99 knots, which the API converts to -50.93 m/s.
    rows = [
        {"time": "2023-05-01T00:00:00Z", "true_wind_speed": "5.1", "latitude": 41.0, "longitude": -70.0},
        {"time": "2023-05-01T00:01:00Z", "true_wind_speed": str(-99 * 0.514444), "latitude": 41.0, "longitude": -70.0},
        {"time": "2023-05-01T00:02:00Z", "true_wind_speed": None, "latitude": 41.0, "longitude": -70.0},
    ]
    _fake_underway(monkeypatch, rows)
    result = asyncio.run(main._underway_cruise_values("hrs2303", "true_wind_speed", None, None, False))
    assert [v["value"] for v in result["values"]] == [pytest.approx(5.1)]
    assert result["dropped"] == 1  # the converted fill value, not the null
    assert result["unit"] == "m/s"
    assert result["vessel"] == "hrs"


def test_underway_cruise_values_drops_raw_sentinel_without_range(monkeypatch):
    rows = [
        {"time": "t0", "air_temperature": -99.0, "latitude": 41.0, "longitude": -70.0},
        {"time": "t1", "air_temperature": 11.2, "latitude": -99.0, "longitude": -70.0},
    ]
    _fake_underway(monkeypatch, rows, units="degC")
    result = asyncio.run(main._underway_cruise_values("hrs2303", "air_temperature", None, None, False))
    assert [v["value"] for v in result["values"]] == [11.2]
    assert result["values"][0]["latitude"] is None
    assert result["dropped"] == 1


def test_underway_direction_out_of_range_dropped(monkeypatch):
    rows = [{"time": "t0", "true_wind_direction": "370"}, {"time": "t1", "true_wind_direction": "350"}]
    _fake_underway(monkeypatch, rows, units="degrees")
    result = asyncio.run(main._underway_cruise_values("x", "true_wind_direction", None, None, False))
    assert [v["value"] for v in result["values"]] == [350.0]
    assert result["kind"] == "circular"
