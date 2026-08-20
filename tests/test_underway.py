import pytest

from main import _circular_mean, _resolve_underway_variable, _vessel_family


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
