from main import _cruise_matches


def test_cruise_without_filters_matches_full_catalog():
    cruise = {
        "name": "AR16",
        "vessel_name": "Neil Armstrong",
        "type": "Opportunistic",
        "start_time": "2017-05-03T13:59:18.313Z",
        "end_time": "2017-05-22T12:06:32.886Z",
    }
    assert _cruise_matches(cruise, None, None, None)


def test_cruise_season_filter_can_span_all_years():
    cruise = {"type": "NES-LTER", "start_time": "2025-01-24T15:06:43Z"}
    assert _cruise_matches(cruise, None, "winter", None)
    assert _cruise_matches(cruise, 2025, "winter", "NES-LTER")
    assert not _cruise_matches(cruise, 2024, "winter", None)
