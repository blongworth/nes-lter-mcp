from main import _point_count_note, _truncation_note, POINT_COUNT_WARNING_THRESHOLD


def test_truncation_note_is_none_when_nothing_is_hidden():
    assert _truncation_note(5, 5) is None
    assert _truncation_note(5, 3) is None  # shown can exceed total after filtering; never claim truncation


def test_truncation_note_names_the_param_to_raise():
    note = _truncation_note(2, 21, "limit")
    assert "2 of 21" in note
    assert "limit" in note


def test_truncation_note_default_param_is_limit():
    assert "limit" in _truncation_note(1, 2)


def test_point_count_note_is_none_at_or_below_threshold():
    assert _point_count_note(POINT_COUNT_WARNING_THRESHOLD) is None
    assert _point_count_note(0) is None


def test_point_count_note_warns_above_threshold():
    note = _point_count_note(POINT_COUNT_WARNING_THRESHOLD + 1)
    assert str(POINT_COUNT_WARNING_THRESHOLD + 1) in note
    assert "include_points=False" in note
