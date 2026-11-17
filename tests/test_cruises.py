import asyncio

import main


def _capture_get(monkeypatch):
    calls = []

    async def fake_get(path, params=None):
        calls.append((path, params))
        return {"count": 0, "cruises": []}

    monkeypatch.setattr(main, "_get", fake_get)
    monkeypatch.setattr(main, "_cache", main._TTLCache())
    return calls


def test_find_cruises_without_filters_requests_full_catalog(monkeypatch):
    calls = _capture_get(monkeypatch)
    asyncio.run(main.find_cruises())
    path, params = calls[0]
    assert path == "/cruises"
    assert main._url(path, params).endswith("/cruises")  # no type=NES-LTER default: all cruise types


def test_find_cruises_season_without_year_spans_all_years(monkeypatch):
    calls = _capture_get(monkeypatch)
    asyncio.run(main.find_cruises(season="winter"))
    assert main._url(*calls[0]).endswith("/cruises?season=winter")


def test_find_cruises_passes_all_filters(monkeypatch):
    calls = _capture_get(monkeypatch)
    asyncio.run(main.find_cruises(2025, "winter", "NES-LTER"))
    assert main._url(*calls[0]).endswith("/cruises?year=2025&season=winter&type=NES-LTER")
