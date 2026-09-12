# tests/test_daily_fetch.py
from datetime import date
from unittest.mock import MagicMock

import pytest
import requests

from ingestion import daily_fetch


def test_get_object_key():
    key = daily_fetch.get_object_key(date(2026, 1, 3))
    assert key == "earthquakes/year=2026/month=01/day=03.csv"


def test_save_day_skips_existing(monkeypatch):
    monkeypatch.setattr(daily_fetch, "object_exists", lambda key: True)

    def fail_if_called(*a, **kw):
        raise AssertionError("fetch_earthquakes should not be called")

    monkeypatch.setattr(daily_fetch, "fetch_earthquakes", fail_if_called)
    status = daily_fetch.save_day(date(2026, 1, 3))
    assert status == "skipped"


def test_save_day_saves_new_file(monkeypatch):
    monkeypatch.setattr(daily_fetch, "object_exists", lambda key: False)
    monkeypatch.setattr(
        daily_fetch, "fetch_earthquakes",
        lambda start, end: "id,mag\nusgs1,4.5\nusgs2,3.1\n"
    )
    mock_put = MagicMock()
    monkeypatch.setattr(daily_fetch.s3, "put_object", mock_put)

    status = daily_fetch.save_day(date(2026, 1, 3))

    assert status == "saved"
    mock_put.assert_called_once()
    call_kwargs = mock_put.call_args.kwargs
    assert call_kwargs["Bucket"] == "raw"
    assert call_kwargs["Key"] == "earthquakes/year=2026/month=01/day=03.csv"


def test_save_day_detects_empty(monkeypatch):
    monkeypatch.setattr(daily_fetch, "object_exists", lambda key: False)
    monkeypatch.setattr(daily_fetch, "fetch_earthquakes", lambda s, e: "id,mag\n")
    monkeypatch.setattr(daily_fetch.s3, "put_object", MagicMock())

    status = daily_fetch.save_day(date(2026, 1, 3))
    assert status == "empty"


def test_save_day_handles_fetch_error(monkeypatch):
    monkeypatch.setattr(daily_fetch, "object_exists", lambda key: False)

    def raise_error(start, end):
        raise requests.RequestException("boom")

    monkeypatch.setattr(daily_fetch, "fetch_earthquakes", raise_error)
    mock_put = MagicMock()
    monkeypatch.setattr(daily_fetch.s3, "put_object", mock_put)

    status = daily_fetch.save_day(date(2026, 1, 3))

    assert status == "error"
    mock_put.assert_not_called()


def test_fetch_earthquakes_sends_correct_params(monkeypatch):
    captured = {}

    class FakeResponse:
        text = "id,mag\n"
        def raise_for_status(self): pass

    def fake_get(url, params, timeout):
        captured["params"] = params
        return FakeResponse()

    monkeypatch.setattr(daily_fetch.requests, "get", fake_get)
    daily_fetch.fetch_earthquakes(date(2026, 1, 3), date(2026, 1, 4))
    assert captured["params"]["starttime"] == "2026-01-03"
    assert captured["params"]["endtime"] == "2026-01-04"