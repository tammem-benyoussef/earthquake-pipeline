from datetime import date
from pathlib import Path

import pytest
import requests

from ingestion import daily_fetch


def test_get_partition_path():
    path = daily_fetch.get_partition_path(date(2026, 1, 3))
    assert path == daily_fetch.RAW_DIR / "year=2026" / "month=01" / "day=03.csv"


def test_save_day_skips_existing(tmp_path, monkeypatch):
    monkeypatch.setattr(daily_fetch, "RAW_DIR", tmp_path)
    day = date(2026, 1, 3)
    existing = daily_fetch.get_partition_path(day)
    existing.parent.mkdir(parents=True)
    existing.write_text("already here")

    def fail_if_called(*a, **kw):
        raise AssertionError("fetch_earthquakes should not be called")

    monkeypatch.setattr(daily_fetch, "fetch_earthquakes", fail_if_called)
    status = daily_fetch.save_day(day)
    assert status == "skipped"


def test_save_day_saves_new_file(tmp_path, monkeypatch):
    monkeypatch.setattr(daily_fetch, "RAW_DIR", tmp_path)
    monkeypatch.setattr(
        daily_fetch, "fetch_earthquakes",
        lambda start, end: "id,mag\nusgs1,4.5\nusgs2,3.1\n"
    )
    status = daily_fetch.save_day(date(2026, 1, 3))
    assert status == "saved"
    assert daily_fetch.get_partition_path(date(2026, 1, 3)).exists()


def test_save_day_detects_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(daily_fetch, "RAW_DIR", tmp_path)
    monkeypatch.setattr(daily_fetch, "fetch_earthquakes", lambda s, e: "id,mag\n")
    status = daily_fetch.save_day(date(2026, 1, 3))
    assert status == "empty"


def test_save_day_handles_fetch_error(tmp_path, monkeypatch):
    monkeypatch.setattr(daily_fetch, "RAW_DIR", tmp_path)

    def raise_error(start, end):
        raise requests.RequestException("boom")

    monkeypatch.setattr(daily_fetch, "fetch_earthquakes", raise_error)
    status = daily_fetch.save_day(date(2026, 1, 3))
    assert status == "error"
    assert not daily_fetch.get_partition_path(date(2026, 1, 3)).exists()


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