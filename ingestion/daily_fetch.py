# ingestion/daily_fetch.py
import time
from datetime import date, timedelta
from pathlib import Path

import click
import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
RAW_DIR = Path("data/raw")


def fetch_earthquakes(start_date: date, end_date: date) -> str:
    """Fetch events in [start_date, end_date) as raw CSV text."""
    params = {
        "format": "csv",
        "starttime": start_date.isoformat(),
        "endtime": end_date.isoformat(),
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.text


def get_partition_path(day: date) -> Path:
    return (
        RAW_DIR
        / f"year={day.year:04d}"
        / f"month={day.month:02d}"
        / f"day={day.day:02d}.csv"
    )


def save_day(day: date) -> str:
    path = get_partition_path(day)
    if path.exists():
        click.echo(f"skip {day} (already exists)")
        return "skipped"

    try:
        csv_text = fetch_earthquakes(day, day + timedelta(days=1))
    except requests.RequestException as e:
        click.echo(f"ERROR fetching {day}: {e}", err=True)
        return "error"

    is_empty = csv_text.count("\n") <= 1

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(csv_text, encoding="utf-8")
    click.echo(f"saved {day} -> {path}")
    return "empty" if is_empty else "saved"


@click.command()
@click.option("--start-date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
def main(start_date, end_date):
    day = start_date.date()
    end = end_date.date()
    empty_days = []

    while day <= end:
        status = save_day(day)
        if status == "empty":
            empty_days.append(day)
        time.sleep(0.2)
        day += timedelta(days=1)

    if empty_days:
        click.echo(f"\n{len(empty_days)} day(s) with no events:")
        for d in empty_days:
            click.echo(f"  {d}")


if __name__ == "__main__":
    main()