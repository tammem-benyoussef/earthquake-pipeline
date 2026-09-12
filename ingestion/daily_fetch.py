import os
import time
from datetime import date, timedelta

import boto3
import click
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
BUCKET = "raw"
S3_PREFIX = "earthquakes"

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
)


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


def get_object_key(day: date) -> str:
    return (
        f"{S3_PREFIX}/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}.csv"
    )


def object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def save_day(day: date) -> str:
    key = get_object_key(day)
    if object_exists(key):
        click.echo(f"skip {day} (already exists)")
        return "skipped"

    try:
        csv_text = fetch_earthquakes(day, day + timedelta(days=1))
    except requests.RequestException as e:
        click.echo(f"ERROR fetching {day}: {e}", err=True)
        return "error"

    is_empty = csv_text.count("\n") <= 1

    s3.put_object(Bucket=BUCKET, Key=key, Body=csv_text.encode("utf-8"))
    click.echo(f"saved {day} -> s3://{BUCKET}/{key}")
    return "empty" if is_empty else "saved"


@click.command()
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
def main(start_date, end_date):
    yesterday = date.today() - timedelta(days=1)
    day = start_date.date() if start_date else yesterday
    end = end_date.date() if end_date else yesterday
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