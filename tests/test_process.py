# tests/test_process.py
import os
os.environ.setdefault("MINIO_ROOT_USER", "dummy")
os.environ.setdefault("MINIO_ROOT_PASSWORD", "dummy")

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from spark.process import add_partition_columns, clean_and_dedupe, earthquake_schema, enrich


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("test-process").master("local[1]").getOrCreate()
    yield spark
    spark.stop()


def make_row(**overrides):
    base = {
        "time": datetime(2025, 1, 15, 10, 30, 0),
        "latitude": 35.0, "longitude": -120.0, "depth": 10.0, "mag": 4.5,
        "magType": "mb", "nst": 20, "gap": 100.0, "dmin": 0.5, "rms": 0.5,
        "net": "us", "id": "us1000abcd", "updated": datetime(2025, 1, 15, 12, 0, 0),
        "place": "test place", "type": "earthquake", "horizontalError": 1.0,
        "depthError": 1.0, "magError": 0.1, "magNst": 15, "status": "reviewed",
        "locationSource": "us", "magSource": "us",
    }
    base.update(overrides)
    return base


def test_clean_and_dedupe_drops_null_id(spark):
    rows = [make_row(id=None), make_row(id="us1000abcd")]
    df = spark.createDataFrame(rows, schema=earthquake_schema)
    assert clean_and_dedupe(df).count() == 1


def test_clean_and_dedupe_drops_null_time(spark):
    rows = [make_row(time=None), make_row()]
    df = spark.createDataFrame(rows, schema=earthquake_schema)
    assert clean_and_dedupe(df).count() == 1


def test_clean_and_dedupe_removes_duplicate_ids(spark):
    rows = [make_row(id="us1"), make_row(id="us1"), make_row(id="us2")]
    df = spark.createDataFrame(rows, schema=earthquake_schema)
    assert clean_and_dedupe(df).count() == 2


def test_enrich_mag_category(spark):
    rows = [
        make_row(id="a", mag=3.0), make_row(id="b", mag=4.5),
        make_row(id="c", mag=5.5), make_row(id="d", mag=6.5),
        make_row(id="e", mag=7.5),
    ]
    df = spark.createDataFrame(rows, schema=earthquake_schema)
    result = {r["id"]: r["mag_category"] for r in enrich(df).select("id", "mag_category").collect()}
    assert result == {"a": "minor", "b": "light", "c": "moderate", "d": "strong", "e": "major"}


def test_enrich_depth_category(spark):
    rows = [make_row(id="a", depth=10.0), make_row(id="b", depth=100.0), make_row(id="c", depth=400.0)]
    df = spark.createDataFrame(rows, schema=earthquake_schema)
    result = {r["id"]: r["depth_category"] for r in enrich(df).select("id", "depth_category").collect()}
    assert result == {"a": "shallow", "b": "intermediate", "c": "deep"}


def test_add_partition_columns_zero_pads(spark):
    rows = [make_row(id="a", time=datetime(2025, 1, 5, 0, 0, 0))]
    df = spark.createDataFrame(rows, schema=earthquake_schema)
    result = add_partition_columns(df).select("year", "month", "day").collect()[0]
    assert result["year"] == 2025 and result["month"] == "01" and result["day"] == "05"