#!/usr/bin/env python

import os
from datetime import date, timedelta

import boto3
import click
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, dayofweek, hour, lpad, lit, month, when, year
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

RAW_PATH = "s3a://raw"
PROCESSED_PATH = "s3a://processed/earthquakes"
RAW_BUCKET = "raw"

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
)

earthquake_schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("mag", DoubleType(), True),
    StructField("magType", StringType(), True),
    StructField("nst", IntegerType(), True),
    StructField("gap", DoubleType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("rms", DoubleType(), True),
    StructField("net", StringType(), True),
    StructField("id", StringType(), True),
    StructField("updated", TimestampType(), True),
    StructField("place", StringType(), True),
    StructField("type", StringType(), True),
    StructField("horizontalError", DoubleType(), True),
    StructField("depthError", DoubleType(), True),
    StructField("magError", DoubleType(), True),
    StructField("magNst", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("locationSource", StringType(), True),
    StructField("magSource", StringType(), True),
])


def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("earthquake-processing")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def read_raw_partitions(spark, start_date: date, end_date: date):
    paths = []
    day = start_date
    while day <= end_date:
        key = (
            f"earthquakes/year={day.year:04d}/month={day.month:02d}/"
            f"day={day.day:02d}.csv"
        )
        try:
            s3.head_object(Bucket=RAW_BUCKET, Key=key)
        except s3.exceptions.ClientError:
            click.echo(f"WARNING skipping {day}: raw file not found", err=True)
        else:
            paths.append(f"{RAW_PATH}/{key}")
        day += timedelta(days=1)

    if not paths:
        raise click.ClickException(
            f"No raw earthquake files found between {start_date} and {end_date}"
        )

    return spark.read.csv(paths, header=True, schema=earthquake_schema)


def clean_and_dedupe(df):
    return df.filter(col("id").isNotNull() & col("time").isNotNull()).dropDuplicates(["id"])


def enrich(df):
    return (
        df
        .withColumn(
            "mag_category",
            when(col("mag") < 4, "minor")
            .when(col("mag") < 5, "light")
            .when(col("mag") < 6, "moderate")
            .when(col("mag") < 7, "strong")
            .otherwise("major")
        )
        .withColumn(
            "depth_category",
            when(col("depth") < 70, "shallow")
            .when(col("depth") < 300, "intermediate")
            .otherwise("deep")
        )
        .withColumn("estimated_energy_joules", lit(10.0) ** (1.5 * col("mag") + 4.8))
        .withColumn("hour_of_day", hour(col("time")))
        .withColumn("day_of_week", dayofweek(col("time")))
    )


def add_partition_columns(df):
    return (
        df
        .withColumn("year", year(col("time")))
        .withColumn("month", lpad(month(col("time")), 2, "0"))
        .withColumn("day", lpad(dayofmonth(col("time")), 2, "0"))
    )


def write_parquet(df):
    df.coalesce(1).write.mode("overwrite").partitionBy("year", "month", "day").parquet(PROCESSED_PATH)


def write_to_postgres(df, jdbc_url, jdbc_properties):
    columns = [
        "id", "time", "latitude", "longitude", "depth", "mag",
        "magType", "place", "type", "status", "net", "updated",
        "mag_category", "depth_category", "estimated_energy_joules",
        "hour_of_day", "day_of_week",
    ]
    connection_url = jdbc_url.removeprefix("jdbc:")

    conn = psycopg2.connect(
        connection_url,
        user=jdbc_properties["user"],
        password=jdbc_properties["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS staging.earthquakes (
                    id TEXT PRIMARY KEY,
                    time TIMESTAMP,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    depth DOUBLE PRECISION,
                    mag DOUBLE PRECISION,
                    "magType" TEXT,
                    place TEXT,
                    type TEXT,
                    status TEXT,
                    net TEXT,
                    updated TIMESTAMP,
                    mag_category TEXT,
                    depth_category TEXT,
                    estimated_energy_joules DOUBLE PRECISION,
                    hour_of_day INTEGER,
                    day_of_week INTEGER
                );
            """)
        conn.commit()
    finally:
        conn.close()

    df.select(*columns).write.mode("overwrite").jdbc(
        jdbc_url,
        "staging.earthquakes_scratch",
        properties=jdbc_properties,
    )

    conn = psycopg2.connect(
        connection_url,
        user=jdbc_properties["user"],
        password=jdbc_properties["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO staging.earthquakes
                SELECT * FROM staging.earthquakes_scratch
                ON CONFLICT (id) DO UPDATE SET
                    time = EXCLUDED.time,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    depth = EXCLUDED.depth,
                    mag = EXCLUDED.mag,
                    "magType" = EXCLUDED."magType",
                    place = EXCLUDED.place,
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    net = EXCLUDED.net,
                    updated = EXCLUDED.updated,
                    mag_category = EXCLUDED.mag_category,
                    depth_category = EXCLUDED.depth_category,
                    estimated_energy_joules = EXCLUDED.estimated_energy_joules,
                    hour_of_day = EXCLUDED.hour_of_day,
                    day_of_week = EXCLUDED.day_of_week;
            """)
        conn.commit()
    finally:
        conn.close()


@click.command()
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
def main(start_date, end_date):
    yesterday = date.today() - timedelta(days=1)
    start = start_date.date() if start_date else yesterday
    end = end_date.date() if end_date else yesterday
    click.echo(f"processing earthquakes from {start} through {end}")

    spark = build_spark_session()
    try:
        raw = read_raw_partitions(spark, start, end)
        cleaned = clean_and_dedupe(raw)
        click.echo(f"cleaned and deduplicated: {cleaned.count()} rows")

        final = add_partition_columns(enrich(cleaned))
        click.echo(f"enriched and partitioned: {final.count()} rows")
        write_parquet(final)
        click.echo("completed Parquet write")

        jdbc_url = "jdbc:postgresql://localhost:5432/earthquake"
        jdbc_properties = {
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
        }
        write_to_postgres(final, jdbc_url, jdbc_properties)
        click.echo("completed PostgreSQL write")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
