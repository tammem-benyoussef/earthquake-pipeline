#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Building earthquake-ingestion..."
docker build -t earthquake-ingestion -f ingestion/Dockerfile .

echo "Building earthquake-spark..."
docker build -t earthquake-spark -f spark/Dockerfile .

echo "Building dbt-earthquakes..."
docker build -t dbt-earthquakes ./dbt_earthquakes

echo "All images rebuilt."