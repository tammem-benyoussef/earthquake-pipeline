#!/bin/bash
set -euo pipefail

TARGET="${1:-dev}"
FULL_REFRESH="${2:-}"

docker run --rm --network host --env-file .env \
    -v ~/.dbt:/root/.dbt \
    dbt-earthquakes build --target "$TARGET" $FULL_REFRESH