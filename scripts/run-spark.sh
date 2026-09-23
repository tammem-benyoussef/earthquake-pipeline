#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

START_DATE="${1:-$(date -d yesterday +%F)}"
END_DATE="${2:-$(date -d yesterday +%F)}"

docker run --rm --network host --env-file .env \
    earthquake-spark --start-date "$START_DATE" --end-date "$END_DATE"