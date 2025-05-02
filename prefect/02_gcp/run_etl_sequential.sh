#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 YEAR MONTH [MONTH...]" >&2
  exit 1
fi

YEAR=$1; shift
MONTHS=("$@")

for MONTH in "${MONTHS[@]}"; do
  echo "👉 Processing ${YEAR}-${MONTH:0:2}..."
  python etl_gcs_to_bq_optimized.py "$YEAR" "$MONTH"
  echo "✔ Finished ${YEAR}-${MONTH:0:2}"
done

#bash run_etl_sequential.sh 2019 1 2 3 4 5 6
