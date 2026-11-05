#!/usr/bin/env bash
# Run Member A's pipeline end to end on one machine (Spark local mode).
#   bash scripts/run_local.sh sample      # 50k-loan-per-vintage sample, all vintages 1999-2026
#   bash scripts/run_local.sh full        # full 2006 / 2012 / 2020 vintages (ingest them first, see README)
set -euo pipefail
TAG=${1:-sample}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA=${DATA:-$ROOT/../data}
PY=${PYTHON:-python}
export SPARK_MASTER=${SPARK_MASTER:-local[*]} SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-5g}

if [ "$TAG" = "sample" ]; then
  $PY "$ROOT/src/a_data/01_ingest_to_parquet.py" --input "$DATA/raw_txt/sample" --output "$DATA/parquet/sample" --dataset sample
fi
$PY "$ROOT/src/a_data/02_build_panel.py"     --parquet "$DATA/parquet/$TAG" --output "$DATA/model/$TAG"
$PY "$ROOT/src/a_data/03_data_profile.py"    --parquet "$DATA/parquet/$TAG" --dataset "$TAG" --out "$ROOT/results"
$PY "$ROOT/src/a_model/04_eda.py"            --parquet "$DATA/parquet/$TAG" --model "$DATA/model/$TAG" --tag "$TAG" --out "$ROOT/results"
$PY "$ROOT/src/a_model/05_baseline_models.py" --parquet "$DATA/parquet/$TAG" --model "$DATA/model/$TAG" --tag "$TAG" --out "$ROOT/results"
