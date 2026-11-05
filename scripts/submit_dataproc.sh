#!/usr/bin/env bash
# Submit Member A's jobs to a Dataproc cluster (the GCS + Dataproc pipeline Member B sets up).
# Raw text files are expected under gs://$BUCKET/freddie/raw/<sample|full>/ ; outputs go to gs://$BUCKET/freddie/.
#   CLUSTER=mortgage-cluster REGION=us-east1 BUCKET=my-bucket bash scripts/submit_dataproc.sh sample
set -euo pipefail
TAG=${1:-sample}
: "${CLUSTER:?set CLUSTER}" "${REGION:?set REGION}" "${BUCKET:?set BUCKET}"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
(cd "$ROOT/src" && zip -qr /tmp/common.zip common)
G=gs://$BUCKET/freddie
run() { gcloud dataproc jobs submit pyspark "$ROOT/$1" --cluster "$CLUSTER" --region "$REGION" \
          --py-files /tmp/common.zip -- "${@:2}"; }

run src/a_data/01_ingest_to_parquet.py  --input "$G/raw/$TAG" --output "$G/parquet/$TAG" --dataset "$TAG"
run src/a_data/02_build_panel.py        --parquet "$G/parquet/$TAG" --output "$G/model/$TAG"
run src/a_data/03_data_profile.py       --parquet "$G/parquet/$TAG" --dataset "$TAG" --out /tmp/results
# 04/05 write CSV/PNG with pandas on the driver; on Dataproc copy them back with gsutil afterwards:
run src/a_model/05_baseline_models.py   --parquet "$G/parquet/$TAG" --model "$G/model/$TAG" --tag "$TAG" --out /tmp/results
