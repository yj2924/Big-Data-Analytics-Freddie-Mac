#!/usr/bin/env bash
# Run Member A's full pipeline on Google Colab (sample data, all vintages 1999-2026).
# Expects the Freddie Mac sample zips in /content/upload, either whole (sample_YYYY.zip) or split into
# parts (sample_YYYY.zip.partNN, created with `split -b 9500k`) because browser uploads are size-limited.
#   !bash /content/mortgage-credit-risk/scripts/colab_run.sh
set -euo pipefail
REPO=$(cd "$(dirname "$0")/.." && pwd)
UP=${UPLOAD_DIR:-/content/upload}
DATA=${DATA:-/content/data}
mkdir -p "$DATA/raw_zip" "$DATA/raw_txt/sample"

echo "== 1. reassemble and unzip the sample files"
for f in $(ls "$UP" | sed -E 's/\.part[0-9]+$//' | sort -u); do
  if ls "$UP/$f".part* >/dev/null 2>&1; then cat "$UP/$f".part* > "$DATA/raw_zip/$f"; else cp "$UP/$f" "$DATA/raw_zip/$f"; fi
done
ls "$DATA/raw_zip" | wc -l
for z in "$DATA"/raw_zip/sample_*.zip; do unzip -o -q "$z" -d "$DATA/raw_txt/sample"; done
du -sh "$DATA/raw_txt/sample"

echo "== 2. environment"
pip install -q "pyspark==3.5.3" setuptools
java -version 2>&1 | head -1
nproc; free -g | head -2

echo "== 3. pipeline"
export SPARK_MASTER="local[*]" SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-9g} SPARK_SHUFFLE_PARTITIONS=64
export DATA PYTHON=python
cd "$REPO"
START=$(date +%s)
time python src/a_data/01_ingest_to_parquet.py --input "$DATA/raw_txt/sample" --output "$DATA/parquet/sample" --dataset sample
time python src/a_data/02_build_panel.py --parquet "$DATA/parquet/sample" --output "$DATA/model/sample"
time python src/a_data/03_data_profile.py --parquet "$DATA/parquet/sample" --dataset sample --out results_colab
time python src/a_model/04_eda.py --parquet "$DATA/parquet/sample" --model "$DATA/model/sample" --tag sample --out results_colab
time python src/a_model/05_baseline_models.py --parquet "$DATA/parquet/sample" --model "$DATA/model/sample" --tag sample --out results_colab
echo "total pipeline minutes: $(( ($(date +%s) - START) / 60 ))"
cd results_colab && zip -q -r /content/results_colab.zip . && ls -la /content/results_colab.zip
