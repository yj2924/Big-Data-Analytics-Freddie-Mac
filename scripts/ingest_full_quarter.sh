#!/usr/bin/env bash
# Ingest ONE full-dataset quarter (historical_data_YYYYQn.zip) into the Parquet store, then delete the text.
# Used for the full 2006 / 2012 / 2020 vintages so a single machine never holds more than one quarter of
# raw text at a time (one quarter of 2020 is ~8 GB of text).  On Dataproc the same Python job reads gs://.
#
#   bash scripts/ingest_full_quarter.sh <zip or folder of zip parts> <work_dir> <parquet_root> <stats_csv>
set -euo pipefail
SRC=$1; WORK=$2; OUT=$3; STATS=$4
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY=${PYTHON:-python}
mkdir -p "$WORK"
Q=$(basename "$SRC" | grep -oE '[0-9]{4}Q[1-4]' | head -1)
ZIP="$WORK/historical_data_${Q}.zip"
if ls "$SRC".zip.part* >/dev/null 2>&1; then cat "$SRC".zip.part* > "$ZIP"; elif [ -d "$SRC" ]; then cat "$SRC"/historical_data_${Q}.zip.part* > "$ZIP"; else cp "$SRC" "$ZIP"; fi
ZIP_BYTES=$(stat -c %s "$ZIP")
TXT="$WORK/txt_$Q"; rm -rf "$TXT"; mkdir -p "$TXT"
unzip -o -q "$ZIP" -d "$TXT" && rm -f "$ZIP"
# file names: orig_YYYYQn.txt / perf_YYYYQn.txt (2026 release) or historical_data_YYYYQn.txt / historical_data_time_YYYYQn.txt (older)
PERF=$(ls "$TXT" | grep -E '^perf_|_time_' | head -1); ORIG=$(ls "$TXT" | grep -v -E '^perf_|_time_' | head -1)
ORIG_ROWS=$(wc -l < "$TXT/$ORIG"); PERF_ROWS=$(wc -l < "$TXT/$PERF")
ORIG_B=$(stat -c %s "$TXT/$ORIG"); PERF_B=$(stat -c %s "$TXT/$PERF")
$PY "$HERE/src/a_data/01_ingest_to_parquet.py" --input "$TXT" --output "$OUT" --dataset full --mode append
[ -f "$STATS" ] || echo "quarter,zip_bytes,orig_txt_bytes,perf_txt_bytes,orig_rows,perf_rows" > "$STATS"
echo "$Q,$ZIP_BYTES,$ORIG_B,$PERF_B,$ORIG_ROWS,$PERF_ROWS" >> "$STATS"
rm -rf "$TXT"
echo "done $Q"
