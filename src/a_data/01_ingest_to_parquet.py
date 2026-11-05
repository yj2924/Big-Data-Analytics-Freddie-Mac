"""Step 1 (Member A): raw Freddie Mac text files -> typed Parquet.

Works for both the sample files (sample_orig_YYYY.txt / sample_perf_YYYY.txt) and the full
quarterly files (historical_data_YYYYQn.txt / historical_data_time_YYYYQn.txt, or the newer
orig_YYYYQn.txt / perf_YYYYQn.txt names).  Input may be a local folder or a gs:// prefix.

Usage
  spark-submit 01_ingest_to_parquet.py --input data/raw_txt/sample --output data/parquet/sample --dataset sample
  spark-submit 01_ingest_to_parquet.py --input gs://BUCKET/freddie/raw/2006 --output gs://BUCKET/freddie/parquet/full --dataset full

Output (partitioned by vintage = origination year, so later jobs read only the years they need)
  <output>/orig/vintage=YYYY/*.parquet      one row per loan
  <output>/perf/vintage=YYYY/*.parquet      one row per loan x reporting month
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from pyspark.sql import functions as F, types as T                        # noqa: E402
from common.freddie_layout import ORIG_COLS, PERF_COLS, ORIG_KEEP, PERF_KEEP  # noqa: E402
from common.spark_utils import get_spark, na_to_null, yyyymm_to_date, vintage_from_loan_id  # noqa: E402

ORIG_PATTERNS = ["sample_orig_*.txt", "orig_*.txt", "historical_data_[0-9]*Q[0-9].txt"]
PERF_PATTERNS = ["sample_perf_*.txt", "sample_svcg_*.txt", "perf_*.txt", "historical_data_time_*.txt"]


def read_pipe(spark, paths, n_cols):
    schema = T.StructType([T.StructField(f"c{i}", T.StringType(), True) for i in range(n_cols)])
    return (spark.read.option("sep", "|").option("header", False).option("mode", "PERMISSIVE")
            .schema(schema).csv(paths))


def existing(spark, base, patterns):
    """Return the glob patterns that match at least one file (works for local paths and gs://)."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    out = []
    for p in patterns:
        path = jvm.org.apache.hadoop.fs.Path(base.rstrip("/") + "/" + p)
        fs = path.getFileSystem(hconf)
        st = fs.globStatus(path)
        if st is not None and len(st) > 0:
            out.append(base.rstrip("/") + "/" + p)
    return out


def clean_orig(df):
    d = df.toDF(*ORIG_COLS[: len(df.columns)])
    year, quarter = vintage_from_loan_id(F.col("loan_id"))
    return (d.select(
        F.trim("loan_id").alias("loan_id"),
        na_to_null(F.col("fico"), 9999).cast("int").alias("fico"),
        yyyymm_to_date(F.col("first_pay_date")).alias("first_pay_date"),
        na_to_null(F.col("first_time_buyer"), 9).alias("first_time_buyer"),
        na_to_null(F.col("msa")).alias("msa"),
        na_to_null(F.col("mi_pct"), 999).cast("double").alias("mi_pct"),
        na_to_null(F.col("units"), 99).cast("int").alias("units"),
        na_to_null(F.col("occupancy"), 9).alias("occupancy"),
        na_to_null(F.col("cltv"), 999).cast("double").alias("cltv"),
        na_to_null(F.col("dti"), 999).cast("double").alias("dti"),
        F.col("orig_upb").cast("double").alias("orig_upb"),
        na_to_null(F.col("ltv"), 999).cast("double").alias("ltv"),
        F.col("orig_rate").cast("double").alias("orig_rate"),
        na_to_null(F.col("channel"), 9).alias("channel"),
        na_to_null(F.col("amort_type")).alias("amort_type"),
        na_to_null(F.col("state")).alias("state"),
        na_to_null(F.col("prop_type"), 99).alias("prop_type"),
        na_to_null(F.col("zip3")).alias("zip3"),
        na_to_null(F.col("purpose"), 9).alias("purpose"),
        F.col("orig_term").cast("int").alias("orig_term"),
        na_to_null(F.col("num_borrowers"), 99).cast("int").alias("num_borrowers"),
        na_to_null(F.col("seller")).alias("seller"),
        na_to_null(F.col("harp")).alias("harp"),
        na_to_null(F.col("program"), 9).alias("program"),
    ).withColumn("vintage", year).withColumn("orig_quarter", quarter)
     .select(*ORIG_KEEP, "vintage", "orig_quarter"))


def clean_perf(df):
    d = df.toDF(*PERF_COLS[: len(df.columns)])
    year, _ = vintage_from_loan_id(F.col("loan_id"))
    return (d.select(
        F.trim("loan_id").alias("loan_id"),
        yyyymm_to_date(F.col("period")).alias("period"),
        F.col("cur_upb").cast("double").alias("cur_upb"),
        F.trim("dlq_status").alias("dlq_status"),                       # keep raw: '00'.., 'RA'
        F.col("loan_age").cast("int").alias("loan_age"),
        F.col("rem_months").cast("int").alias("rem_months"),
        na_to_null(F.col("mod_flag")).alias("mod_flag"),
        na_to_null(F.col("zb_code")).alias("zb_code"),
        yyyymm_to_date(na_to_null(F.col("zb_date"))).alias("zb_date"),
        F.col("cur_rate").cast("double").alias("cur_rate"),
        na_to_null(F.col("payment_deferral")).alias("payment_deferral"),
        na_to_null(F.col("eltv"), 999).cast("double").alias("eltv"),
        na_to_null(F.col("disaster_flag")).alias("disaster_flag"),
        na_to_null(F.col("assistance_status")).alias("assistance_status"),
    ).withColumn("vintage", year)
     .select(*PERF_KEEP, "vintage"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="folder (local or gs://) with the unzipped .txt files")
    ap.add_argument("--output", required=True, help="parquet root (local or gs://)")
    ap.add_argument("--dataset", choices=["sample", "full"], required=True)
    ap.add_argument("--mode", default="overwrite", choices=["overwrite", "append"],
                    help="append = add new vintages/quarters (monthly or quarterly refresh)")
    a = ap.parse_args()

    spark = get_spark(f"freddie-ingest-{a.dataset}")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")   # only rewrite vintages present

    orig_paths = existing(spark, a.input, ORIG_PATTERNS)
    perf_paths = existing(spark, a.input, PERF_PATTERNS)
    print("origination files:", orig_paths, "\nperformance files:", perf_paths)

    orig = clean_orig(read_pipe(spark, orig_paths, len(ORIG_COLS))).withColumn("dataset", F.lit(a.dataset))
    perf = clean_perf(read_pipe(spark, perf_paths, len(PERF_COLS)))

    (orig.repartition("vintage").write.mode(a.mode).partitionBy("vintage")
         .parquet(a.output.rstrip("/") + "/orig"))
    (perf.repartition("vintage").sortWithinPartitions("loan_id", "period")
         .write.mode(a.mode).partitionBy("vintage").parquet(a.output.rstrip("/") + "/perf"))

    o = spark.read.parquet(a.output.rstrip("/") + "/orig")
    p = spark.read.parquet(a.output.rstrip("/") + "/perf")
    print(f"orig rows: {o.count():,} | perf rows: {p.count():,}")


if __name__ == "__main__":
    main()
