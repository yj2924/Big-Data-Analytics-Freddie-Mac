"""Step 3 (Member A): data profile for the proposal's 3Vs table and for the cleaning log.

Reports, per dataset (sample / full): loans, loan-month rows, vintages, reporting-period range,
on-disk size of the Parquet store, missing-value rates of the key borrower fields, duplicate keys
and out-of-range values.  Writes results/profile_<dataset>.json and prints a readable summary.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from pyspark.sql import functions as F                                    # noqa: E402
from common.spark_utils import get_spark                                  # noqa: E402


def dir_bytes(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return int(p.getFileSystem(spark._jsc.hadoopConfiguration()).getContentSummary(p).getLength())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    spark = get_spark("freddie-profile")
    root = a.parquet.rstrip("/")
    o = spark.read.parquet(root + "/orig")
    p = spark.read.parquet(root + "/perf")

    n_loans = o.count()
    prof = {
        "dataset": a.dataset,
        "loans": n_loans,
        "loan_month_rows": p.count(),
        "vintages": sorted(r[0] for r in o.select("vintage").distinct().collect()),
        "states": o.select("state").distinct().count(),
        "parquet_bytes_orig": dir_bytes(spark, root + "/orig"),
        "parquet_bytes_perf": dir_bytes(spark, root + "/perf"),
        "duplicate_loan_ids": n_loans - o.select("loan_id").distinct().count(),
        "duplicate_loan_month_keys": p.count() - p.select("loan_id", "period").distinct().count(),
    }
    pr = p.agg(F.min("period"), F.max("period")).first()
    prof["period_min"], prof["period_max"] = str(pr[0]), str(pr[1])
    miss = o.agg(*[F.avg(F.col(c).isNull().cast("double")).alias(c) for c in
                   ["fico", "ltv", "cltv", "dti", "orig_rate", "state", "mi_pct", "occupancy", "purpose"]]).first()
    prof["missing_rate_orig"] = {k: round(v, 5) for k, v in miss.asDict().items()}
    rng = o.agg(F.min("fico"), F.max("fico"), F.min("ltv"), F.max("ltv"), F.min("dti"), F.max("dti"),
                F.min("orig_rate"), F.max("orig_rate")).first()
    prof["ranges_orig"] = dict(zip(["fico_min", "fico_max", "ltv_min", "ltv_max", "dti_min", "dti_max",
                                    "rate_min", "rate_max"], [float(x) if x is not None else None for x in rng]))
    prof["dlq_status_values"] = {r[0]: r[1] for r in p.groupBy("dlq_status").count()
                                 .orderBy(F.desc("count")).limit(12).collect()}
    prof["loans_by_vintage"] = {int(r[0]): r[1] for r in o.groupBy("vintage").count().orderBy("vintage").collect()}

    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, f"profile_{a.dataset}.json"), "w") as f:
        json.dump(prof, f, indent=1)
    print(json.dumps(prof, indent=1))


if __name__ == "__main__":
    main()
