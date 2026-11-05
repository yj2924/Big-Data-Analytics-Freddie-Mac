"""Step 2 (Member A): default label + loan x month panel for the discrete-time hazard model.

Default definition (target), main version "d90_ex_relief"
  A loan DEFAULTS in the first reporting month in which ANY of these holds:
    * current delinquency status >= 3  (90+ days past due) while the loan is NOT in forbearance
      (borrower-assistance status F) and the delinquency is NOT flagged as disaster-related, or
    * delinquency status 'RA'           (REO acquisition), or
    * zero-balance code in {02 third-party sale, 03 short sale / charge-off, 09 REO disposition, 15 note sale}
  This is the usual "D90+ or credit-event termination" definition, adjusted for payment relief: during COVID
  (2020-21) and after hurricanes, servicers report loans in forbearance as 90+ DPD although payments were
  formally suspended; counting them would turn a policy program into "defaults".  --definition d90 keeps the
  raw version for a sensitivity check.
  Loans that pay off (zero-balance code 01) or leave for other reasons (16, 96) are CENSORED, i.e. they
  stop contributing at-risk months without an event.

Outputs
  <out>/panel/vintage=YYYY   one row per loan-month while the loan is at risk, up to and including the
                             default month; columns: loan_id, period, loan_age, event (0/1), cur_upb, ...
  <out>/loans/vintage=YYYY   one row per loan: outcome, default month/age, months observed, default_24m
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from pyspark.sql import functions as F                                    # noqa: E402
from common.freddie_layout import DEFAULT_ZB_CODES, PREPAY_ZB_CODES       # noqa: E402
from common.spark_utils import get_spark                                  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True, help="root written by 01_ingest_to_parquet.py")
    ap.add_argument("--output", required=True)
    ap.add_argument("--dpd", type=int, default=3, help="months delinquent that counts as default (3 = 90+ DPD)")
    ap.add_argument("--horizon", type=int, default=24, help="months for the loan-level default_<h>m label")
    ap.add_argument("--vintages", default="", help="optional comma list, e.g. 2006,2012,2020")
    ap.add_argument("--definition", default="d90_ex_relief", choices=["d90_ex_relief", "d90"],
                    help="d90_ex_relief (main): 90+ DPD months during forbearance / disaster relief do not count; "
                         "d90: any 90+ DPD month counts (sensitivity check)")
    a = ap.parse_args()

    spark = get_spark("freddie-panel")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    perf = spark.read.parquet(a.parquet.rstrip("/") + "/perf")
    if a.vintages:
        perf = perf.where(F.col("vintage").isin([int(v) for v in a.vintages.split(",")]))

    dlq_num = F.when(F.col("dlq_status") == "RA", F.lit(99)).otherwise(F.col("dlq_status").cast("int"))
    # payment-relief months: borrower in forbearance (assistance status F) or delinquency caused by a declared
    # disaster (flag Y).  In 2020 most new 90+ DPD loans were in COVID forbearance, not in economic default.
    relief = (F.coalesce(F.col("assistance_status"), F.lit("")) == "F") | \
             (F.coalesce(F.col("disaster_flag"), F.lit("")) == "Y")
    dlq_default = F.col("dlq_num") >= a.dpd
    if a.definition == "d90_ex_relief":
        dlq_default = dlq_default & ~relief
    perf = (perf.withColumn("dlq_num", dlq_num)
                .withColumn("relief", relief.cast("int"))
                .withColumn("is_default_row", dlq_default | F.col("zb_code").isin(DEFAULT_ZB_CODES)))

    # ---- per-loan summary ---------------------------------------------------------------------
    loans = (perf.groupBy("loan_id", "vintage").agg(
        F.min(F.when(F.col("is_default_row"), F.col("period"))).alias("default_period"),
        F.min("period").alias("first_period"),
        F.max("period").alias("last_period"),
        F.max(F.when(F.col("zb_code").isNotNull(), F.col("zb_code"))).alias("zb_code"),
        F.max(F.col("mod_flag").isin("Y", "P").cast("int")).alias("ever_modified"),
        F.max((F.col("assistance_status") == "F").cast("int")).alias("ever_forbearance"),
        F.max("relief").alias("ever_relief"),
        F.max(F.when(F.col("dlq_num") < 99, F.col("dlq_num"))).alias("max_dlq_months"),
        F.min(F.when(F.col("is_default_row"), F.col("loan_age"))).alias("default_loan_age"),
        F.max("loan_age").alias("max_loan_age"),
        F.count("*").alias("n_months"),
    ))
    loans = (loans
             .withColumn("default_flag", F.col("default_period").isNotNull().cast("int"))
             .withColumn("default_age", F.months_between("default_period", "first_period").cast("int"))
             .withColumn("months_observed", F.months_between("last_period", "first_period").cast("int") + 1)
             .withColumn("outcome",
                         F.when(F.col("default_flag") == 1, "default")
                          .when(F.col("zb_code").isin(PREPAY_ZB_CODES), "prepaid")
                          .when(F.col("zb_code").isNotNull(), "other_exit")
                          .otherwise("active"))
             # loan-level label: default within <horizon> months of the first payment (loan_age <= horizon).
             # observed_<h>m = we can tell (either it defaulted inside the window or we saw >= h months, or
             # it prepaid inside the window -> a known non-default for this horizon)
             .withColumn(f"default_{a.horizon}m",
                         ((F.col("default_flag") == 1) & (F.col("default_loan_age") <= a.horizon)).cast("int"))
             .withColumn(f"observed_{a.horizon}m",
                         ((F.col(f"default_{a.horizon}m") == 1) | (F.col("max_loan_age") >= a.horizon)
                          | (F.col("outcome").isin("prepaid", "other_exit"))).cast("int")))

    # ---- loan x month panel (at-risk months only) ------------------------------------------------
    # broadcast the small per-loan table so the 100M+-row performance table is not shuffled a second time
    panel = (perf.join(F.broadcast(loans.select("loan_id", "default_period")), "loan_id")
                 .where(F.col("default_period").isNull() | (F.col("period") <= F.col("default_period")))
                 .withColumn("event", (F.col("period") == F.col("default_period")).cast("int"))
                 .withColumn("event", F.coalesce("event", F.lit(0)))
                 .select("loan_id", "vintage", "period", "loan_age", "event", "dlq_num", "cur_upb",
                         "cur_rate", "eltv", "mod_flag", "relief"))

    out = a.output.rstrip("/")
    loans = loans.persist()
    loans.write.mode("overwrite").partitionBy("vintage").parquet(out + "/loans")
    panel.write.mode("overwrite").partitionBy("vintage").parquet(out + "/panel")

    L = spark.read.parquet(out + "/loans")
    P = spark.read.parquet(out + "/panel")
    print(f"loans: {L.count():,} | panel rows: {P.count():,} | events: {P.where('event = 1').count():,}")
    L.groupBy("outcome").count().orderBy("outcome").show()


if __name__ == "__main__":
    main()
