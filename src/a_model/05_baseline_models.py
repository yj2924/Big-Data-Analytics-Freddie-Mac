"""Step 5 (Member A): baseline default models with an OUT-OF-TIME split.

Model 1  Loan-level logistic regression
         target  : default_24m  (90+ DPD / credit event within 24 months of first payment)
         sample  : loans whose 24-month outcome is known (observed_24m = 1)
         split   : train = vintages <= --train-last-vintage, test = later vintages

Model 2  Discrete-time hazard model (logistic regression on the loan x month panel)
         target  : event = 1 in the month the loan first defaults; 0 in every earlier at-risk month
         features: borrower/loan traits at origination + loan-age buckets (+ optional macro / Beige Book)
         split   : train = reporting months before --test-start, test = months from --test-start on
                   (true out-of-time test: the model never sees the test calendar period)
         Non-default months in TRAIN are down-sampled (rate --neg-rate) and re-weighted by 1/rate, so the
         fitted probabilities stay calibrated to the real monthly default rate.

Feature sets (for the "do Beige Book signals help?" comparison with Members B and C)
  M0  loan traits + loan age                       (always available)
  M1  M0 + every column starting with macro_       (FRED features from Member B)
  M2  M1 + every column starting with bb_          (Beige Book LLM scores from Member C)
  The extra table is passed with --extra-features; it must have columns state, period (first day of
  the month) and the feature columns, i.e. it is already aligned state x month (Member B's alignment job).

Outputs  results/models_<tag>/ : metrics.json, auc_ks_by_vintage.csv, calibration_by_month.csv,
         calibration.png, coefficients_<model>.csv
"""
import argparse
import json
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import matplotlib                                                         # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                           # noqa: E402
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
from pyspark.ml import Pipeline                                           # noqa: E402
from pyspark.ml.classification import LogisticRegression                  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.functions import vector_to_array                          # noqa: E402
from pyspark import StorageLevel                                          # noqa: E402
from pyspark.sql import functions as F, Window                            # noqa: E402
from sklearn.metrics import roc_auc_score, roc_curve                      # noqa: E402
from common.spark_utils import get_spark                                  # noqa: E402

CATS = ["occupancy", "purpose", "channel", "prop_type", "state", "first_time_buyer"]
AGE_BUCKETS = [(0, 6), (7, 12), (13, 24), (25, 36), (37, 60), (61, 120), (121, 10_000)]


def loan_features(orig):
    """Borrower / loan traits at origination (+ missing flags, simple imputation)."""
    w = Window.partitionBy("first_pay_date", (F.col("orig_term") > 180).cast("int"))
    d = (orig
         .withColumn("rate_spread", F.col("orig_rate") - F.avg("orig_rate").over(w))   # vs loans of the same month
         .withColumn("fico_missing", F.col("fico").isNull().cast("double"))
         .withColumn("dti_missing", F.col("dti").isNull().cast("double"))
         .withColumn("ltv_missing", F.col("ltv").isNull().cast("double"))
         .fillna({"fico": 740, "dti": 35.0, "ltv": 75.0, "mi_pct": 0.0, "units": 1, "num_borrowers": 1})
         .withColumn("cltv", F.coalesce("cltv", "ltv"))
         .withColumn("fico_c", (F.col("fico") - 740) / 50.0)          # centred / scaled for readable coefficients
         .withColumn("ltv_c", (F.col("ltv") - 75) / 10.0)
         .withColumn("cltv_gap", (F.col("cltv") - F.col("ltv")) / 10.0)
         .withColumn("dti_c", (F.col("dti") - 35) / 10.0)
         .withColumn("log_upb", F.log(F.col("orig_upb")))
         .withColumn("has_mi", (F.col("mi_pct") > 0).cast("double"))
         .withColumn("multi_unit", (F.col("units") > 1).cast("double"))
         .withColumn("single_borrower", (F.col("num_borrowers") == 1).cast("double"))
         .withColumn("short_term", (F.col("orig_term") <= 180).cast("double"))
         .withColumn("arm", (F.col("amort_type") == "ARM").cast("double"))
         .fillna({c: "NA" for c in CATS})
         .fillna({"rate_spread": 0.0}))
    nums = ["fico_c", "fico_missing", "ltv_c", "ltv_missing", "cltv_gap", "dti_c", "dti_missing", "rate_spread",
            "log_upb", "has_mi", "multi_unit", "single_borrower", "short_term", "arm"]
    return d, nums


def build_pipeline(nums, cats, weight=None, reg=1e-4):
    idx = [StringIndexer(inputCol=c, outputCol=c + "_i", handleInvalid="keep") for c in cats]
    ohe = OneHotEncoder(inputCols=[c + "_i" for c in cats], outputCols=[c + "_v" for c in cats],
                        handleInvalid="keep", dropLast=True)
    va = VectorAssembler(inputCols=nums + [c + "_v" for c in cats], outputCol="features", handleInvalid="keep")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=reg,
                            elasticNetParam=0.0, standardization=True)
    if weight:
        lr.setWeightCol(weight)
    return Pipeline(stages=idx + [ohe, va, lr])


def ks_stat(y, p):
    fpr, tpr, _ = roc_curve(y, p)
    return float(np.max(tpr - fpr))


def scores_by_vintage(pdf, label="label", score="p"):
    rows = []
    for v, g in pdf.groupby("vintage"):
        if g[label].nunique() == 2 and g[label].sum() >= 20:
            rows.append({"vintage": int(v), "n": len(g), "events": int(g[label].sum()),
                         "auc": roc_auc_score(g[label], g[score]), "ks": ks_stat(g[label], g[score])})
    return pd.DataFrame(rows)


def coef_table(model, nums, cats, train_df):
    """Coefficient / odds-ratio table, with names read from the assembled feature-vector metadata."""
    lr = model.stages[-1]
    meta = model.transform(train_df.limit(1)).schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    pairs = sorted((a["idx"], a["name"]) for group in meta.values() for a in group)
    names = [n.replace("_v_", "=") for _, n in pairs]
    coefs = lr.coefficients.toArray()
    names = (names + [f"x{i}" for i in range(len(names), len(coefs))])[: len(coefs)]
    t = pd.DataFrame({"feature": names, "coef": coefs})
    t["odds_ratio"] = np.exp(t.coef)
    return pd.concat([pd.DataFrame({"feature": ["(intercept)"], "coef": [lr.intercept],
                                    "odds_ratio": [np.exp(lr.intercept)]}), t], ignore_index=True)


def extra_cols(df, prefixes):
    return [c for c in df.columns if any(c.startswith(p) for p in prefixes)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--tag", default="sample")
    ap.add_argument("--out", default="results")
    ap.add_argument("--train-last-vintage", type=int, default=2014)
    ap.add_argument("--test-start", default="2016-01-01")
    ap.add_argument("--neg-rate", type=float, default=0.05)
    ap.add_argument("--extra-features", default="", help="parquet with state, period, macro_*, bb_* columns")
    ap.add_argument("--feature-sets", default="M0", help="comma list of M0,M1,M2")
    ap.add_argument("--skip-loan-level", action="store_true")
    a = ap.parse_args()
    out = os.path.join(a.out, f"models_{a.tag}")
    os.makedirs(out, exist_ok=True)
    spark = get_spark("freddie-models")

    orig = spark.read.parquet(a.parquet.rstrip("/") + "/orig")
    loans = spark.read.parquet(a.model.rstrip("/") + "/loans").drop("vintage")
    panel = spark.read.parquet(a.model.rstrip("/") + "/panel")
    lf, nums = loan_features(orig)
    lf = lf.join(loans.select("loan_id", "default_24m", "observed_24m"), "loan_id").persist(StorageLevel.DISK_ONLY)
    metrics = {"tag": a.tag, "train_last_vintage": a.train_last_vintage, "test_start": a.test_start}

    # =============== Model 1: loan-level logistic, 24-month default =================================
    if not a.skip_loan_level:
        d = lf.where("observed_24m = 1").withColumn("label", F.col("default_24m").cast("double"))
        tr = d.where(F.col("vintage") <= a.train_last_vintage)
        te = d.where(F.col("vintage") > a.train_last_vintage)
        m1 = build_pipeline(nums, CATS).fit(tr)
        pt = (m1.transform(te).select("vintage", "label", vector_to_array("probability")[1].alias("p")).toPandas())
        ptr = (m1.transform(tr.sample(fraction=min(1.0, 300_000 / max(1, tr.count())), seed=1))
                 .select("label", vector_to_array("probability")[1].alias("p")).toPandas())
        metrics["loan_level"] = {
            "train_loans": tr.count(), "test_loans": len(pt), "test_default_rate": float(pt.label.mean()),
            "train_auc": roc_auc_score(ptr.label, ptr.p), "test_auc": roc_auc_score(pt.label, pt.p),
            "test_ks": ks_stat(pt.label, pt.p),
            "test_mean_pred": float(pt.p.mean()),
        }
        scores_by_vintage(pt).assign(model="loan_level_24m").to_csv(os.path.join(out, "auc_ks_by_vintage_loan_level.csv"), index=False)
        coef_table(m1, nums, CATS, tr).to_csv(os.path.join(out, "coefficients_loan_level.csv"), index=False)
        print("loan-level:", json.dumps(metrics["loan_level"], indent=1))

    # =============== Model 2: discrete-time hazard on the loan x month panel ===========================
    age_cols = []
    pnl = panel.select("loan_id", "vintage", "period", "loan_age", "event")
    for lo, hi in AGE_BUCKETS[1:]:                                # first bucket (0-6 months) is the reference
        name = f"age_{lo}_{hi if hi < 10_000 else 'plus'}"
        pnl = pnl.withColumn(name, F.col("loan_age").between(lo, hi).cast("double"))
        age_cols.append(name)
    # materialise the hazard design table once (loan-month rows + loan traits) so that training and scoring
    # stream from Parquet instead of re-running the big join (keeps driver memory small on a laptop)
    design_path = a.model.rstrip("/") + "/design_hazard"
    keep = ["loan_id", "state"] + nums + CATS
    (pnl.join(lf.select(*dict.fromkeys(keep)), "loan_id")
        .write.mode("overwrite").parquet(design_path))
    pnl = spark.read.parquet(design_path)
    if a.extra_features:
        extra = spark.read.parquet(a.extra_features)
        pnl = pnl.join(extra, ["state", "period"], "left")
    pnl = pnl.withColumn("label", F.col("event").cast("double"))
    test_start = F.to_date(F.lit(a.test_start))

    train = (pnl.where(F.col("period") < test_start)
                .where((F.col("event") == 1) | (F.rand(seed=7) < a.neg_rate))
                .withColumn("w", F.when(F.col("event") == 1, 1.0).otherwise(1.0 / a.neg_rate))).persist(StorageLevel.DISK_ONLY)
    test = pnl.where(F.col("period") >= test_start)
    metrics["hazard_train_rows_after_downsampling"] = train.count()
    metrics["hazard_train_events"] = train.where("event = 1").count()

    sets = {"M0": []}
    if a.extra_features:
        sets["M1"] = extra_cols(pnl, ["macro_"])
        sets["M2"] = extra_cols(pnl, ["macro_", "bb_"])
    calib_all, byv_all = [], []
    for name in [s.strip() for s in a.feature_sets.split(",") if s.strip() in sets]:
        feats = nums + age_cols + sets[name]
        tr = train.fillna(0.0, subset=sets[name]) if sets[name] else train
        te = test.fillna(0.0, subset=sets[name]) if sets[name] else test
        mdl = build_pipeline(feats, CATS, weight="w").fit(tr)
        scored = mdl.transform(te).select("vintage", "period", "label", vector_to_array("probability")[1].alias("p"))
        # calibration on the FULL test panel: expected vs actual defaults per month
        cal = (scored.groupBy("period").agg(F.count("*").alias("at_risk"), F.sum("label").alias("actual"),
                                             F.sum("p").alias("expected")).orderBy("period").toPandas())
        cal["model"] = name
        calib_all.append(cal)
        # AUC / KS: all events + a uniform 5% of non-events (uniform sub-sampling of negatives leaves AUC/KS unbiased)
        smp = scored.where((F.col("label") == 1) | (F.rand(seed=11) < 0.05)).toPandas()
        byv = scores_by_vintage(smp).assign(model=f"hazard_{name}")
        byv_all.append(byv)
        tot_a, tot_e = float(cal.actual.sum()), float(cal.expected.sum())
        metrics[f"hazard_{name}"] = {
            "features": feats, "test_rows": int(cal.at_risk.sum()), "test_events": int(tot_a),
            "test_auc": roc_auc_score(smp.label, smp.p), "test_ks": ks_stat(smp.label, smp.p),
            "expected_over_actual_defaults": tot_e / tot_a if tot_a else math.nan,
        }
        coef_table(mdl, feats, CATS, tr).to_csv(os.path.join(out, f"coefficients_hazard_{name}.csv"), index=False)
        print(name, json.dumps({k: v for k, v in metrics[f"hazard_{name}"].items() if k != "features"}, indent=1))

    cal = pd.concat(calib_all)
    cal.to_csv(os.path.join(out, "calibration_by_month.csv"), index=False)
    pd.concat(byv_all).to_csv(os.path.join(out, "auc_ks_by_vintage_hazard.csv"), index=False)
    fig, ax = plt.subplots(figsize=(10, 3.8))
    c0 = cal[cal.model == cal.model.iloc[0]]
    ax.plot(pd.to_datetime(c0.period), 1e4 * c0.actual / c0.at_risk, color="#444", label="actual")
    for mname, g in cal.groupby("model"):
        ax.plot(pd.to_datetime(g.period), 1e4 * g.expected / g.at_risk, label=f"predicted ({mname})")
    ax.set_ylabel("defaults per 10,000 loans at risk")
    ax.set_title(f"Out-of-time calibration of the hazard model (test period from {a.test_start})")
    ax.legend(); ax.grid(alpha=.3); fig.tight_layout()
    fig.savefig(os.path.join(out, "calibration.png"), dpi=150); plt.close(fig)
    with open(os.path.join(out, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=1, default=float)


if __name__ == "__main__":
    main()
