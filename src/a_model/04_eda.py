"""Step 4 (Member A): exploratory analysis of default vs borrower traits and by vintage.

Outputs (results/eda_<tag>/):
  default_by_fico.csv / _ltv.csv / _dti.csv   24-month default rate by band (loans observed >= 24 months)
  default_fico_x_ltv.csv                       two-way table FICO band x LTV band
  vintage_curves.csv + vintage_curves.png      cumulative default % by loan age for each vintage
  monthly_default_rate.csv + .png              defaults / loans at risk by calendar month
  default_by_traits.png                        the three one-way charts
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import matplotlib                                                         # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                           # noqa: E402
import pandas as pd                                                       # noqa: E402
from pyspark.sql import functions as F                                    # noqa: E402
from common.spark_utils import get_spark                                  # noqa: E402

FICO_BANDS = [(None, 620, "<620"), (620, 660, "620-659"), (660, 700, "660-699"), (700, 740, "700-739"),
              (740, 780, "740-779"), (780, None, "780+")]
LTV_BANDS = [(None, 60.0001, "<=60"), (60.0001, 70.0001, "60-70"), (70.0001, 80.0001, "70-80"),
             (80.0001, 90.0001, "80-90"), (90.0001, 95.0001, "90-95"), (95.0001, None, ">95")]
DTI_BANDS = [(None, 20.0001, "<=20"), (20.0001, 30.0001, "20-30"), (30.0001, 36.0001, "30-36"),
             (36.0001, 43.0001, "36-43"), (43.0001, 50.0001, "43-50"), (50.0001, None, ">50")]
HILITE = {2006: "#c5221f", 2007: "#e8710a", 2012: "#1a73e8", 2020: "#188038"}


def band(col, bands):
    expr = None
    for lo, hi, lab in bands:
        cond = F.lit(True)
        if lo is not None:
            cond = cond & (F.col(col) >= lo)
        if hi is not None:
            cond = cond & (F.col(col) < hi)
        expr = F.when(cond, lab) if expr is None else expr.when(cond, lab)
    return expr.otherwise("missing")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True, help="root with orig/ (from step 1)")
    ap.add_argument("--model", required=True, help="root with loans/ and panel/ (from step 2)")
    ap.add_argument("--tag", default="sample")
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    out = os.path.join(a.out, f"eda_{a.tag}")
    os.makedirs(out, exist_ok=True)
    spark = get_spark("freddie-eda")

    orig = spark.read.parquet(a.parquet.rstrip("/") + "/orig")
    loans = spark.read.parquet(a.model.rstrip("/") + "/loans").drop("vintage")
    panel = spark.read.parquet(a.model.rstrip("/") + "/panel")
    d = (orig.join(loans, "loan_id")
             .withColumn("fico_band", band("fico", FICO_BANDS))
             .withColumn("ltv_band", band("ltv", LTV_BANDS))
             .withColumn("dti_band", band("dti", DTI_BANDS))
             .withColumn("era", F.when(F.col("vintage").between(2005, 2008), "2005-08 vintages")
                                 .otherwise("other vintages")))
    obs = d.where("observed_24m = 1").cache()

    # ---- one-way default rates ---------------------------------------------------------------
    tabs = {}
    for var, bands in (("fico", FICO_BANDS), ("ltv", LTV_BANDS), ("dti", DTI_BANDS)):
        t = (obs.groupBy(f"{var}_band", "era").agg(F.count("*").alias("loans"),
                                                    F.avg("default_24m").alias("default_24m_rate"))
                .toPandas())
        allv = (obs.groupBy(f"{var}_band").agg(F.count("*").alias("loans"), F.avg("default_24m").alias("default_24m_rate"))
                .toPandas().assign(era="all vintages"))
        t = pd.concat([allv, t])
        order = [b[2] for b in bands] + ["missing"]
        t[f"{var}_band"] = pd.Categorical(t[f"{var}_band"], order, ordered=True)
        t = t.sort_values(["era", f"{var}_band"])
        t["default_24m_pct"] = (100 * t.default_24m_rate).round(3)
        t.drop(columns="default_24m_rate").to_csv(os.path.join(out, f"default_by_{var}.csv"), index=False)
        tabs[var] = t

    fig, ax = plt.subplots(1, 3, figsize=(15, 4.2))
    for k, (var, title) in enumerate((("fico", "FICO score"), ("ltv", "Original LTV (%)"), ("dti", "Original DTI (%)"))):
        t = tabs[var][tabs[var][f"{var}_band"] != "missing"]
        for era, col in (("2005-08 vintages", "#c5221f"), ("other vintages", "#1a73e8"), ("all vintages", "#444")):
            s = t[t.era == era]
            ax[k].plot(s[f"{var}_band"].astype(str), s.default_24m_pct, "o-", color=col, label=era,
                       lw=2.2 if era == "all vintages" else 1.4)
        ax[k].set_title(f"24-month default rate by {title}")
        ax[k].set_ylabel("% of loans in 90+ DPD within 24 months")
        ax[k].grid(alpha=.3)
    ax[0].legend()
    fig.tight_layout(); fig.savefig(os.path.join(out, "default_by_traits.png"), dpi=150); plt.close(fig)

    two = (obs.where("fico_band != 'missing' and ltv_band != 'missing'")
              .groupBy("fico_band", "ltv_band").agg(F.count("*").alias("loans"), F.avg("default_24m").alias("rate"))
              .toPandas())
    two["default_24m_pct"] = (100 * two.rate).round(2)
    two.pivot(index="fico_band", columns="ltv_band", values="default_24m_pct") \
       .reindex([b[2] for b in FICO_BANDS])[[b[2] for b in LTV_BANDS]] \
       .to_csv(os.path.join(out, "default_fico_x_ltv.csv"))

    # ---- vintage curves: cumulative share of the ORIGINAL cohort that has defaulted by loan age ----------
    size = d.groupBy("vintage").count().toPandas().set_index("vintage")["count"]
    max_age = (panel.groupBy("vintage").agg(F.max("loan_age").alias("max_age")).toPandas()
               .set_index("vintage")["max_age"])
    dd = (d.where("default_flag = 1").groupBy("vintage", "default_loan_age").count().toPandas()
          .pivot(index="default_loan_age", columns="vintage", values="count").fillna(0).sort_index())
    dd = dd.reindex(range(0, int(dd.index.max()) + 1), fill_value=0)
    curves = dd.cumsum() / size[dd.columns] * 100
    for v in curves.columns:                                     # do not draw beyond what we have observed
        curves.loc[curves.index > max_age.get(v, 0), v] = float("nan")
    curves.index.name = "loan_age_months"
    curves.round(4).to_csv(os.path.join(out, "vintage_curves.csv"))

    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    for v in curves.columns:
        if v not in HILITE:
            ax.plot(curves.index, curves[v], color="#c8c8c8", lw=.8)
    for v, col in HILITE.items():
        if v in curves.columns:
            ax.plot(curves.index, curves[v], color=col, lw=2.4, label=str(v))
    ax.set_xlim(0, 180); ax.set_xlabel("loan age (months since first payment)")
    ax.set_ylabel("cumulative % of vintage in default (90+ DPD)")
    ax.set_title("Cumulative default by vintage (grey = other vintages)")
    ax.legend(title="vintage"); ax.grid(alpha=.3)
    fig.tight_layout(); fig.savefig(os.path.join(out, "vintage_curves.png"), dpi=150); plt.close(fig)

    # ---- monthly default hazard by calendar month --------------------------------------------------
    m = (panel.groupBy("period").agg(F.count("*").alias("at_risk"), F.sum("event").alias("defaults"))
              .orderBy("period").toPandas())
    m["monthly_default_rate_bp"] = (1e4 * m.defaults / m.at_risk).round(3)
    m.to_csv(os.path.join(out, "monthly_default_rate.csv"), index=False)
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.plot(pd.to_datetime(m.period), m.monthly_default_rate_bp, color="#1a73e8")
    ax.set_ylabel("new defaults per 10,000 active loans"); ax.set_title("Monthly default rate by calendar month")
    ax.grid(alpha=.3); fig.tight_layout(); fig.savefig(os.path.join(out, "monthly_default_rate.png"), dpi=150)
    plt.close(fig)

    for var in ("fico", "ltv", "dti"):
        print(tabs[var][tabs[var].era == "all vintages"].to_string(index=False))
    print(curves[[v for v in HILITE if v in curves.columns]].iloc[[12, 24, 36, 60, 120]].round(2))


if __name__ == "__main__":
    main()
