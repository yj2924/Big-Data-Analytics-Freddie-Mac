"""Step 6 (Member A): presentation-ready figures built from the CSVs written by steps 4-5 (pandas only).

  python src/a_model/06_slide_figures.py --eda results/eda_full --eda-raw results/eda_sample_d90raw --out docs/figures
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

COL = {2006: "#c5221f", 2007: "#e8710a", 2012: "#1a73e8", 2020: "#188038"}
plt.rcParams.update({"font.size": 13, "axes.spines.top": False, "axes.spines.right": False})


def vintage_chart(eda, out, title_suffix):
    c = pd.read_csv(os.path.join(eda, "vintage_curves.csv"), index_col=0)
    c.columns = [int(x) for x in c.columns]
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    for v in c.columns:
        if v not in COL:
            ax.plot(c.index, c[v], color="#d0d0d0", lw=.8)
    for v, col in COL.items():
        if v in c.columns:
            s = c[v].dropna()
            s = s[s.index <= 156]
            ax.plot(s.index, s, color=col, lw=3)
            ax.text(s.index.max() + 3, s.iloc[-1], str(v), color=col, fontsize=14, fontweight="bold", va="center")
    ax.set_xlim(0, 180)
    ax.set_xlabel("months since first payment")
    ax.set_ylabel("% of vintage in default (90+ DPD)")
    ax.set_title(title_suffix or "Cumulative default by origination year", fontsize=14)
    ax.grid(alpha=.25)
    fig.tight_layout()
    fig.savefig(out, dpi=200, transparent=False)
    plt.close(fig)


def relief_chart(eda_main, eda_raw, out):
    m = pd.read_csv(os.path.join(eda_main, "monthly_default_rate.csv"), parse_dates=["period"])
    r = pd.read_csv(os.path.join(eda_raw, "monthly_default_rate.csv"), parse_dates=["period"])
    fig, ax = plt.subplots(figsize=(7.2, 3.4))
    ax.plot(r.period, r.monthly_default_rate_bp, color="#b0b0b0", lw=1.6, label="raw 90+ DPD")
    ax.plot(m.period, m.monthly_default_rate_bp, color="#1a73e8", lw=2, label="excluding forbearance / disaster relief")
    ax.set_ylabel("defaults per 10,000 loans")
    ax.set_title("Monthly default rate: COVID forbearance spike removed", fontsize=14)
    ax.legend(frameon=False, fontsize=11)
    ax.grid(alpha=.25)
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eda", required=True)
    ap.add_argument("--eda-sample", default="results/eda_sample")
    ap.add_argument("--eda-raw", default="results/eda_sample_d90raw")
    ap.add_argument("--out", default="docs/figures")
    ap.add_argument("--suffix", default="")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    vintage_chart(a.eda, os.path.join(a.out, "vintage_curves_slide.png"), a.suffix)
    relief_chart(a.eda_sample, a.eda_raw, os.path.join(a.out, "relief_adjustment_slide.png"))
    print("written to", a.out)


if __name__ == "__main__":
    main()
