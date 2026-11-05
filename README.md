# Mortgage Credit Risk & Stress Testing (+ Beige Book) — EECS E6893 Final Project, Option C

**Question.** How do borrower traits and macro conditions drive mortgage default, and do text signals from the
Fed's Beige Book improve prediction?

This folder holds **Member A's part (Yuxuan Ji, yj2924): Freddie Mac loan data, default label, EDA and the
baseline default models.** Member B (FRED + scenarios + GCS/Dataproc pipeline) and Member C (Beige Book + LLM
scores + dashboard) plug into the interfaces described at the end.

## Data (Freddie Mac Single-Family Loan-Level Dataset, July 2026 release)

Downloaded from Freddie Mac Clarity (free registration). Standard dataset: about 49.5 M loans originated
1999-01 to 2026-06, monthly performance through 2026-06. Raw files are not committed (licence + size).

1. **Sample** (50,000 loans per vintage, 1999-2026): 1,375,000 loans, 76,002,480 loan-month rows;
   1.27 GB zipped, 8.3 GB raw text, 0.50 GB as Parquet.
2. **Full vintages 2006 + 2012 + 2020**: 7,140,622 loans, 473,001,445 loan-month rows; 7.33 GB zipped,
   54.6 GB raw text, 4.3 GB as Parquet. By vintage: 2006 has 1,193,544 loans / 76,565,111 rows (1.11 GB zip,
   9.0 GB text); 2012 has 2,033,338 / 185,759,800 (2.84 GB, 21.2 GB); 2020 has 3,913,740 / 210,676,534
   (3.38 GB, 24.4 GB).

Per-quarter counts: `results/full_stats.csv`. Data profile (missing rates, ranges, duplicates):
`results/profile_sample.json`, `results/profile_full.json`.

File format: two pipe-delimited files per vintage/quarter, no header. Origination file = 31 fields per loan
(FICO, LTV, CLTV, DTI, rate, state, purpose, occupancy, ...). Performance file = 35 fields per loan-month
(UPB, delinquency status, loan age, modification, zero-balance code, forbearance status, losses, ...).
Layout: `src/common/freddie_layout.py`. Not-available codes (9999, 999, 99, 9) are converted to NULL.

## Default definition (target)

A loan defaults in the **first month it is 90+ days delinquent, or reaches REO / third-party sale / short
sale / note sale** (zero-balance codes 02, 03, 09, 15). Prepaid loans (code 01) and other exits are censored.

**Relief adjustment (main definition `d90_ex_relief`).** 90+ DPD months while the borrower is in
forbearance (assistance status F) or the delinquency is flagged disaster-related do not count. In the sample,
82 % of the loans that reached 90 DPD in 2020 were in COVID forbearance; counting them would turn a policy
program into "defaults" (monthly default rate 175 per 10,000 in June 2020 vs about 5 in normal months).
The raw definition is kept as a sensitivity check (`--definition d90`, results in `results/*_d90raw/`).

## Pipeline (PySpark; same code locally and on Dataproc)

1. `src/a_data/01_ingest_to_parquet.py`: text (local or `gs://`) to typed Parquet partitioned by vintage;
   `--mode append` adds new quarters (incremental refresh).
2. `src/a_data/02_build_panel.py`: default label, per-loan table (`loans/`) and loan × month at-risk panel
   (`panel/`); `--vintages` processes one vintage at a time on small machines.
3. `src/a_data/03_data_profile.py`: rows, sizes, missing rates, ranges, duplicate keys (for the 3Vs slide).
4. `src/a_model/04_eda.py`: default rate by FICO / LTV / DTI band, vintage curves, monthly default rate.
5. `src/a_model/05_baseline_models.py`: loan-level logistic and discrete-time hazard models, out-of-time
   test, AUC / KS by vintage, calibration, coefficient tables.
6. `src/a_model/06_slide_figures.py`: presentation figures from the CSV outputs.

```bash
pip install -r requirements.txt
bash scripts/run_local.sh sample          # local Spark (expects ../data/raw_txt/sample/*.txt)
bash scripts/ingest_full_quarter.sh ...   # full vintages, one quarter at a time (keeps disk use low)
CLUSTER=... REGION=... BUCKET=... bash scripts/submit_dataproc.sh sample   # Dataproc
```

## First results (sample data, all vintages)

**Borrower traits (24-month default rate, loans observed ≥ 24 months).**
FICO < 620: 9.02 % vs 780+: 0.26 %. LTV > 95: 3.82 % vs ≤ 60: 0.63 %. DTI > 50: 4.50 % vs ≤ 20: 0.56 %.
For the 2005-08 vintages the gaps are 2-4 times wider in percentage points (e.g. LTV > 95: 12.4 % vs ≤ 60:
1.5 %), so trait effects depend on the macro regime (`results/eda_sample/default_by_traits.png`).

**Vintages.** Cumulative default after 60 months (sample): 2006 10.9 %, 2007 13.8 %, 2012 1.4 %, 2020 1.0 %
(`results/eda_sample/vintage_curves.png`). The full 2006 / 2012 / 2020 vintages give the same picture on every
loan: 10.8 %, 1.34 % and 0.90 % after 60 months; 14.2 % of all 1.19 M loans originated in 2006 eventually
defaulted (`results/eda_full/`, `docs/figures/vintage_curves_slide.png`). The full-vintage panel has
461,875,758 at-risk loan-months and 254,722 defaults.

**Baseline models (out-of-time).**

1. **Loan-level logistic regression, default within 24 months.** Trained on vintages 1999-2014 (800,000
   loans), tested on vintages 2015-2025 (481,880 loans, default rate 0.92 %): test AUC 0.790, KS 0.443;
   the model predicts 1.77 times the defaults that occurred.
2. **Discrete-time hazard model (loan × month logistic regression).** Trained on reporting months 1999-2015
   (all 39,097 default months plus a 5 % sample of non-default months, re-weighted), tested on every
   loan-month from 2016-01 to 2026-06 (37.5 M rows, 16,060 defaults): test AUC 0.752, KS 0.378; predicted
   defaults are 2.31 times the actual number.

Origination traits rank borrowers well, but a model without macro variables cannot follow the *level* of risk
across regimes: trained on a period that includes 2007-2011, it predicts about 2.3× the defaults actually seen
after 2016, and its AUC on seasoned 1999-2008 loans falls to 0.55-0.67. This is the gap the FRED (M1) and
Beige Book (M2) features are meant to close.

## Interfaces for Members B and C

`05_baseline_models.py --extra-features <parquet> --feature-sets M0,M1,M2` joins one table keyed by
`state` (2-letter) and `period` (first day of the month) and compares:

* **M0** loan traits + loan age (this folder)
* **M1** M0 + all columns starting with `macro_` (FRED: state unemployment, state HPI change, mortgage rate, ...)
* **M2** M1 + all columns starting with `bb_` (Beige Book LLM scores for the state's Fed district)

Use only information available at `period` (e.g. the Beige Book issue released before that month) to avoid
look-ahead bias. Same out-of-time split and metrics for all three, so the M2 − M1 difference answers the
project question.

## Repository layout

```
src/common/    file layout, Spark helpers
src/a_data/    01 ingest, 02 panel + label, 03 profile
src/a_model/   04 EDA, 05 baseline models
scripts/       run_local.sh, ingest_full_quarter.sh, submit_dataproc.sh
results/       CSV / PNG / JSON outputs (small files only)
docs/          data card, slides 2-3 content
```

Note: `results/models_sample/coefficients_hazard_M0.csv` lists the numeric and loan-age coefficients only;
re-running step 5 regenerates the full table including the state / purpose / occupancy dummies.
