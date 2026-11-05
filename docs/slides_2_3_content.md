# Proposal slides 2 and 3 (Member A, Yuxuan Ji)

Both slides are in the team deck "6893 Final Presentation" (Google Slides). This file keeps the text and the
sources of every number, so teammates can check or reuse them.

## Slide 2: Goal + what is new + challenges

Title: Goal: predict mortgage default and test if Fed text adds signal

Goal
1. Monthly probability that a loan becomes 90+ days delinquent, from FICO, LTV, DTI, loan age and regional
   macro data.
2. Stress test: default rates under baseline vs. adverse unemployment and house-price paths.
3. Do LLM-scored Beige Book signals beat FRED data alone, out of time?

What is new
1. Fed district text turned into housing, labor and credit scores, joined to a loan × month panel.
2. Hazard model on 473M loan-months in Spark, tested out of time and by vintage.

Challenges
1. Rare events: about 4 new defaults per 10,000 loans per month (24 in 2009-10)
   (source: `results/eda_sample/monthly_default_rate.csv`; mean 2016-2026 = 4.3, mean 2009-2010 = 24.0).
2. COVID: 82 % of 2020's new 90+ DPD loans were in forbearance, so the label excludes relief months
   (sample performance file, 2020: 14,630 of the 17,755 loan-months that entered 90 DPD had assistance status F).
3. Regime shift: a loan-only model predicts 2.3× the defaults seen after 2016, so it needs macro + text
   (`results/models_sample/metrics.json`, hazard M0, expected / actual = 2.31).

Figure: cumulative default by vintage, 2006 vs 2012 vs 2020 (`docs/figures/vintage_curves_slide.png`).

## Slide 3: Data and the 3Vs

Title: Data: three sources joined into one loan × month panel (3Vs)

1. Freddie Mac loan-level data (Member A)
   * Volume: 49.5M loans, 1999-2026. Pulled: full 2006/2012/2020 = 7.1M loans, 473M loan-months
     (7.3 GB zip, 54.6 GB text, 4.3 GB Parquet); sample of 1.4M loans, 76M rows.
   * Velocity: quarterly release of monthly performance; new quarters appended as Parquet partitions.
   * Variety: structured, pipe-delimited: 31 loan fields + 35 monthly fields, coded missing values.
2. FRED (Member B, to be confirmed)
   * Volume: unemployment, house prices, 30-yr mortgage rate for 50 states + DC, 1999-2026 (~50K observations).
   * Velocity: monthly (house prices quarterly); free API.
   * Variety: numeric time series (JSON / CSV).
3. Fed Beige Book (Member C, to be confirmed)
   * Volume: 8 issues a year × 12 districts, 1999-2026 (~2,700 reports).
   * Velocity: 8 issues a year.
   * Variety: unstructured text, turned into LLM scores for housing, labor and credit.

Footnote: join keys are loan state → FRED state series and Fed district, and calendar month, using only
information released before that month.
