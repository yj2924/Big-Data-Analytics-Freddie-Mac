Sensitivity run with the RAW default definition (`--definition d90`: every 90+ DPD month counts, including
COVID-forbearance and disaster-relief months).  Sample data, same splits as the main run.
Loan-level logistic (24-month default): train vintages <= 2014 (800,000 loans), test 2015+ (481,957 loans,
default rate 1.85 %): train AUC 0.851, test AUC 0.755, test KS 0.380.
Hazard model (test months 2016-01 .. 2026-06, 36.9 M loan-months, 28,433 events): AUC 0.724, KS 0.332,
expected / actual defaults 1.27.  See metrics.json and the CSVs in this folder.
(Coefficient tables of this run were removed: their category labels were misaligned by an earlier version of the code; the main run in models_sample/ has the corrected tables.)
