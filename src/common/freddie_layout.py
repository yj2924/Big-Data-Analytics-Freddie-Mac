"""Column layout of the Freddie Mac Single-Family Loan-Level Dataset (SFLLD).

Source: Freddie Mac "General User Guide" (July 2026 release) and the files themselves.
Both file types are pipe-delimited and have NO header row.

  * Origination file  (orig_YYYYQn.txt / sample_orig_YYYY.txt):        31 columns
  * Monthly performance (perf_YYYYQn.txt / sample_perf_YYYY.txt):      35 columns

Older releases had slightly different trailing columns (servicer name used to sit in the
origination file).  The key fields used by this project are all at stable leading positions,
so the readers below select columns BY POSITION and ignore any extra trailing fields.
"""

# ---- origination file (position -> name) -------------------------------------------------
ORIG_COLS = [
    "fico",                 # 0  Classic FICO, 300-850, 9999 = n/a
    "first_pay_date",       # 1  YYYYMM
    "first_time_buyer",     # 2  Y/N/9
    "maturity_date",        # 3  YYYYMM
    "msa",                  # 4  MSA / metro division code (null = non-metro / unknown)
    "mi_pct",               # 5  mortgage insurance %, 0 = none, 999 = n/a
    "units",                # 6  1-4, 99 = n/a
    "occupancy",            # 7  P/I/S, 9 = n/a
    "cltv",                 # 8  original combined LTV, 999 = n/a
    "dti",                  # 9  original DTI 0-65, 999 = n/a
    "orig_upb",             # 10 original unpaid principal balance ($)
    "ltv",                  # 11 original LTV, 999 = n/a
    "orig_rate",            # 12 original note rate (%)
    "channel",              # 13 R retail / B broker / C correspondent / T TPO n/a / 9
    "ppm_flag",             # 14 prepayment-penalty mortgage Y/N
    "amort_type",           # 15 FRM / ARM
    "state",                # 16 two-letter property state  <- join key to FRED / Fed district
    "prop_type",            # 17 SF/CO/PU/MH/CP, 99 = n/a
    "zip3",                 # 18 first three digits of the postal code
    "loan_id",              # 19 loan sequence number, e.g. F06Q10000059
    "purpose",              # 20 P purchase / C cash-out refi / N no-cash-out refi / R refi n/s
    "orig_term",            # 21 months
    "num_borrowers",        # 22 1, 2, ... 99 = n/a
    "seller",               # 23 seller name
    "super_conforming",     # 24
    "pre_harp_loan_id",     # 25
    "program",              # 26 special eligibility program
    "harp",                 # 27 HARP indicator
    "valuation_method",     # 28 property valuation method
    "interest_only",        # 29 I/O indicator
    "vantage4",             # 30 VantageScore 4.0 (9999 = n/a; mostly n/a before 2025)
]

# ---- monthly performance file --------------------------------------------------------------
PERF_COLS = [
    "loan_id",              # 0
    "period",               # 1  monthly reporting period YYYYMM
    "cur_upb",              # 2  current actual UPB
    "dlq_status",           # 3  months delinquent: 00, 01, 02, ... ; RA = REO acquisition
    "loan_age",             # 4  months since first payment
    "rem_months",           # 5  remaining months to legal maturity
    "defect_settle_date",   # 6
    "mod_flag",             # 7  Y / P / blank
    "zb_code",              # 8  zero-balance code (01 prepaid, 02 3rd-party sale, 03 short sale,
                            #    09 REO disposition, 15 note sale, 16 reperforming sale, 96 repurchase)
    "zb_date",              # 9  YYYYMM
    "cur_rate",             # 10 current interest rate
    "non_int_upb",          # 11 current non-interest-bearing UPB
    "ddlpi",                # 12 due date of last paid installment
    "mi_recoveries",        # 13
    "net_sale_proceeds",    # 14
    "non_mi_recoveries",    # 15
    "total_expenses",       # 16
    "legal_costs",          # 17
    "maint_costs",          # 18
    "taxes_insurance",      # 19
    "misc_expenses",        # 20
    "actual_loss",          # 21
    "cum_mod_cost",         # 22
    "step_mod_flag",        # 23
    "payment_deferral",     # 24
    "eltv",                 # 25 estimated LTV (999 = n/a)
    "zb_removal_upb",       # 26
    "dlq_accrued_interest", # 27
    "disaster_flag",        # 28 delinquency due to disaster
    "assistance_status",    # 29 borrower assistance plan (F forbearance, R repayment, T trial)
    "cur_period_mod_cost",  # 30
    "int_bearing_upb",      # 31
    "mi_cancel_flag",       # 32
    "servicer",             # 33
    "bankruptcy_cramdown",  # 34
]

# zero-balance codes that mean a credit loss event (used in the default definition)
DEFAULT_ZB_CODES = ["02", "03", "09", "15"]
# zero-balance codes that end the loan WITHOUT default (censoring)
PREPAY_ZB_CODES = ["01"]
OTHER_EXIT_ZB_CODES = ["16", "96"]

# columns kept downstream (keeps the parquet panel small)
ORIG_KEEP = ["loan_id", "fico", "first_pay_date", "first_time_buyer", "msa", "mi_pct", "units", "occupancy",
             "cltv", "dti", "orig_upb", "ltv", "orig_rate", "channel", "amort_type", "state", "prop_type",
             "zip3", "purpose", "orig_term", "num_borrowers", "seller", "harp", "program"]
PERF_KEEP = ["loan_id", "period", "cur_upb", "dlq_status", "loan_age", "rem_months", "mod_flag", "zb_code",
             "zb_date", "cur_rate", "payment_deferral", "eltv", "disaster_flag", "assistance_status"]
