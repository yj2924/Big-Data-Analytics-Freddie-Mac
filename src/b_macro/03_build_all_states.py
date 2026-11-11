
"""
Member B - Step 3
Build macro features for all 50 US states + DC.

Sources:
- FRED state unemployment rates
- FRED/FHFA quarterly state HPI
- FRED national 30-year mortgage rate

Output:
data/processed/macro_all_states.csv
data/processed/macro_all_states.parquet
"""

from pathlib import Path
from urllib.parse import urlencode
import pandas as pd

STATES = """
AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY
LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND
OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC
""".split()

START = "2000-01-01"
END = "2025-12-01"

OUTPUT = Path("data/processed")


def fetch_fred_batch(series_ids):
    """Download multiple FRED series in one request."""
    params = urlencode({"id": ",".join(series_ids)})
    url = (
        "https://fred.stlouisfed.org/graph/"
        f"fredgraph.csv?{params}"
    )

    df = pd.read_csv(url)
    df = df.rename(columns={df.columns[0]: "date"})
    df["date"] = pd.to_datetime(df["date"])

    missing = set(series_ids) - set(df.columns)
    if missing:
        raise ValueError(f"Missing FRED series: {missing}")

    for sid in series_ids:
        df[sid] = pd.to_numeric(
            df[sid], errors="coerce"
        )

    return df.set_index("date").sort_index()


def fetch_all(series_ids):
    """Download in small batches to reduce requests."""
    frames = []

    for i in range(0, len(series_ids), 8):
        batch = series_ids[i:i + 8]
        print("Downloading:", ", ".join(batch))
        frames.append(fetch_fred_batch(batch))

    return pd.concat(frames, axis=1)


def monthly_feature(series, name, lag_months=0):
    """Align a series to the month it can be used."""
    result = series.dropna().copy()
    result.index = (
        result.index + pd.DateOffset(months=lag_months)
    )
    result = result.resample("MS").ffill()
    result.name = name
    return result


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    # 1. State unemployment (monthly)
    unemployment_ids = [f"{s}UR" for s in STATES]
    unemployment = fetch_all(unemployment_ids)

    # 2. State HPI (quarterly)
    hpi_ids = [f"{s}STHPI" for s in STATES]
    hpi = fetch_all(hpi_ids)

    # 3. National mortgage rates (weekly)
    mortgage = fetch_fred_batch(
        ["MORTGAGE30US"]
    )["MORTGAGE30US"]

    # Monthly average, available in following month
    mortgage = mortgage.resample("MS").mean()
    mortgage = monthly_feature(
        mortgage, "macro_mortgage_rate",
        lag_months=1
    )

    months = pd.date_range(
        START, END, freq="MS"
    )

    results = []

    for state in STATES:
        u = monthly_feature(
            unemployment[f"{state}UR"],
            "macro_unemployment",
            lag_months=1
        )

        # Conservative six-month lag for
        # quarterly HPI; not exact release dates.
        h = monthly_feature(
            hpi[f"{state}STHPI"],
            "macro_hpi",
            lag_months=6
        )

        df = pd.DataFrame(index=months)
        df.index.name = "period"

        df = df.join(u)
        df = df.join(mortgage)
        df = df.join(h)

        df["state"] = state
        results.append(df.reset_index())

    result = pd.concat(
        results, ignore_index=True
    )

    result = result[[
        "state",
        "period",
        "macro_unemployment",
        "macro_mortgage_rate",
        "macro_hpi"
    ]]

    result = result.sort_values(
        ["state", "period"]
    ).reset_index(drop=True)

    # Verify one row per state-month
    assert not result.duplicated(
        ["state", "period"]
    ).any()

    # Keep only rows with all three indicators
    complete = result.dropna().copy()

    # Spark date column compatibility
    complete["period"] = complete["period"].dt.date

    csv_path = OUTPUT / "macro_all_states.csv"
    parquet_path = OUTPUT / "macro_all_states.parquet"

    complete.to_csv(csv_path, index=False)
    complete.to_parquet(parquet_path, index=False)

    print("\n=== MEMBER B DATA SUMMARY ===")
    print("States:", complete["state"].nunique())
    print("Rows:", len(complete))
    print("Date range:",
          complete["period"].min(),
          "to",
          complete["period"].max())
    print("Columns:", complete.columns.tolist())

    print("\nIncomplete rows:",
          len(result) - len(complete))

    print("\nSample:")
    print(complete.head())

    print("\nSaved:", csv_path)
    print("Saved:", parquet_path)


if __name__ == "__main__":
    main()
