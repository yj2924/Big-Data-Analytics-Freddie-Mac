
"""
Member B - Step 2
Build state-month macroeconomic feature table.
NY/NJ prototype for Member A's M1 model.
"""

from pathlib import Path
import pandas as pd

INPUT = Path("data/processed/macro_test_NY_NJ.csv")
OUTPUT = Path("data/processed")


def fetch_hpi(state):
    series_id = f"{state}STHPI"
    url = (
        "https://fred.stlouisfed.org/graph/"
        f"fredgraph.csv?id={series_id}"
    )

    df = pd.read_csv(url)
    df.columns = ["observation_date", "macro_hpi"]

    df["observation_date"] = pd.to_datetime(
        df["observation_date"]
    )

    df["macro_hpi"] = pd.to_numeric(
        df["macro_hpi"], errors="coerce"
    )

    # Quarterly HPI is not available at quarter start.
    # A conservative 6-month lag for this prototype.
    df["period"] = (
        df["observation_date"]
        + pd.DateOffset(months=6)
    )

    df = df.set_index("period")[["macro_hpi"]]

    # Carry latest lagged observation into each month.
    df = df.resample("MS").ffill()

    df["state"] = state
    return df.reset_index()


def main():
    macro = pd.read_csv(INPUT)
    macro["period"] = pd.to_datetime(macro["period"])

    frames = []

    for state in ["NY", "NJ"]:
        hpi = fetch_hpi(state)
        frames.append(hpi)

    hpi_all = pd.concat(frames, ignore_index=True)

    result = macro.merge(
        hpi_all,
        on=["state", "period"],
        how="left",
        validate="one_to_one"
    )

    result = result.dropna().sort_values(
        ["state", "period"]
    )

    assert not result.duplicated(
        ["state", "period"]
    ).any()

    OUTPUT.mkdir(parents=True, exist_ok=True)

    result.to_csv(
        OUTPUT / "macro_features_NY_NJ.csv",
        index=False
    )

    result.to_parquet(
        OUTPUT / "macro_features_NY_NJ.parquet",
        index=False
    )

    print(result.head(10))
    print("Total rows:", len(result))
    print("States:", result["state"].unique())
    print("Columns:", result.columns.tolist())
    print("Macro feature table created successfully!")


if __name__ == "__main__":
    main()
