
import pandas as pd
from pathlib import Path

# Member B: FRED Macroeconomic Data
STATES = ["NY", "NJ"]

def fetch_fred(series_id):
    url = (
        "https://fred.stlouisfed.org/graph/"
        f"fredgraph.csv?id={series_id}"
    )
    df = pd.read_csv(url)
    df.columns = ["period", "value"]
    df["period"] = pd.to_datetime(df["period"])
    df["value"] = pd.to_numeric(
        df["value"], errors="coerce"
    )
    return df.set_index("period")["value"]

def main():
    mortgage = fetch_fred("MORTGAGE30US")
    mortgage = mortgage.resample("MS").mean()
    mortgage.name = "macro_mortgage_rate"

    frames = []

    for state in STATES:
        unemployment = fetch_fred(f"{state}UR")
        unemployment = unemployment.resample("MS").mean()
        unemployment.name = "macro_unemployment"

        df = pd.concat(
            [unemployment, mortgage], axis=1
        )
        df["state"] = state
        frames.append(df.reset_index())

    macro = pd.concat(frames, ignore_index=True)

    macro = macro[
        (macro["period"] >= "2000-01-01") &
        (macro["period"] <= "2025-12-01")
    ]

    macro = macro.dropna().sort_values(
        ["state", "period"]
    )

    assert not macro.duplicated(
        ["state", "period"]
    ).any()

    output = Path("data/processed")
    output.mkdir(parents=True, exist_ok=True)

    macro.to_csv(
        output / "macro_test_NY_NJ.csv",
        index=False
    )

    print(macro.head(10))
    print("Total rows:", len(macro))
    print("FRED data downloaded successfully!")

if __name__ == "__main__":
    main()

