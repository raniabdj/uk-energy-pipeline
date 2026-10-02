"""
Silver + Gold layers: clean and model the bronze JSON into tidy,
analysis-ready tables.

Silver = cleaned, typed, deduplicated, one row per fact (still granular).
Gold   = aggregated, business-facing tables — the ones Power BI connects to.

This mirrors a Databricks/Fabric medallion architecture, just running
locally with pandas instead of Spark. Swapping pandas for PySpark later
is a drop-in change to the same logic — the bronze/silver/gold contract
doesn't change.
"""
import glob
import json
import os

import pandas as pd

BASE_DIR = os.path.dirname(__file__)
BRONZE_DIR = os.path.join(BASE_DIR, "..", "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "..", "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "..", "data", "gold")


def _load_latest(prefix: str) -> dict:
    files = sorted(glob.glob(os.path.join(BRONZE_DIR, f"{prefix}_*.json")))
    if not files:
        raise FileNotFoundError(
            f"No bronze files found for '{prefix}'. Run extract.py first."
        )
    with open(files[-1]) as f:
        return json.load(f)


def build_silver_intensity() -> pd.DataFrame:
    raw = _load_latest("intensity")
    rows = raw["data"]
    df = pd.DataFrame(
        [
            {
                "period_start": r["from"],
                "period_end": r["to"],
                "forecast_gco2_per_kwh": r["intensity"]["forecast"],
                "actual_gco2_per_kwh": r["intensity"]["actual"],
                "index": r["intensity"]["index"],
            }
            for r in rows
        ]
    )
    df["period_start"] = pd.to_datetime(df["period_start"])
    df["period_end"] = pd.to_datetime(df["period_end"])
    return df


def build_silver_generation_mix() -> pd.DataFrame:
    raw = _load_latest("generation_mix")
    period = raw["data"]
    rows = [
        {
            "period_from": period["from"],
            "period_to": period["to"],
            "fuel": f["fuel"],
            "pct_of_mix": f["perc"],
        }
        for f in period["generationmix"]
    ]
    df = pd.DataFrame(rows)
    df["period_from"] = pd.to_datetime(df["period_from"])
    df["period_to"] = pd.to_datetime(df["period_to"])
    return df


def build_silver_regional() -> pd.DataFrame:
    raw = _load_latest("regional")
    period = raw["data"][0]
    rows = []
    for region in period["regions"]:
        for f in region["generationmix"]:
            rows.append(
                {
                    "region_id": region["regionid"],
                    "region_name": region["shortname"],
                    "dno_region": region.get("dnoregion"),
                    "intensity_forecast": region["intensity"]["forecast"],
                    "intensity_index": region["intensity"]["index"],
                    "fuel": f["fuel"],
                    "pct_of_mix": f["perc"],
                }
            )
    df = pd.DataFrame(rows)
    return df


def build_gold_fuel_mix_summary(silver_generation: pd.DataFrame) -> pd.DataFrame:
    """One row per fuel type, national mix right now — Power BI pie/bar chart ready."""
    return (
        silver_generation.groupby("fuel", as_index=False)["pct_of_mix"]
        .sum()
        .sort_values("pct_of_mix", ascending=False)
        .reset_index(drop=True)
    )


def build_gold_regional_cleanest(silver_regional: pd.DataFrame) -> pd.DataFrame:
    """Rank GB regions by carbon intensity — cleanest grid right now."""
    return (
        silver_regional[["region_name", "dno_region", "intensity_forecast", "intensity_index"]]
        .drop_duplicates()
        .sort_values("intensity_forecast")
        .reset_index(drop=True)
    )


def main():
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)

    silver_intensity = build_silver_intensity()
    silver_generation = build_silver_generation_mix()
    silver_regional = build_silver_regional()

    silver_intensity.to_parquet(os.path.join(SILVER_DIR, "intensity.parquet"), index=False)
    silver_generation.to_parquet(os.path.join(SILVER_DIR, "generation_mix.parquet"), index=False)
    silver_regional.to_parquet(os.path.join(SILVER_DIR, "regional.parquet"), index=False)

    gold_fuel_mix = build_gold_fuel_mix_summary(silver_generation)
    gold_regional = build_gold_regional_cleanest(silver_regional)

    # Gold lands as CSV too — Power BI can connect directly to a folder of CSVs
    gold_fuel_mix.to_csv(os.path.join(GOLD_DIR, "fuel_mix_summary.csv"), index=False)
    gold_regional.to_csv(os.path.join(GOLD_DIR, "regional_cleanliness_ranking.csv"), index=False)

    print("Transform complete — silver (parquet) and gold (csv) layers built.")
    print(f"  Cleanest region right now: {gold_regional.iloc[0]['region_name']}")
    print(f"  Dirtiest region right now: {gold_regional.iloc[-1]['region_name']}")


if __name__ == "__main__":
    main()
