"""
build_baselines_json.py
=======================
Reads SST_v3.csv (the output of pipeline Step 5) and produces
CAH_REH_Baselines.json — per-year percentile financial baselines for
Critical Access Hospitals (CAH) and Rural Emergency Hospital converters (REH).

Usage:
    python build_baselines_json.py                              # defaults
    python build_baselines_json.py <sst_path> <output_path>    # explicit paths
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── CONFIG ───────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
DATA_OUT    = BASE_DIR / "data" / "output"

SST_PATH    = Path(sys.argv[1]) if len(sys.argv) > 1 else DATA_OUT / "SST_v3.csv"
OUTPUT_PATH = Path(sys.argv[2]) if len(sys.argv) > 2 else DATA_OUT / "CAH_REH_Baselines.json"

# Numeric metrics to summarise; silently skipped if column is absent
METRICS = [
    "Operating_Margin_Pct",
    "Net_Profit_Margin_Pct",
    "Days_Cash_on_Hand",
    "Current_Ratio",
    "Debt_Ratio",
    "Medicare_Pct_Days",
    "Medicaid_Pct_Days",
    "Outpatient_Rev_Pct",
    "Labor_Cost_Pct",
    "Charity_Care_Pct_Revenue",
    "Rev_per_Discharge",
    "Cost_per_Discharge",
    "Total_Discharges",
    "Num_Beds",
    "CMS_Net_Patient_Revenue",
    "Is_340B_Enrolled",
]


# ── HELPERS ──────────────────────────────────────────────────────────────────

def compute_baselines(df: pd.DataFrame) -> dict:
    """Return per-year summary stats dict for the given hospital subset."""
    result = {}
    for year, grp in df.groupby("Year"):
        year_key = str(int(year))
        year_stats: dict = {"n_hospitals": int(grp["CCN"].nunique())}
        for metric in METRICS:
            if metric not in grp.columns:
                continue
            col = pd.to_numeric(grp[metric], errors="coerce").dropna()
            if len(col) == 0:
                continue
            year_stats[metric] = {
                "p25":     round(float(np.percentile(col, 25)), 4),
                "median":  round(float(np.percentile(col, 50)), 4),
                "p75":     round(float(np.percentile(col, 75)), 4),
                "mean":    round(float(col.mean()), 4),
                "n_valid": int(len(col)),
            }
        result[year_key] = year_stats
    return result


# ── MAIN ─────────────────────────────────────────────────────────────────────

print(f"Loading {SST_PATH}...")
df = pd.read_csv(SST_PATH, dtype=str, low_memory=False)
print(f"  Loaded {len(df):,} rows")

# Coerce year to int; drop rows with unparseable year
df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
df = df.dropna(subset=["Year"])
df["Year"] = df["Year"].astype(int)

# Coerce flag columns so comparisons work on a str-read CSV
for flag in ("Is_CAH", "Is_REH_Converter", "Is_340B_Enrolled"):
    if flag in df.columns:
        df[flag] = pd.to_numeric(df[flag], errors="coerce").fillna(0).astype(int)

cah_df = df[df["Is_CAH"] == 1].copy()
reh_df = df[df["Is_REH_Converter"] == 1].copy()

print(f"  CAH rows : {len(cah_df):,}  ({cah_df['CCN'].nunique():,} unique CCNs)")
print(f"  REH rows : {len(reh_df):,}  ({reh_df['CCN'].nunique():,} unique CCNs)")

output = {
    "generated": pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S"),
    "source":    str(SST_PATH),
    "CAH":       compute_baselines(cah_df),
    "REH":       compute_baselines(reh_df),
}

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with OUTPUT_PATH.open("w") as f:
    json.dump(output, f, indent=2)

print(f"Saved -> {OUTPUT_PATH}")
