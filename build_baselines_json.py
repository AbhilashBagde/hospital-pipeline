"""
build_baselines_json.py
=======================
Reads SST_v6.csv (5-state region: AR, LA, NM, OK, TX) and produces
CAH_REH_Baselines.json — per-hospital financial baselines used by the
CAH/REH Calculator.

Output structure:
  {
    "metadata": { ... },
    "baseline_average":     [ per-hospital averages across all CAH years ],
    "baseline_most_recent": [ most-recent CAH year per hospital ]
  }

Usage:
    python build_baselines_json.py                              # defaults
    python build_baselines_json.py <sst_path> <output_path>    # explicit paths
"""

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
DATA_OUT    = BASE_DIR / "data" / "output"

SST_PATH    = Path(sys.argv[1]) if len(sys.argv) > 1 else DATA_OUT / "SST_v6.csv"
OUTPUT_PATH = Path(sys.argv[2]) if len(sys.argv) > 2 else DATA_OUT / "CAH_REH_Baselines.json"

CAH_CODE_MIN, CAH_CODE_MAX = 1300, 1399

AFP_MONTHLY = {2023: 272_866, 2024: 276_234, 2025: 285_625.90, 2026: 295_051.54}
AFP_ANNUAL  = {yr: mo * 12 for yr, mo in AFP_MONTHLY.items()}

COERCE_COLS = [
    'Total_Hospital_Expenses', 'NASHP_Net_Patient_Revenue', 'NASHP_Net_Income',
    'NASHP_Operating_Expenses', 'NASHP_Hospital_Op_Costs',
    'CMS_Inpatient_Revenue', 'CMS_Outpatient_Revenue', 'CMS_Net_Patient_Revenue',
    'CMS_Total_Costs', 'CMS_Net_Income', 'CMS_Total_Salaries',
    'CMS_Total_Assets', 'CMS_Total_Liabilities', 'CMS_Current_Assets',
    'CMS_Current_Liabilities', 'CMS_Cash',
    'Num_Beds', 'FTE_Employees', 'Total_Discharges', 'Total_Patient_Days',
    'Inpatient_Occupancy', 'Operating_Margin_Pct', 'Net_Profit_Margin_Pct',
    'Days_Cash_on_Hand', 'Current_Ratio', 'Debt_Ratio',
    'Medicare_Payer_Mix_Pct', 'Medicare_Pct_Days',
    'Medicaid_Payer_Mix_Pct', 'Commercial_Payer_Mix_Pct',
    'COVID_PHE_Funding', 'Direct_Labor_Cost', 'Contract_Labor_Cost',
    'Is_340B_Enrolled', 'Fund_Balance',
    'nearest_building_road_dist_miles',
    # SDOH
    'Any disability among adults',
    'Cancer (non-skin) or melanoma among adults',
    'Current lack of health insurance among adults aged 18-64 years',
    'Fair or poor self-rated health status among adults',
    'Food insecurity in the past 12 months among adults',
    'Obesity among adults',
    'Cancer_PCTL', 'Obesity_PCTL', 'Food_Insecurity_PCTL',
    'Uninsured_PCTL', 'Disability_PCTL', 'Poor_Health_PCTL',
]

BASELINE_COLS = [
    'NASHP_Net_Patient_Revenue', 'Total_Hospital_Expenses', 'NASHP_Net_Income',
    'OP_Share', 'Medicare_Payer_Mix_Pct', 'Is_340B_Enrolled',
    'Days_Cash_on_Hand', 'Current_Ratio', 'Debt_Ratio', 'Operating_Margin_Pct',
    'Num_Beds', 'FTE_Employees', 'Total_Discharges', 'Total_Patient_Days',
    'Inpatient_Occupancy', 'NonCOVID_Other_Income',
    'Direct_Labor_Cost', 'Contract_Labor_Cost', 'Fund_Balance',
]

SDOH_COLS = [
    'Any disability among adults',
    'Cancer (non-skin) or melanoma among adults',
    'Current lack of health insurance among adults aged 18-64 years',
    'Fair or poor self-rated health status among adults',
    'Food insecurity in the past 12 months among adults',
    'Obesity among adults',
    'Cancer_PCTL', 'Obesity_PCTL', 'Food_Insecurity_PCTL',
    'Uninsured_PCTL', 'Disability_PCTL', 'Poor_Health_PCTL',
]

SDOH_RENAMES = {
    'Any disability among adults':                                   'SDOH_Disability_Pct',
    'Cancer (non-skin) or melanoma among adults':                    'SDOH_Cancer_Pct',
    'Current lack of health insurance among adults aged 18-64 years':'SDOH_Uninsured_Pct',
    'Fair or poor self-rated health status among adults':            'SDOH_PoorHealth_Pct',
    'Food insecurity in the past 12 months among adults':            'SDOH_FoodInsecurity_Pct',
    'Obesity among adults':                                          'SDOH_Obesity_Pct',
    'Cancer_PCTL':          'SDOH_Cancer_PCTL',
    'Obesity_PCTL':         'SDOH_Obesity_PCTL',
    'Food_Insecurity_PCTL': 'SDOH_FoodInsecurity_PCTL',
    'Uninsured_PCTL':       'SDOH_Uninsured_PCTL',
    'Disability_PCTL':      'SDOH_Disability_PCTL',
    'Poor_Health_PCTL':     'SDOH_PoorHealth_PCTL',
}


# ── Solvency ──────────────────────────────────────────────────────────────────

def classify_solvency(row):
    score = 0
    flags = []
    dcoh = row.get('Days_Cash_on_Hand', np.nan)
    cr   = row.get('Current_Ratio',     np.nan)
    dr   = row.get('Debt_Ratio',        np.nan)
    om   = row.get('Operating_Margin_Pct', np.nan)

    if pd.notna(dcoh):
        if dcoh < 30:   score += 2; flags.append('DCOH<30')
        elif dcoh < 90: score += 1; flags.append('DCOH<90')
    if pd.notna(cr):
        if cr < 1.0:    score += 2; flags.append('CR<1.0')
        elif cr < 2.0:  score += 1; flags.append('CR<2.0')
    if pd.notna(dr):
        if dr > 0.7:    score += 2; flags.append('DR>0.7')
        elif dr > 0.5:  score += 1; flags.append('DR>0.5')
    if pd.notna(om):
        if om < -0.10:  score += 2; flags.append('OM<-10%')
        elif om < 0:    score += 1; flags.append('OM<0%')

    if score >= 4:   status = 'Distressed'
    elif score >= 2: status = 'Marginal'
    else:            status = 'Stable'

    return score, status, ', '.join(flags) if flags else 'None'


# ── JSON cleanup ──────────────────────────────────────────────────────────────

def clean_for_json(df):
    records = []
    for _, row in df.iterrows():
        rec = {}
        for col, val in row.items():
            if isinstance(val, float) and np.isnan(val):
                rec[col] = None
            elif pd.isna(val) if not isinstance(val, (bool, str, list)) else False:
                rec[col] = None
            elif isinstance(val, np.integer):
                rec[col] = int(val)
            elif isinstance(val, np.floating):
                rec[col] = None if np.isnan(val) else float(round(val, 6))
            elif isinstance(val, np.bool_):
                rec[col] = bool(val)
            elif isinstance(val, pd.Timestamp):
                rec[col] = val.isoformat()
            else:
                rec[col] = val
        records.append(rec)
    return records


# ── Main ──────────────────────────────────────────────────────────────────────

print(f"Loading {SST_PATH} ...")
df = pd.read_csv(SST_PATH, low_memory=False, encoding='utf-8')
df['CCN'] = df['CCN'].astype(str).str.strip().str.zfill(6)
print(f"  {len(df):,} rows, {df['CCN'].nunique()} unique hospitals")

for col in COERCE_COLS:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# ── Hospital type flags ────────────────────────────────────────────────────────

df['Pre_REH_CCN'] = df['Pre_REH_CCN'].apply(
    lambda x: str(x).split('.')[0].zfill(6)
    if pd.notna(x) and str(x).lower() not in ('nan', '', '.') else np.nan
)
df['Unified_CCN'] = df['Pre_REH_CCN'].fillna(df['CCN'])
facility_code = pd.to_numeric(df['Unified_CCN'].str[2:], errors='coerce')
df['Is_True_CAH']     = facility_code.between(CAH_CODE_MIN, CAH_CODE_MAX)
df['Is_PPS_Converter'] = df['Pre_REH_CCN'].notna()

df_scope = df[df['Is_True_CAH'] | df['Is_PPS_Converter']].copy()
print(f"  {len(df_scope):,} rows for CAHs/converters")

# ── Date parsing ──────────────────────────────────────────────────────────────

df_scope['FY_End_dt'] = pd.to_datetime(
    df_scope['FY_End_NASHP'].fillna(df_scope['FY_End_CMS']), errors='coerce'
)
df_scope['FY_Begin_dt'] = pd.to_datetime(
    df_scope['FY_Begin_NASHP'].fillna(df_scope['FY_Begin_CMS']), errors='coerce'
)
df_scope['REH_Conv_dt'] = pd.to_datetime(df_scope['REH_Conversion_Date'], errors='coerce')

never_conv = df_scope['REH_Conv_dt'].isna()
df_scope.loc[never_conv, 'Row_Type'] = 'CAH_Year'

converted = ~never_conv
df_scope.loc[converted & (df_scope['FY_End_dt'] <= df_scope['REH_Conv_dt']), 'Row_Type'] = 'CAH_Year'
df_scope.loc[converted & (df_scope['FY_Begin_dt'] >= df_scope['REH_Conv_dt']), 'Row_Type'] = 'REH_Year'
df_scope.loc[converted & df_scope['Row_Type'].isna(), 'Row_Type'] = 'Transition'

# ── OP Share ──────────────────────────────────────────────────────────────────

total_gross = df_scope['CMS_Outpatient_Revenue'].fillna(0) + df_scope['CMS_Inpatient_Revenue'].fillna(0)
has_cms = (
    df_scope['CMS_Outpatient_Revenue'].notna() &
    df_scope['CMS_Inpatient_Revenue'].notna() &
    (total_gross > 0)
)
df_scope['OP_Share'] = np.where(
    has_cms,
    df_scope['CMS_Outpatient_Revenue'] / total_gross,
    df_scope['Outpatient_Rev_Pct'].fillna(np.nan) / 100
)
df_scope['OP_Share'] = df_scope['OP_Share'].clip(0, 1)

# ── Non-COVID other income ────────────────────────────────────────────────────

df_scope['Implied_Other_Income'] = (
    df_scope['NASHP_Net_Income'] -
    (df_scope['NASHP_Net_Patient_Revenue'] - df_scope['Total_Hospital_Expenses'])
)
df_scope['NonCOVID_Other_Income'] = (
    df_scope['Implied_Other_Income'] - df_scope['COVID_PHE_Funding'].fillna(0)
)

# ── CAH year rows only ────────────────────────────────────────────────────────

df_cah = df_scope[df_scope['Row_Type'] == 'CAH_Year'].copy()
print(f"  {len(df_cah):,} CAH-year rows")

# ── Average baseline ──────────────────────────────────────────────────────────

print("Building average baseline ...")
baseline_avg = df_cah.groupby(['Unified_CCN', 'State'])[BASELINE_COLS].mean().reset_index()

# Most-common hospital name, city, county
for col, target in [('Hospital_Name', 'Hospital_Name'), ('City', 'City'), ('County', 'County')]:
    col_map = df_cah.groupby('Unified_CCN')[col].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0]
    ).reset_index().rename(columns={col: target})
    baseline_avg = baseline_avg.merge(col_map, on='Unified_CCN', how='left')

# Rural/RUCA — take first non-null per hospital
for col in ('Rural_Urban', 'RUCA_Code'):
    col_map = (
        df_cah.groupby('Unified_CCN')[col]
        .first()
        .reset_index()
    )
    baseline_avg = baseline_avg.merge(col_map, on='Unified_CCN', how='left')

# Year counts
yr_counts = df_cah.groupby('Unified_CCN')['Year'].agg(['count', 'min', 'max']).reset_index()
yr_counts.columns = ['Unified_CCN', 'N_CAH_Years', 'First_CAH_Year', 'Last_CAH_Year']
baseline_avg = baseline_avg.merge(yr_counts, on='Unified_CCN', how='left')

# Boolean type flags
type_map = df_cah.groupby('Unified_CCN')[['Is_True_CAH', 'Is_PPS_Converter']].first().reset_index()
baseline_avg = baseline_avg.merge(type_map, on='Unified_CCN', how='left')
baseline_avg['Is_340B_Enrolled'] = (baseline_avg['Is_340B_Enrolled'] > 0).astype(int)

# Solvency
solv = baseline_avg[['Days_Cash_on_Hand', 'Current_Ratio', 'Debt_Ratio', 'Operating_Margin_Pct']].apply(
    lambda row: classify_solvency(row), axis=1, result_type='expand'
)
solv.columns = ['Solvency_Score', 'Solvency_Status', 'Solvency_Flags']
baseline_avg = pd.concat([baseline_avg, solv], axis=1)

# Derived financial fields
baseline_avg['Historical_Op_Income'] = (
    baseline_avg['NASHP_Net_Patient_Revenue'] - baseline_avg['Total_Hospital_Expenses']
)
baseline_avg['Historical_Op_Margin'] = np.where(
    baseline_avg['NASHP_Net_Patient_Revenue'] > 0,
    baseline_avg['Historical_Op_Income'] / baseline_avg['NASHP_Net_Patient_Revenue'],
    np.nan
)
baseline_avg['IP_Revenue'] = (
    baseline_avg['NASHP_Net_Patient_Revenue'] * (1 - baseline_avg['OP_Share'].fillna(0.5))
)
baseline_avg['OP_Revenue'] = (
    baseline_avg['NASHP_Net_Patient_Revenue'] * baseline_avg['OP_Share'].fillna(0.5)
)
baseline_avg['Swing_Bed_Risk'] = (1 - baseline_avg['OP_Share'].fillna(0.5)) > 0.25

# Converter metadata
conv_meta = (
    df_scope[df_scope['Is_PPS_Converter']]
    .groupby('Unified_CCN')[['REH_Conv_dt', 'Pre_REH_CCN']].first().reset_index()
)
conv_meta.columns = ['Unified_CCN', 'REH_Conversion_Date', 'Pre_REH_CCN_raw']
baseline_avg = baseline_avg.merge(conv_meta, on='Unified_CCN', how='left')
baseline_avg['REH_Conversion_Date'] = (
    baseline_avg['REH_Conversion_Date'].astype(str).replace('NaT', None)
)

# Road distance — most recent non-null value per hospital
dist_map = (
    df_cah.sort_values('FY_End_dt')
    .groupby('Unified_CCN')['nearest_building_road_dist_miles']
    .last()
    .reset_index()
)
baseline_avg = baseline_avg.merge(dist_map, on='Unified_CCN', how='left')

# SDOH — first non-null per hospital, renamed to SDOH_* keys
for raw_col, sdoh_key in SDOH_RENAMES.items():
    if raw_col in df_cah.columns:
        sdoh_map = (
            df_cah.groupby('Unified_CCN')[raw_col]
            .first()
            .reset_index()
            .rename(columns={raw_col: sdoh_key})
        )
        baseline_avg = baseline_avg.merge(sdoh_map, on='Unified_CCN', how='left')

print(f"  {len(baseline_avg):,} hospitals in average baseline")

# ── Most-recent-year baseline ─────────────────────────────────────────────────

print("Building most-recent-year baseline ...")
df_cah_sorted = df_cah.sort_values('FY_End_dt')
baseline_recent = df_cah_sorted.groupby('Unified_CCN').last().reset_index()
baseline_recent['Baseline_Year'] = baseline_recent['Year']

keep_cols = ['Unified_CCN', 'Baseline_Year'] + [c for c in BASELINE_COLS if c in baseline_recent.columns]
baseline_recent = baseline_recent[keep_cols].copy()

# Merge metadata from avg baseline
meta_cols = [
    'Unified_CCN', 'State', 'Hospital_Name', 'City', 'County',
    'Rural_Urban', 'RUCA_Code',
    'N_CAH_Years', 'First_CAH_Year', 'Last_CAH_Year',
    'Is_True_CAH', 'Is_PPS_Converter', 'REH_Conversion_Date',
    'nearest_building_road_dist_miles',
] + [v for v in SDOH_RENAMES.values() if v in baseline_avg.columns]
baseline_recent = baseline_recent.merge(
    baseline_avg[[c for c in meta_cols if c in baseline_avg.columns]],
    on='Unified_CCN', how='left'
)
baseline_recent['Is_340B_Enrolled'] = (baseline_recent['Is_340B_Enrolled'] > 0).astype(int)

# Solvency
solv_r = baseline_recent[['Days_Cash_on_Hand', 'Current_Ratio', 'Debt_Ratio', 'Operating_Margin_Pct']].apply(
    lambda row: classify_solvency(row), axis=1, result_type='expand'
)
solv_r.columns = ['Solvency_Score', 'Solvency_Status', 'Solvency_Flags']
baseline_recent = pd.concat([baseline_recent, solv_r], axis=1)

# Derived financial fields
baseline_recent['Historical_Op_Income'] = (
    baseline_recent['NASHP_Net_Patient_Revenue'] - baseline_recent['Total_Hospital_Expenses']
)
baseline_recent['Historical_Op_Margin'] = np.where(
    baseline_recent['NASHP_Net_Patient_Revenue'] > 0,
    baseline_recent['Historical_Op_Income'] / baseline_recent['NASHP_Net_Patient_Revenue'],
    np.nan
)
baseline_recent['IP_Revenue'] = (
    baseline_recent['NASHP_Net_Patient_Revenue'] * (1 - baseline_recent['OP_Share'].fillna(0.5))
)
baseline_recent['OP_Revenue'] = (
    baseline_recent['NASHP_Net_Patient_Revenue'] * baseline_recent['OP_Share'].fillna(0.5)
)
baseline_recent['Swing_Bed_Risk'] = (1 - baseline_recent['OP_Share'].fillna(0.5)) > 0.25

print(f"  {len(baseline_recent):,} hospitals in most-recent baseline")

# ── Serialize ─────────────────────────────────────────────────────────────────

output = {
    "metadata": {
        "generated_from": str(SST_PATH.name),
        "processing_date": date.today().isoformat(),
        "states": ["AR", "LA", "NM", "OK", "TX"],
        "total_hospitals_avg":    int(len(baseline_avg)),
        "true_cahs_avg":          int(baseline_avg['Is_True_CAH'].sum()),
        "total_hospitals_recent": int(len(baseline_recent)),
        "true_cahs_recent":       int(baseline_recent['Is_True_CAH'].sum()),
    },
    "baseline_average":     clean_for_json(baseline_avg),
    "baseline_most_recent": clean_for_json(baseline_recent),
}

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
print(f"Writing {OUTPUT_PATH} ...")
with OUTPUT_PATH.open('w', encoding='utf-8') as f:
    json.dump(output, f, separators=(',', ':'), default=str)

size_kb = len(json.dumps(output).encode('utf-8')) / 1024
print(f"Done. {size_kb:.0f} KB, {len(output['baseline_average'])} avg records, "
      f"{len(output['baseline_most_recent'])} recent records")
