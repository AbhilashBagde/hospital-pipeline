"""
build_dataset.py
================
Builds the Final_Hospital_Dataset.csv from raw source files.

Required input files (all in the same folder as this script):
  - Hospital_Provider_Cost_Report_2020.csv
  - Hospital_Provider_Cost_Report_2021.csv
  - Hospital_Provider_Cost_Report_2022.csv
  - Hospital_Provider_Cost_Report_2023_updated.csv
  - NASHP 2020-2024 HCT Data 2025 Dec (1).xlsx
  - REH Info Cleaned.csv

Output:
  - Final_Hospital_Dataset.csv

Usage:
  python build_dataset.py
"""

import json
import pandas as pd
import numpy as np
import os

# ---------------------------------------------------------------------------
# CONFIGURATION  (defaults; pipeline.py overrides via BD_* env vars)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CMS_FILES = {
    2020: os.path.join(BASE_DIR, "Hospital_Provider_Cost_Report_2020.csv"),
    2021: os.path.join(BASE_DIR, "Hospital_Provider_Cost_Report_2021.csv"),
    2022: os.path.join(BASE_DIR, "Hospital_Provider_Cost_Report_2022.csv"),
    2023: os.path.join(BASE_DIR, "Hospital_Provider_Cost_Report_2023.csv"),
}

NASHP_FILE  = os.path.join(BASE_DIR, "NASHP 2020-2024 HCT Data 2025 Dec.xlsx")
REH_FILE    = os.path.join(BASE_DIR, "REH Info Cleaned.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "Final_Hospital_Dataset.csv")

# Allow pipeline.py to override paths via environment variables
if os.environ.get("BD_CMS_FILES"):
    CMS_FILES = {int(k): v for k, v in json.loads(os.environ["BD_CMS_FILES"]).items()}
if os.environ.get("BD_NASHP_FILE"):
    NASHP_FILE = os.environ["BD_NASHP_FILE"]
if os.environ.get("BD_REH_FILE"):
    REH_FILE = os.environ["BD_REH_FILE"]
if os.environ.get("BD_OUTPUT_FILE"):
    OUTPUT_FILE = os.environ["BD_OUTPUT_FILE"]


# ---------------------------------------------------------------------------
# STEP 1: LOAD AND STACK CMS COST REPORTS (2020-2023)
# ---------------------------------------------------------------------------
# CMS raw column  →  standardized name used in final dataset
CMS_COL_MAP = {
    "Provider CCN":                              "CCN",
    "Hospital Name":                             "Hospital_Name_CMS",
    "Street Address":                            "Address",
    "City":                                      "City",
    "State Code":                                "State",
    "Zip Code":                                  "Zip",
    "County":                                    "County",
    "Medicare CBSA Number":                      "CBSA",
    "Rural Versus Urban":                        "Rural_Urban",
    "CCN Facility Type":                         "Facility_Type_CMS",
    "Provider Type":                             "Provider_Type",
    "Type of Control":                           "Type_of_Control",
    "Fiscal Year Begin Date":                    "FY_Begin",
    "Fiscal Year End Date":                      "FY_End",
    "FTE - Employees on Payroll":                "FTE_Employees",
    "Number of Beds":                            "Num_Beds",
    "Total Days Title XVIII":                    "Medicare_Days",
    "Total Days Title XIX":                      "Medicaid_Days",
    "Total Days (V + XVIII + XIX + Unknown)":    "Total_Patient_Days",
    "Total Discharges Title XVIII":              "Medicare_Discharges",
    "Total Discharges Title XIX":                "Medicaid_Discharges",
    "Total Discharges (V + XVIII + XIX + Unknown)": "Total_Discharges",
    "Inpatient Revenue":                         "CMS_Inpatient_Revenue",
    "Outpatient Revenue":                        "CMS_Outpatient_Revenue",
    "Net Patient Revenue":                       "CMS_Net_Patient_Revenue",
    "Less Total Operating Expense":              "CMS_Operating_Expense",
    "Net Income from Service to Patients":       "CMS_Net_Income_from_Patients",
    "Total Other Income":                        "CMS_Other_Income",
    "Net Income":                                "CMS_Net_Income",
    "Total Salaries From Worksheet A":           "CMS_Total_Salaries",
    "Cost of Charity Care":                      "CMS_Charity_Care_Cost",
    "Total Bad Debt Expense":                    "CMS_Bad_Debt_Expense",
    "Cost of Uncompensated Care":                "CMS_Uncompensated_Care",
    "Total Assets":                              "CMS_Total_Assets",
    "Total Liabilities":                         "CMS_Total_Liabilities",
    "Total Current Assets":                      "CMS_Current_Assets",
    "Total Current Liabilities":                 "CMS_Current_Liabilities",
    "Cash on Hand and in Banks":                 "CMS_Cash",
    "Depreciation Cost":                         "CMS_Depreciation",
    "Disproportionate Share Adjustment":         "CMS_DSH_Payment",
    "Allowable DSH Percentage":                  "CMS_DSH_Pct",
    "Net Revenue from Medicaid":                 "CMS_Medicaid_Revenue",
    "Total Costs":                               "CMS_Total_Costs",
    "Cost To Charge Ratio":                      "Cost_to_Charge_Ratio",
    "General Fund Balance":                      "Fund_Balance",
}

# ---------------------------------------------------------------------------
# HELPER: DOMINANT YEAR
# ---------------------------------------------------------------------------
def dominant_year(begin_series, end_series):
    """Return the calendar year that contains the most days in each FY range.
    E.g. 4/1/2019–3/31/2020 → 2019 (275 days in 2019 vs 91 in 2020).
    Falls back to the original value when dates are missing.
    """
    result = []
    for begin, end in zip(
        pd.to_datetime(begin_series, errors="coerce"),
        pd.to_datetime(end_series,   errors="coerce"),
    ):
        if pd.isna(begin) or pd.isna(end):
            result.append(pd.NA)
            continue
        best_year, best_days = begin.year, 0
        for yr in range(begin.year, end.year + 1):
            yr_start = max(begin, pd.Timestamp(yr, 1, 1))
            yr_end   = min(end,   pd.Timestamp(yr, 12, 31))
            days = (yr_end - yr_start).days + 1
            if days > best_days:
                best_days, best_year = days, yr
        result.append(best_year)
    return pd.array(result, dtype="Int64")


print("STEP 1: Loading CMS cost reports...")
cms_frames = []
for year, path in CMS_FILES.items():
    df = pd.read_csv(path, low_memory=False)
    df = df.rename(columns=CMS_COL_MAP)
    # Keep only the columns we need (mapped ones)
    keep = [c for c in CMS_COL_MAP.values() if c in df.columns]
    df = df[keep].copy()
    df["Year"] = year
    df["Data_Source_CMS"] = 1
    cms_frames.append(df)
    print(f"  {year}: {len(df):,} rows")

cms = pd.concat(cms_frames, ignore_index=True)
cms["CCN"] = pd.to_numeric(cms["CCN"], errors="coerce")
cms = cms.dropna(subset=["CCN"])
cms["CCN"] = cms["CCN"].astype(int)
# Overwrite Year with the calendar year containing the most FY days
cms["Year"] = dominant_year(cms["FY_Begin"], cms["FY_End"])
print(f"  CMS total: {len(cms):,} rows\n")


# ---------------------------------------------------------------------------
# STEP 2: DEDUPLICATE CMS — SPLIT FISCAL YEAR RECORDS
# ---------------------------------------------------------------------------
# Some hospitals file two cost reports in the same calendar year due to
# ownership changes or fiscal year realignments. CMS requires a new filing
# whenever this happens, resulting in two partial-year reports for the same
# CCN+Year. We keep the longer reporting period as the most representative.
# Tiebreaker: keep the more recent FY_Begin (successor / surviving entity).
print("STEP 2: Deduplicating CMS split fiscal year records...")
cms["FY_Begin_dt"] = pd.to_datetime(cms["FY_Begin"], errors="coerce")
cms["FY_End_dt"]   = pd.to_datetime(cms["FY_End"],   errors="coerce")
cms["FY_days"]     = (cms["FY_End_dt"] - cms["FY_Begin_dt"]).dt.days

before = len(cms)
cms = cms.sort_values(
    ["CCN", "Year", "FY_days", "FY_Begin_dt"],
    ascending=[True, True, False, False]
).drop_duplicates(subset=["CCN", "Year"], keep="first")
cms = cms.drop(columns=["FY_Begin_dt", "FY_End_dt", "FY_days"])
print(f"  Removed {before - len(cms)} duplicate rows -> {len(cms):,} rows remaining\n")


# ---------------------------------------------------------------------------
# STEP 3: LOAD NASHP DATA (2020-2024)
# ---------------------------------------------------------------------------
# NASHP column  →  standardized name
NASHP_COL_MAP = {
    "CCN#":                                              "CCN",
    " Facility Type":                                    "Facility_Type_NASHP",
    "Year":                                              "Year",
    "Fiscal Year Beginning":                             "FY_Begin",
    "Fiscal Year Ending":                                "FY_End",
    "Hospital Name":                                     "Hospital_Name_NASHP",
    "Health System ID":                                  "Health_System_ID",
    "Health System":                                     "Health_System",
    "Hospital Ownership Type":                           "Ownership_Type",
    "Independent":                                       "Is_Independent",
    "Bed Size":                                          "NASHP_Bed_Size",
    "Inpatient Occupancy":                               "Inpatient_Occupancy",
    "Net Patient Revenue":                               "NASHP_Net_Patient_Revenue",
    "Operating Expenses":                                "NASHP_Operating_Expenses",
    "Net Income (Loss)":                                 "NASHP_Net_Income",
    "Net Profit Margin":                                 "NASHP_Net_Profit_Margin",
    "Fund Balance":                                      "NASHP_Fund_Balance",
    "Hospital Operating Costs":                          "NASHP_Hospital_Op_Costs",
    "Net Charity Care Cost":                             "NASHP_Charity_Care_Cost",
    "Uninsured and Bad Debt Cost":                       "NASHP_Uninsured_Bad_Debt",
    "Operating Profit (Loss)":                           "NASHP_Operating_Profit",
    "Operating Profit Margin":                           "NASHP_Operating_Margin",
    "Geographic Classification (Urban=1, Rural=2)":      "NASHP_Geo_Classification",
    "Medicaid Payer Mix":                                "Medicaid_Payer_Mix_Pct",
    "SCHIP and Low Income Gov't Program Payer Mix":      "SCHIP_Payer_Mix_Pct",
    "Medicare Payer Mix":                                "Medicare_Payer_Mix_Pct",
    "Medicare Adv Payer Mix":                            "Medicare_Adv_Payer_Mix_Pct",
    "Commercial Payer Mix":                              "Commercial_Payer_Mix_Pct",
    "Medicaid Operating Profit Margin":                  "Medicaid_Op_Margin",
    "Medicare Operating Profit Margin":                  "Medicare_Op_Margin",
    "Medicare Adv Operating Profit Margin":              "Medicare_Adv_Op_Margin",
    "Commercial Operating Profit Margin":                "Commercial_Op_Margin",
    "Medicaid Revenue as % of Net Patient Revenue":      "Medicaid_Revenue_Pct",
    "COVID-19 PHE Funding":                              "COVID_PHE_Funding",
    "Direct Patient Care Hospital Labor Cost":           "Direct_Labor_Cost",
    "Direct Patient Care Contracted Labor Cost":         "Contract_Labor_Cost",
    "RUCA Primary Code":                                 "RUCA_Code",
    "Adjusted Patient Discharges":                       "Adjusted_Patient_Discharges",
    "Net Patient Revenue per Adjusted Discharge":        "Rev_per_Adj_Discharge",
    "Hospital Operating Costs per Adjusted Discharge":   "Cost_per_Adj_Discharge",
    "Hospital Payment Type (P=PPS, T=TEFRA, O=Other, N= Not applicable)": "Payment_Type",
    "Hospital Expenses (Inclusive of All Services)":     "Total_Hospital_Expenses",
}

print("STEP 3: Loading NASHP data...")
nashp = pd.read_excel(NASHP_FILE, sheet_name="Downloadable_2020-24")
nashp = nashp.rename(columns=NASHP_COL_MAP)
keep_nashp = [c for c in NASHP_COL_MAP.values() if c in nashp.columns]
nashp = nashp[keep_nashp].copy()
nashp["CCN"] = pd.to_numeric(nashp["CCN"], errors="coerce")
nashp = nashp.dropna(subset=["CCN"])
nashp["CCN"] = nashp["CCN"].astype(int)
nashp["Year"] = pd.to_numeric(nashp["Year"], errors="coerce").astype("Int64")
nashp["Data_Source_NASHP"] = 1
# Overwrite Year with the calendar year containing the most FY days
nashp["Year"] = dominant_year(nashp["FY_Begin"], nashp["FY_End"])
print(f"  NASHP total: {len(nashp):,} rows\n")


# ---------------------------------------------------------------------------
# STEP 4: MERGE CMS + NASHP ON CCN + YEAR
# ---------------------------------------------------------------------------
# Outer join so hospitals present in only one source are kept.
# 2024 data is NASHP-only (CMS reports not yet available).
print("STEP 4: Merging CMS and NASHP...")

# NASHP has FY_Begin/FY_End too — rename to avoid collision before merge
nashp = nashp.rename(columns={"FY_Begin": "FY_Begin_NASHP", "FY_End": "FY_End_NASHP"})

merged = pd.merge(
    cms,
    nashp,
    on=["CCN", "Year"],
    how="outer",
    suffixes=("_CMS", "_NASHP"),
)

# Fill CMS flag NaN → 0
merged["Data_Source_CMS"]   = merged["Data_Source_CMS"].fillna(0).astype(int)
merged["Data_Source_NASHP"] = merged["Data_Source_NASHP"].fillna(0).astype(int)

# Build human-readable Data_Source label
def data_source_label(row):
    if row["Data_Source_CMS"] == 1 and row["Data_Source_NASHP"] == 1:
        return "CMS + NASHP"
    elif row["Data_Source_CMS"] == 1:
        return "CMS only"
    else:
        return "NASHP only"

merged["Data_Source"] = merged.apply(data_source_label, axis=1)

# Unified Hospital_Name: prefer CMS name; fall back to NASHP name
merged["Hospital_Name"] = merged["Hospital_Name_CMS"].combine_first(merged["Hospital_Name_NASHP"])

# Unified Facility_Type: prefer CMS; fall back to NASHP
merged["Facility_Type"] = merged["Facility_Type_CMS"].combine_first(merged["Facility_Type_NASHP"])

# Rename CMS dates to source-specific names
merged = merged.rename(columns={"FY_Begin": "FY_Begin_CMS", "FY_End": "FY_End_CMS"})

print(f"  Merged shape: {merged.shape[0]:,} rows × {merged.shape[1]} columns")
print(f"  Data source breakdown:\n{merged['Data_Source'].value_counts().to_string()}\n")


# ---------------------------------------------------------------------------
# STEP 5: DERIVE CLASSIFICATION FLAGS
# ---------------------------------------------------------------------------
print("STEP 5: Adding classification flags...")

# Is_CAH: Critical Access Hospital
# Source: CMS Facility Type = 'CAH'  OR  NASHP Facility Type = 'Critical Access Hospitals'
merged["Is_CAH"] = (
    (merged["Facility_Type_CMS"] == "CAH") |
    (merged["Facility_Type_NASHP"] == "Critical Access Hospitals")
).astype(int)

print(f"  CAH rows: {merged['Is_CAH'].sum():,}")
print(f"  Unique CAH CCNs: {merged[merged['Is_CAH']==1]['CCN'].nunique():,}")


# ---------------------------------------------------------------------------
# STEP 6: DERIVE FINANCIAL RATIOS
# ---------------------------------------------------------------------------
print("\nSTEP 6: Deriving financial ratios...")

# Force numeric on all financial columns
fin_cols = [
    "CMS_Net_Patient_Revenue", "CMS_Net_Income_from_Patients", "CMS_Net_Income",
    "CMS_Current_Assets", "CMS_Current_Liabilities", "CMS_Total_Assets",
    "CMS_Total_Liabilities", "CMS_Cash", "CMS_Operating_Expense",
    "CMS_Total_Salaries", "CMS_Total_Costs", "CMS_Outpatient_Revenue",
    "CMS_Charity_Care_Cost", "Medicare_Days", "Medicaid_Days", "Total_Patient_Days",
    "Total_Discharges", "NASHP_Net_Patient_Revenue", "NASHP_Hospital_Op_Costs",
    "Adjusted_Patient_Discharges", "NASHP_Operating_Margin", "NASHP_Net_Profit_Margin",
]
for col in fin_cols:
    if col in merged.columns:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

def safe_div(num, denom):
    """Divide two series, returning NaN where denominator is 0 or NaN."""
    return np.where(
        denom.notna() & (denom != 0) & num.notna(),
        num / denom,
        np.nan
    )

# Operating Margin % — from NASHP (decimal → percentage)
merged["Operating_Margin_Pct"] = merged["NASHP_Operating_Margin"] * 100

# Net Profit Margin % — from NASHP (decimal → percentage)
merged["Net_Profit_Margin_Pct"] = merged["NASHP_Net_Profit_Margin"] * 100

# Current Ratio = Current Assets / Current Liabilities
merged["Current_Ratio"] = safe_div(merged["CMS_Current_Assets"], merged["CMS_Current_Liabilities"])

# Debt Ratio = Total Liabilities / Total Assets
merged["Debt_Ratio"] = safe_div(merged["CMS_Total_Liabilities"], merged["CMS_Total_Assets"])

# Days Cash on Hand = Cash / (Operating Expense / 365)
merged["Days_Cash_on_Hand"] = safe_div(merged["CMS_Cash"], merged["CMS_Operating_Expense"] / 365)

# Labor Cost % = Total Salaries / Total Costs * 100
merged["Labor_Cost_Pct"] = safe_div(merged["CMS_Total_Salaries"], merged["CMS_Total_Costs"]) * 100

# Revenue per Discharge = Net Patient Revenue / Total Discharges
merged["Rev_per_Discharge"] = safe_div(merged["CMS_Net_Patient_Revenue"], merged["Total_Discharges"])

# Cost per Discharge = Total Costs / Total Discharges
merged["Cost_per_Discharge"] = safe_div(merged["CMS_Total_Costs"], merged["Total_Discharges"])

# Outpatient Revenue % = Outpatient Revenue / Net Patient Revenue * 100
merged["Outpatient_Rev_Pct"] = safe_div(merged["CMS_Outpatient_Revenue"], merged["CMS_Net_Patient_Revenue"]) * 100

# Medicare % of Days = Medicare Days / Total Patient Days * 100
merged["Medicare_Pct_Days"] = safe_div(merged["Medicare_Days"], merged["Total_Patient_Days"]) * 100

# Medicaid % of Days = Medicaid Days / Total Patient Days * 100
merged["Medicaid_Pct_Days"] = safe_div(merged["Medicaid_Days"], merged["Total_Patient_Days"]) * 100

# Charity Care % of Revenue = Charity Care Cost / Net Patient Revenue * 100
merged["Charity_Care_Pct_Revenue"] = safe_div(merged["CMS_Charity_Care_Cost"], merged["CMS_Net_Patient_Revenue"]) * 100

# Revenue per Adjusted Discharge — from NASHP
merged["Rev_per_Adj_Discharge"] = safe_div(merged["NASHP_Net_Patient_Revenue"], merged["Adjusted_Patient_Discharges"])

# Cost per Adjusted Discharge — from NASHP
merged["Cost_per_Adj_Discharge"] = safe_div(merged["NASHP_Hospital_Op_Costs"], merged["Adjusted_Patient_Discharges"])

print("  Financial ratios derived.\n")


# ---------------------------------------------------------------------------
# STEP 7: ADD REH (RURAL EMERGENCY HOSPITAL) FLAGS
# ---------------------------------------------------------------------------
# REH Info contains hospitals that converted from CAH to REH.
# Key nuance: CMS issues a NEW CCN to many converting hospitals.
# We handle both cases:
#   - Hospitals that KEPT their CCN after converting
#   - Hospitals that received a NEW CCN (Post-REH CCN)
# For the latter, their post-conversion rows exist under the new CCN in the
# dataset but would otherwise be completely disconnected from their history.
# We fix this by:
#   1. Flagging all REH converter rows (Is_REH_Converter = 1)
#   2. Adding Pre_REH_CCN so analysts can join pre- and post-conversion data
#   3. Populating conversion date, status, and prior payment type
print("STEP 7: Adding REH conversion flags...")

reh_info = pd.read_csv(REH_FILE)

# Initialize REH columns
merged["Is_REH_Converter"]     = 0
merged["REH_Conversion_Date"]  = None   # object dtype — accepts date strings
merged["REH_Status"]           = None   # object dtype — accepts strings
merged["Pre_REH_Payment_Type"] = None   # object dtype — accepts strings
merged["Pre_REH_CCN"]          = np.nan

# Coerce CCN columns to numeric to handle floats / NaNs gracefully
reh_info["Pre-REH CCN"]  = pd.to_numeric(reh_info["Pre-REH CCN"],  errors="coerce")
reh_info["Post-REH CCN"] = pd.to_numeric(reh_info["Post-REH CCN"], errors="coerce")

# --- Case A: Hospitals that kept their CCN after converting ---
same_ccn = reh_info[reh_info["Post-REH CCN"].isna() & reh_info["Pre-REH CCN"].notna()].copy()
same_ccn["Pre_CCN"] = same_ccn["Pre-REH CCN"].astype(int)

for _, row in same_ccn.iterrows():
    mask = merged["CCN"] == row["Pre_CCN"]
    merged.loc[mask, "Is_REH_Converter"]     = 1
    merged.loc[mask, "REH_Conversion_Date"]  = row["REH Conversion Date"]
    merged.loc[mask, "REH_Status"]           = row["Current Status"]
    merged.loc[mask, "Pre_REH_Payment_Type"] = row["Previous Medicare Payment Type"]
    merged.loc[mask, "Pre_REH_CCN"]          = row["Pre_CCN"]

# --- Case B: Hospitals that received a NEW CCN after converting ---
# Their post-conversion data lives under the new CCN.
# Pre-conversion data lives under the old CCN.
has_post = reh_info[reh_info["Post-REH CCN"].notna()].copy()
has_post["Post_CCN"] = has_post["Post-REH CCN"].astype(int)
has_post = has_post[has_post["Pre-REH CCN"].notna()].copy()
has_post["Pre_CCN"]  = has_post["Pre-REH CCN"].astype(int)

for _, row in has_post.iterrows():
    # Flag pre-conversion rows (old CCN)
    mask_pre = merged["CCN"] == row["Pre_CCN"]
    merged.loc[mask_pre, "Is_REH_Converter"]     = 1
    merged.loc[mask_pre, "REH_Conversion_Date"]  = row["REH Conversion Date"]
    merged.loc[mask_pre, "REH_Status"]           = row["Current Status"]
    merged.loc[mask_pre, "Pre_REH_Payment_Type"] = row["Previous Medicare Payment Type"]
    merged.loc[mask_pre, "Pre_REH_CCN"]          = row["Pre_CCN"]

    # Flag post-conversion rows (new CCN) — previously had Is_REH_Converter = 0 (wrong)
    mask_post = merged["CCN"] == row["Post_CCN"]
    merged.loc[mask_post, "Is_REH_Converter"]     = 1
    merged.loc[mask_post, "REH_Conversion_Date"]  = row["REH Conversion Date"]
    merged.loc[mask_post, "REH_Status"]           = row["Current Status"]
    merged.loc[mask_post, "Pre_REH_Payment_Type"] = row["Previous Medicare Payment Type"]
    merged.loc[mask_post, "Pre_REH_CCN"]          = row["Pre_CCN"]

# Post_REH_Months: number of months the hospital has been operating as an REH
# Calculated as months between REH_Conversion_Date and FY_End
merged["REH_Conversion_Date_dt"] = pd.to_datetime(merged["REH_Conversion_Date"], errors="coerce")
merged["FY_End_dt"]              = pd.to_datetime(merged["FY_End_NASHP"], errors="coerce").fillna(
                                   pd.to_datetime(merged["FY_End_CMS"], errors="coerce"))
reh_mask = merged["Is_REH_Converter"] == 1
merged.loc[reh_mask, "Post_REH_Months"] = (
    (merged.loc[reh_mask, "FY_End_dt"] - merged.loc[reh_mask, "REH_Conversion_Date_dt"])
    .dt.days / 30.44
).clip(lower=0).round(1)
merged = merged.drop(columns=["REH_Conversion_Date_dt", "FY_End_dt"], errors="ignore")

print(f"  REH converter rows flagged: {merged['Is_REH_Converter'].sum()}")
print(f"  Unique REH converter CCNs: {merged[merged['Is_REH_Converter']==1]['CCN'].nunique()}\n")


# ---------------------------------------------------------------------------
# STEP 8: FINAL COLUMN ORDERING AND CLEANUP
# ---------------------------------------------------------------------------
print("STEP 8: Finalizing column order and cleanup...")

FINAL_COLUMNS = [
    # Identifiers
    "CCN", "Hospital_Name", "State", "City", "Zip", "County", "Address", "Year",
    "FY_Begin_NASHP", "FY_End_NASHP",
    "FY_Begin_CMS", "FY_End_CMS",
    # Source tracking
    "Data_Source", "Data_Source_CMS", "Data_Source_NASHP",
    # Classification
    "Facility_Type", "Facility_Type_CMS", "Facility_Type_NASHP",
    "Rural_Urban", "RUCA_Code", "CBSA",
    "Is_CAH",
    # REH flags
    "Is_REH_Converter", "REH_Conversion_Date", "Post_REH_Months",
    "REH_Status", "Pre_REH_CCN", "Pre_REH_Payment_Type",
    # Ownership / system
    "Ownership_Type", "Health_System", "Health_System_ID", "Is_Independent",
    "Payment_Type", "Type_of_Control",
    # Operations
    "Num_Beds", "NASHP_Bed_Size", "FTE_Employees",
    "Total_Discharges", "Total_Patient_Days",
    "Medicare_Days", "Medicaid_Days",
    "Medicare_Discharges", "Medicaid_Discharges",
    "Adjusted_Patient_Discharges", "Inpatient_Occupancy",
    # Derived financial ratios
    "Operating_Margin_Pct", "Net_Profit_Margin_Pct",
    "Current_Ratio", "Debt_Ratio",
    "Medicare_Pct_Days", "Medicaid_Pct_Days",
    "Outpatient_Rev_Pct", "Days_Cash_on_Hand", "Labor_Cost_Pct",
    "Rev_per_Discharge", "Cost_per_Discharge",
    "Rev_per_Adj_Discharge", "Cost_per_Adj_Discharge",
    "Charity_Care_Pct_Revenue",
    # CMS financial columns
    "CMS_Inpatient_Revenue", "CMS_Outpatient_Revenue", "CMS_Net_Patient_Revenue",
    "CMS_Total_Costs", "CMS_Operating_Expense",
    "CMS_Net_Income_from_Patients", "CMS_Other_Income", "CMS_Net_Income",
    "CMS_Total_Salaries", "CMS_Charity_Care_Cost", "CMS_Bad_Debt_Expense",
    "CMS_Uncompensated_Care",
    "CMS_Total_Assets", "CMS_Total_Liabilities",
    "CMS_Current_Assets", "CMS_Current_Liabilities",
    "CMS_Cash", "CMS_Depreciation",
    "CMS_DSH_Payment", "CMS_DSH_Pct", "CMS_Medicaid_Revenue",
    "Cost_to_Charge_Ratio", "Fund_Balance",
    # NASHP financial columns
    "NASHP_Net_Patient_Revenue", "NASHP_Operating_Expenses", "NASHP_Net_Income",
    "NASHP_Net_Profit_Margin", "NASHP_Hospital_Op_Costs",
    "NASHP_Charity_Care_Cost", "NASHP_Uninsured_Bad_Debt",
    "NASHP_Operating_Profit", "NASHP_Operating_Margin",
    "NASHP_Geo_Classification", "Total_Hospital_Expenses",
    # Payer mix
    "Medicaid_Payer_Mix_Pct", "SCHIP_Payer_Mix_Pct",
    "Medicare_Payer_Mix_Pct", "Medicare_Adv_Payer_Mix_Pct",
    "Commercial_Payer_Mix_Pct", "Medicaid_Revenue_Pct",
    # Payer margins
    "Medicaid_Op_Margin", "Medicare_Op_Margin",
    "Medicare_Adv_Op_Margin", "Commercial_Op_Margin",
    # Other
    "COVID_PHE_Funding", "Direct_Labor_Cost", "Contract_Labor_Cost",
    "Hospital_Name_CMS", "Hospital_Name_NASHP",
]

# Keep only columns that exist in the merged frame
final_cols = [c for c in FINAL_COLUMNS if c in merged.columns]
# Append any remaining columns not in our ordered list
extra_cols = [c for c in merged.columns if c not in final_cols]
final = merged[final_cols + extra_cols].copy()

# Clean up zip codes (strip trailing dash/spaces)
if "Zip" in final.columns:
    final["Zip"] = final["Zip"].astype(str).str.strip().str.split("-").str[0].str.zfill(5)
    final["Zip"] = final["Zip"].replace("00nan", np.nan)

print(f"  Final shape: {final.shape[0]:,} rows × {final.shape[1]} columns\n")


# ---------------------------------------------------------------------------
# STEP 9: SAVE
# ---------------------------------------------------------------------------
final.to_csv(OUTPUT_FILE, index=False)
print(f"DONE. Saved to: {OUTPUT_FILE}")
print(f"\nSummary:")
print(f"  Total rows      : {len(final):,}")
print(f"  Unique hospitals: {final['CCN'].nunique():,}")
print(f"  Year range      : {final['Year'].min()} – {final['Year'].max()}")
print(f"  Duplicate CCN+Year: {final.duplicated(subset=['CCN','Year']).sum()}")
print(f"  Null CCNs       : {final['CCN'].isnull().sum()}")
print(f"  REH converters  : {final['Is_REH_Converter'].sum()} rows, {final[final['Is_REH_Converter']==1]['CCN'].nunique()} unique hospitals")
print(f"  CAH hospitals   : {final[final['Is_CAH']==1]['CCN'].nunique():,} unique")
print(f"\nData source breakdown:")
print(final["Data_Source"].value_counts().to_string())
