"""
build_q2_visuals.py
===================
Generates Q2_Visualizations.xlsx — one chart per sheet — from
Final_Hospital_Dataset.csv.

Run:
    python build_q2_visuals.py

Output:
    data/output/Q2_Visualizations.xlsx
"""

import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import numpy as np
import pandas as pd
import xlsxwriter

SRC  = Path("data/output/Final_Hospital_Dataset.csv")
DEST = Path("data/output/Q2_Visualizations.xlsx")

# ── helpers ───────────────────────────────────────────────────────────────────

HEADER_FMT = dict(bold=True, font_color="white", bg_color="#1F4E79",
                  border=1, align="center", valign="vcenter")
SUBHEADER_FMT = dict(bold=True, bg_color="#D6E4F0", border=1, align="center")
NUMBER_FMT  = "#,##0.0"
PCT_FMT     = "0.0%"
INT_FMT     = "#,##0"

FACILITY_COLORS = {
    "STH":   "#2E75B6",
    "CAH":   "#ED7D31",
    "LTCH":  "#A9D18E",
    "PH":    "#FFC000",
    "RH":    "#5B9BD5",
    "Other": "#767171",
}

def write_header(ws, fmt, title, row=0, cols=1):
    ws.merge_range(row, 0, row, cols - 1, title, fmt)
    ws.set_row(row, 22)

def autofit(ws, df, offset_col=0):
    for i, col in enumerate(df.columns):
        width = max(len(str(col)), df[col].astype(str).str.len().max())
        ws.set_column(offset_col + i, offset_col + i, min(width + 2, 40))


# ── load data ─────────────────────────────────────────────────────────────────

print("Loading dataset…")
df = pd.read_csv(SRC, low_memory=False)

# Normalise facility type to 5 buckets
def norm_ftype(ft):
    ft = str(ft).strip()
    if ft in ("STH", "Short Term (General and Specialty) Hospitals"): return "STH"
    if ft in ("CAH", "Critical Access Hospitals"):                     return "CAH"
    if ft == "LTCH":  return "LTCH"
    if ft == "PH":    return "PH"
    if ft == "RH":    return "RH"
    return "Other"

df["FType"] = df["Facility_Type"].apply(norm_ftype)

workbook  = xlsxwriter.Workbook(str(DEST))

hdr_fmt  = workbook.add_format(HEADER_FMT)
sub_fmt  = workbook.add_format(SUBHEADER_FMT)
num_fmt  = workbook.add_format({"num_format": NUMBER_FMT, "border": 1})
pct_fmt  = workbook.add_format({"num_format": "0.0%",    "border": 1})
int_fmt  = workbook.add_format({"num_format": INT_FMT,   "border": 1})
cell_fmt = workbook.add_format({"border": 1})
bold_fmt = workbook.add_format({"bold": True, "border": 1})

# ══════════════════════════════════════════════════════════════════════════════
# 1. National Financial Trends (2011-2024)
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 1: National Financial Trends")
ws = workbook.add_worksheet("1. National Trends")
ws.set_zoom(90)

pivot = (
    df.groupby(["Year", "FType"])["Operating_Margin_Pct"]
    .median().reset_index()
    .pivot(index="Year", columns="FType", values="Operating_Margin_Pct")
    .reset_index()
)
ftypes = [c for c in pivot.columns if c != "Year"]

write_header(ws, hdr_fmt, "Median Operating Margin % by Facility Type (2011–2024)", cols=len(pivot.columns))
ws.write_row(1, 0, pivot.columns.tolist(), sub_fmt)
for r, row in enumerate(pivot.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    for c, v in enumerate(row[1:], start=1):
        ws.write(r, c, round(float(v), 4) if pd.notna(v) else "", pct_fmt)

chart = workbook.add_chart({"type": "line"})
for i, ft in enumerate(ftypes):
    col = pivot.columns.tolist().index(ft)
    chart.add_series({
        "name":       ft,
        "categories": ["1. National Trends", 2, 0, 2 + len(pivot) - 1, 0],
        "values":     ["1. National Trends", 2, col, 2 + len(pivot) - 1, col],
        "line":       {"width": 2.25},
    })
chart.set_title({"name": "Median Operating Margin % by Facility Type"})
chart.set_x_axis({"name": "Year"})
chart.set_y_axis({"name": "Operating Margin %", "num_format": "0%"})
chart.set_size({"width": 720, "height": 400})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(pivot) + 2, 0, chart)
autofit(ws, pivot)


# ══════════════════════════════════════════════════════════════════════════════
# 2. State Performance — Top & Bottom 15 (most recent year)
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 2: State Performance")
ws = workbook.add_worksheet("2. State Performance")
ws.set_zoom(90)

latest = df[df["Year"] == df["Year"].max()]
state_perf = (
    latest.groupby("State")["Operating_Margin_Pct"]
    .median().dropna().sort_values()
    .reset_index()
)
state_perf.columns = ["State", "Median_Op_Margin"]
bottom15 = state_perf.head(15)
top15    = state_perf.tail(15).iloc[::-1]
combined = pd.concat([top15, pd.DataFrame([["---", None]], columns=top15.columns), bottom15], ignore_index=True)

write_header(ws, hdr_fmt, f"Top 15 & Bottom 15 States by Median Operating Margin ({df['Year'].max()})", cols=2)
ws.write_row(1, 0, ["State", "Median Op Margin"], sub_fmt)
for r, row in enumerate(combined.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, round(float(row[1]), 4) if pd.notna(row[1]) and row[1] != "---" else "", pct_fmt)

def make_bar(name, data_df, start_row, col_offset=4, title=""):
    chart = workbook.add_chart({"type": "bar"})
    chart.add_series({
        "name":       name,
        "categories": [ws.name, start_row, 0, start_row + len(data_df) - 1, 0],
        "values":     [ws.name, start_row, 1, start_row + len(data_df) - 1, 1],
        "fill":       {"color": "#2E75B6"},
    })
    chart.set_title({"name": title})
    chart.set_x_axis({"num_format": "0%"})
    chart.set_size({"width": 500, "height": 380})
    chart.set_legend({"none": True})
    return chart

top_chart = make_bar("Top 15", top15, 2, title=f"Top 15 States — Op Margin ({df['Year'].max()})")
bot_chart = make_bar("Bot 15", bottom15, 2 + 15 + 2, title=f"Bottom 15 States — Op Margin ({df['Year'].max()})")
top_chart.add_series({
    "name":       "Top 15",
    "categories": [ws.name, 2, 0, 2 + 14, 0],
    "values":     [ws.name, 2, 1, 2 + 14, 1],
    "fill":       {"color": "#2E75B6"},
})
ws.insert_chart(2 + len(combined) + 2, 0, top_chart)
ws.set_column(0, 0, 8)
ws.set_column(1, 1, 18)


# ══════════════════════════════════════════════════════════════════════════════
# 3. REH Conversion Analysis
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 3: REH Conversions")
ws = workbook.add_worksheet("3. REH Conversions")
ws.set_zoom(90)

reh = df[df["Is_REH_Converter"] == 1].drop_duplicates("CCN")
reh["Conv_Year"] = pd.to_datetime(reh["REH_Conversion_Date"], errors="coerce").dt.year
reh_by_year = (
    reh.groupby(["Conv_Year", "Pre_REH_Payment_Type"])
    .size().reset_index(name="Count")
    .pivot(index="Conv_Year", columns="Pre_REH_Payment_Type", values="Count")
    .fillna(0).astype(int).reset_index()
)

write_header(ws, hdr_fmt, "REH Conversions by Year and Prior Payment Type", cols=len(reh_by_year.columns))
ws.write_row(1, 0, reh_by_year.columns.tolist(), sub_fmt)
for r, row in enumerate(reh_by_year.itertuples(index=False), start=2):
    ws.write(r, 0, row[0] if pd.notna(row[0]) else "", cell_fmt)
    for c, v in enumerate(row[1:], start=1):
        ws.write(r, c, int(v), int_fmt)

ptype_cols = [c for c in reh_by_year.columns if c != "Conv_Year"]
chart = workbook.add_chart({"type": "column", "subtype": "stacked"})
colors = ["#ED7D31", "#2E75B6", "#A9D18E", "#FFC000"]
for i, pt in enumerate(ptype_cols):
    col_idx = reh_by_year.columns.tolist().index(pt)
    chart.add_series({
        "name":       pt,
        "categories": [ws.name, 2, 0, 2 + len(reh_by_year) - 1, 0],
        "values":     [ws.name, 2, col_idx, 2 + len(reh_by_year) - 1, col_idx],
        "fill":       {"color": colors[i % len(colors)]},
    })
chart.set_title({"name": "REH Conversions by Year (stacked by Prior Payment Type)"})
chart.set_x_axis({"name": "Conversion Year"})
chart.set_y_axis({"name": "Number of Hospitals"})
chart.set_size({"width": 640, "height": 380})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(reh_by_year) + 2, 0, chart)
autofit(ws, reh_by_year)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Ownership Type Financial Comparison
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 4: Ownership Comparison")
ws = workbook.add_worksheet("4. Ownership Comparison")
ws.set_zoom(90)

own = (
    df[df["Ownership_Type"].isin(["Non-Profit", "For-Profit", "Governmental"])]
    .groupby("Ownership_Type")
    .agg(
        Median_Op_Margin=("Operating_Margin_Pct", "median"),
        Median_Cost_per_Discharge=("Cost_per_Discharge", "median"),
        Median_Charity_Care_Pct=("Charity_Care_Pct_Revenue", "median"),
        Median_Medicare_Days_Pct=("Medicare_Pct_Days", "median"),
        Hospital_Count=("CCN", "nunique"),
    ).reset_index()
)

write_header(ws, hdr_fmt, "Financial Metrics by Ownership Type (All Years)", cols=len(own.columns))
ws.write_row(1, 0, own.columns.tolist(), sub_fmt)
for r, row in enumerate(own.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, round(float(row[1]), 4) if pd.notna(row[1]) else "", pct_fmt)
    ws.write(r, 2, round(float(row[2]), 0) if pd.notna(row[2]) else "", int_fmt)
    ws.write(r, 3, round(float(row[3]), 4) if pd.notna(row[3]) else "", pct_fmt)
    ws.write(r, 4, round(float(row[4]), 4) if pd.notna(row[4]) else "", pct_fmt)
    ws.write(r, 5, int(row[5]), int_fmt)

metrics = [
    ("Median_Op_Margin", 1, "Operating Margin %", "0%"),
    ("Median_Charity_Care_Pct", 3, "Charity Care % Revenue", "0%"),
    ("Median_Medicare_Days_Pct", 4, "Medicare Days %", "0%"),
]
for k, (metric, col_idx, title, num_fmt_str) in enumerate(metrics):
    chart = workbook.add_chart({"type": "column"})
    colors_own = ["#2E75B6", "#ED7D31", "#A9D18E"]
    chart.add_series({
        "name":       title,
        "categories": [ws.name, 2, 0, 2 + len(own) - 1, 0],
        "values":     [ws.name, 2, col_idx, 2 + len(own) - 1, col_idx],
        "points":     [{"fill": {"color": c}} for c in colors_own],
    })
    chart.set_title({"name": title + " by Ownership Type"})
    chart.set_y_axis({"num_format": num_fmt_str})
    chart.set_size({"width": 380, "height": 300})
    chart.set_legend({"none": True})
    ws.insert_chart(2 + len(own) + 2 + k * 20, 0, chart)
autofit(ws, own)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Rural vs Urban Financial Gap
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 5: Rural vs Urban")
ws = workbook.add_worksheet("5. Rural vs Urban")
ws.set_zoom(90)

ru = (
    df[df["Rural_Urban"].isin(["R", "U"])]
    .groupby(["Year", "Rural_Urban"])
    .agg(
        Op_Margin=("Operating_Margin_Pct", "median"),
        Cost_per_Discharge=("Cost_per_Discharge", "median"),
        Medicare_Days_Pct=("Medicare_Pct_Days", "median"),
    ).reset_index()
    .pivot(index="Year", columns="Rural_Urban", values="Op_Margin")
    .reset_index()
)
ru.columns = ["Year", "Rural", "Urban"]

write_header(ws, hdr_fmt, "Median Operating Margin % — Rural vs Urban (2011–2024)", cols=3)
ws.write_row(1, 0, ["Year", "Rural", "Urban"], sub_fmt)
for r, row in enumerate(ru.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, round(float(row[1]), 4) if pd.notna(row[1]) else "", pct_fmt)
    ws.write(r, 2, round(float(row[2]), 4) if pd.notna(row[2]) else "", pct_fmt)

chart = workbook.add_chart({"type": "line"})
for i, (label, color) in enumerate([("Rural", "#ED7D31"), ("Urban", "#2E75B6")]):
    chart.add_series({
        "name":       label,
        "categories": [ws.name, 2, 0, 2 + len(ru) - 1, 0],
        "values":     [ws.name, 2, i + 1, 2 + len(ru) - 1, i + 1],
        "line":       {"width": 2.5, "color": color},
        "marker":     {"type": "circle", "size": 5},
    })
chart.set_title({"name": "Rural vs Urban — Median Operating Margin %"})
chart.set_x_axis({"name": "Year"})
chart.set_y_axis({"name": "Operating Margin %", "num_format": "0%"})
chart.set_size({"width": 680, "height": 400})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(ru) + 2, 0, chart)
autofit(ws, ru)


# ══════════════════════════════════════════════════════════════════════════════
# 6. Payer Mix Trends
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 6: Payer Mix Trends")
ws = workbook.add_worksheet("6. Payer Mix Trends")
ws.set_zoom(90)

pm = (
    df.groupby("Year")
    .agg(
        Medicare=("Medicare_Pct_Days", "median"),
        Medicaid=("Medicaid_Pct_Days", "median"),
    ).reset_index()
)

write_header(ws, hdr_fmt, "Median Medicare & Medicaid Patient Day Mix (2011–2024)", cols=3)
ws.write_row(1, 0, ["Year", "Medicare %", "Medicaid %"], sub_fmt)
for r, row in enumerate(pm.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, round(float(row[1]), 4) if pd.notna(row[1]) else "", pct_fmt)
    ws.write(r, 2, round(float(row[2]), 4) if pd.notna(row[2]) else "", pct_fmt)

chart = workbook.add_chart({"type": "line"})
for i, (label, color) in enumerate([("Medicare %", "#2E75B6"), ("Medicaid %", "#ED7D31")]):
    chart.add_series({
        "name":       label,
        "categories": [ws.name, 2, 0, 2 + len(pm) - 1, 0],
        "values":     [ws.name, 2, i + 1, 2 + len(pm) - 1, i + 1],
        "line":       {"width": 2.5, "color": color},
        "marker":     {"type": "circle", "size": 5},
    })
chart.set_title({"name": "Payer Mix Trends — Medicare & Medicaid % of Patient Days"})
chart.set_x_axis({"name": "Year"})
chart.set_y_axis({"name": "% of Patient Days", "num_format": "0%"})
chart.set_size({"width": 680, "height": 400})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(pm) + 2, 0, chart)
autofit(ws, pm)


# ══════════════════════════════════════════════════════════════════════════════
# 7. CAH vs STH Financial Comparison
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 7: CAH vs STH")
ws = workbook.add_worksheet("7. CAH vs STH")
ws.set_zoom(90)

cah_sth = (
    df[df["FType"].isin(["CAH", "STH"])]
    .groupby(["Year", "FType"])
    .agg(
        Op_Margin=("Operating_Margin_Pct", "median"),
        Cost_per_Discharge=("Cost_per_Discharge", "median"),
        Medicare_Pct=("Medicare_Pct_Days", "median"),
    ).reset_index()
)
cah_pivot = cah_sth.pivot(index="Year", columns="FType", values="Op_Margin").reset_index()
cah_pivot.columns = ["Year", "CAH", "STH"]

write_header(ws, hdr_fmt, "CAH vs STH — Median Operating Margin % (2011–2024)", cols=3)
ws.write_row(1, 0, ["Year", "CAH", "STH"], sub_fmt)
for r, row in enumerate(cah_pivot.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, round(float(row[1]), 4) if pd.notna(row[1]) else "", pct_fmt)
    ws.write(r, 2, round(float(row[2]), 4) if pd.notna(row[2]) else "", pct_fmt)

chart = workbook.add_chart({"type": "line"})
for i, (label, color) in enumerate([("CAH", "#ED7D31"), ("STH", "#2E75B6")]):
    chart.add_series({
        "name":       label,
        "categories": [ws.name, 2, 0, 2 + len(cah_pivot) - 1, 0],
        "values":     [ws.name, 2, i + 1, 2 + len(cah_pivot) - 1, i + 1],
        "line":       {"width": 2.5, "color": color},
        "marker":     {"type": "circle", "size": 5},
    })
chart.set_title({"name": "CAH vs Short-Term Hospital — Median Operating Margin %"})
chart.set_x_axis({"name": "Year"})
chart.set_y_axis({"name": "Operating Margin %", "num_format": "0%"})
chart.set_size({"width": 680, "height": 400})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(cah_pivot) + 2, 0, chart)
autofit(ws, cah_pivot)


# ══════════════════════════════════════════════════════════════════════════════
# 8. 340B Participation by State
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 8: 340B by State")
ws = workbook.add_worksheet("8. 340B by State")
ws.set_zoom(90)

sst = pd.read_csv("data/output/SST_v3.csv", low_memory=False)
latest_yr = sst["Year"].max()
sst_latest = sst[sst["Year"] == latest_yr]

state_340b = (
    sst_latest.groupby("State")
    .agg(
        Total=("CCN", "nunique"),
        Enrolled_340B=("Is_340B_Enrolled", lambda x: (x == 1).sum()),
    )
    .assign(Participation_Rate=lambda d: d["Enrolled_340B"] / d["Total"])
    .sort_values("Participation_Rate", ascending=False)
    .reset_index()
)
top20 = state_340b.head(20)

write_header(ws, hdr_fmt, f"340B Participation Rate by State — Top 20 ({latest_yr})", cols=4)
ws.write_row(1, 0, ["State", "Total Hospitals", "340B Enrolled", "Participation Rate"], sub_fmt)
for r, row in enumerate(top20.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, int(row[1]), int_fmt)
    ws.write(r, 2, int(row[2]), int_fmt)
    ws.write(r, 3, round(float(row[3]), 4) if pd.notna(row[3]) else "", pct_fmt)

chart = workbook.add_chart({"type": "bar"})
chart.add_series({
    "name":       "Participation Rate",
    "categories": [ws.name, 2, 0, 2 + len(top20) - 1, 0],
    "values":     [ws.name, 2, 3, 2 + len(top20) - 1, 3],
    "fill":       {"color": "#2E75B6"},
})
chart.set_title({"name": f"340B Participation Rate — Top 20 States ({latest_yr})"})
chart.set_x_axis({"num_format": "0%"})
chart.set_y_axis({"reverse": True})
chart.set_size({"width": 560, "height": 480})
chart.set_legend({"none": True})
ws.insert_chart(2 + len(top20) + 2, 0, chart)
autofit(ws, top20)


# ══════════════════════════════════════════════════════════════════════════════
# 9. COVID PHE Funding Impact
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 9: COVID PHE Funding")
ws = workbook.add_worksheet("9. COVID PHE Funding")
ws.set_zoom(90)

df["COVID_PHE_Funding_num"] = pd.to_numeric(df["COVID_PHE_Funding"], errors="coerce")
covid = (
    df[df["Year"].between(2019, 2023)]
    .groupby("Year")
    .agg(
        Total_COVID_Funding=("COVID_PHE_Funding_num", "sum"),
        Median_Op_Margin=("Operating_Margin_Pct", "median"),
        Hospitals_with_Funding=("COVID_PHE_Funding_num", lambda x: (x > 0).sum()),
    ).reset_index()
)

write_header(ws, hdr_fmt, "COVID PHE Funding & Operating Margin Impact (2019–2023)", cols=4)
ws.write_row(1, 0, ["Year", "Total COVID Funding ($)", "Hospitals Receiving Funding", "Median Op Margin"], sub_fmt)
for r, row in enumerate(covid.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    ws.write(r, 1, round(float(row[1]), 0) if pd.notna(row[1]) else "", int_fmt)
    ws.write(r, 2, int(row[3]) if pd.notna(row[3]) else "", int_fmt)
    ws.write(r, 3, round(float(row[2]), 4) if pd.notna(row[2]) else "", pct_fmt)

chart = workbook.add_chart({"type": "column"})
chart.add_series({
    "name":       "Total COVID Funding ($)",
    "categories": [ws.name, 2, 0, 2 + len(covid) - 1, 0],
    "values":     [ws.name, 2, 1, 2 + len(covid) - 1, 1],
    "fill":       {"color": "#ED7D31"},
    "y2_axis":    False,
})
chart2 = workbook.add_chart({"type": "line"})
chart2.add_series({
    "name":       "Median Op Margin",
    "categories": [ws.name, 2, 0, 2 + len(covid) - 1, 0],
    "values":     [ws.name, 2, 3, 2 + len(covid) - 1, 3],
    "line":       {"width": 2.5, "color": "#2E75B6"},
    "y2_axis":    True,
})
chart.combine(chart2)
chart.set_title({"name": "COVID PHE Funding vs Operating Margin (2019–2023)"})
chart.set_x_axis({"name": "Year"})
chart.set_y_axis({"name": "Total COVID Funding ($)", "num_format": "$#,##0"})
chart.set_y2_axis({"name": "Median Op Margin", "num_format": "0%"})
chart.set_size({"width": 680, "height": 400})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(covid) + 2, 0, chart)
autofit(ws, covid)


# ══════════════════════════════════════════════════════════════════════════════
# 10. Cost per Discharge Trend by Facility Type
# ══════════════════════════════════════════════════════════════════════════════
print("  Chart 10: Cost per Discharge")
ws = workbook.add_worksheet("10. Cost per Discharge")
ws.set_zoom(90)

cpd = (
    df[df["FType"].isin(["STH", "CAH", "LTCH"])]
    .groupby(["Year", "FType"])["Cost_per_Discharge"]
    .median().reset_index()
    .pivot(index="Year", columns="FType", values="Cost_per_Discharge")
    .reset_index()
)
ftypes_cpd = [c for c in cpd.columns if c != "Year"]

write_header(ws, hdr_fmt, "Median Cost per Discharge by Facility Type (2011–2024)", cols=len(cpd.columns))
ws.write_row(1, 0, cpd.columns.tolist(), sub_fmt)
for r, row in enumerate(cpd.itertuples(index=False), start=2):
    ws.write(r, 0, row[0], cell_fmt)
    for c, v in enumerate(row[1:], start=1):
        ws.write(r, c, round(float(v), 0) if pd.notna(v) else "", int_fmt)

chart = workbook.add_chart({"type": "line"})
colors_cpd = {"STH": "#2E75B6", "CAH": "#ED7D31", "LTCH": "#A9D18E"}
for ft in ftypes_cpd:
    col_idx = cpd.columns.tolist().index(ft)
    chart.add_series({
        "name":       ft,
        "categories": [ws.name, 2, 0, 2 + len(cpd) - 1, 0],
        "values":     [ws.name, 2, col_idx, 2 + len(cpd) - 1, col_idx],
        "line":       {"width": 2.5, "color": colors_cpd.get(ft, "#767171")},
        "marker":     {"type": "circle", "size": 5},
    })
chart.set_title({"name": "Median Cost per Discharge by Facility Type"})
chart.set_x_axis({"name": "Year"})
chart.set_y_axis({"name": "Cost per Discharge ($)", "num_format": "$#,##0"})
chart.set_size({"width": 680, "height": 400})
chart.set_legend({"position": "bottom"})
ws.insert_chart(2 + len(cpd) + 2, 0, chart)
autofit(ws, cpd)


# ── save ──────────────────────────────────────────────────────────────────────
workbook.close()
print(f"\nDone → {DEST}")
print(f"       ({DEST.stat().st_size / 1e6:.1f} MB)")
