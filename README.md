# Hospital Data Pipeline

Automated quarterly pipeline that downloads, merges, and publishes U.S. hospital financial
data for the five-state region (AR, LA, NM, OK, TX). The primary output is **`SST_v6.csv`** —
a panel dataset covering Critical Access Hospitals (CAH), REH converters, payer mix, 340B
enrollment, county-level community health, and road distance to the nearest hospital (2020–present).
The pipeline also generates **`CAH_REH_Baselines.json`**, which powers the
[CAH → REH Conversion Financial Calculator](CAH_REH_Calculator_local.html).

---

## Outputs

| File | Description | Scope |
|------|-------------|-------|
| `SST_v6.csv` | **Primary output.** Panel dataset with demographics + road distance | AR, LA, NM, OK, TX |
| `SST_v4.csv` | All-states panel with county demographics (no road distance) | All 50 states |
| `SST_v3.csv` | All-states panel, 340B matched, no demographics | All 50 states |
| `CAH_REH_Baselines.json` | Per-hospital avg + most-recent baselines for the calculator | AR, LA, NM, OK, TX |
| `Final_Hospital_Dataset.csv` | Pre-340B intermediate (CMS + NASHP merged) | All 50 states |

### Quick load

```python
import pandas as pd

# Primary 5-state dataset
url = "https://github.com/AbhilashBagde/hospital-pipeline/releases/latest/download/SST_v6.csv"
df = pd.read_csv(url)

# Baselines JSON (used by the calculator)
import json, urllib.request
url = "https://raw.githubusercontent.com/AbhilashBagde/hospital-pipeline/main/data/output/CAH_REH_Baselines.json"
baselines = json.loads(urllib.request.urlopen(url).read())
```

---

## How It Works

```
CMS data.cms.gov ──────────────────────┐
  Cost Reports 2020–present             │
  (Socrata API)                          │  build_dataset.py
NASHP nashp.org ───────────────────────┼──────────────────► Final_Hospital_Dataset.csv
  HCT Excel (Playwright headless)        │                              │
UNC Sheps Center ──────────────────────┤                              │
  REH list (Playwright headless)         │                              │ 340B matching
CMS Provider of Services ──────────────┘                              ▼
  REH/CAH CCN resolution (Socrata API)                           SST_v3.csv
                                                                       │
HRSA 340bopais.hrsa.gov ──────────────────────────────────────────────┘
  Covered Entity Daily Export (OPAIS API)                              │ CDC PLACES merge
                                                                        ▼
CDC PLACES (Socrata API) ──────────────────────────────────────── SST_v4.csv
  County-level community health metrics                                 │
                                                                        │ 5-state filter
data/manual/updated_df_road_dist.csv ──────────────────────────────────┤ + road distance
  Nearest hospital road distance (pre-computed, LFS)                   ▼
                                                                   SST_v6.csv
                                                                        │
                                                         build_baselines_json.py
                                                                        │
                                                                        ▼
                                                          CAH_REH_Baselines.json
                                                          (pushed to capstone_project_1)
```

### Pipeline Steps

| Step | Script | Output |
|------|--------|--------|
| 1 | `scrapers/cms_scraper.py` | CMS cost report CSVs (2020–present) |
| 2 | `scrapers/hrsa_340b_scraper.py` | 340B covered entity Excel |
| 3 | `scrapers/nashp_scraper.py` | NASHP HCT Excel |
| 3b | `scrapers/reh_scraper.py` | REH conversion info CSV |
| 3c | `scrapers/places_scraper.py` | CDC PLACES county health CSV |
| 4 | `build_dataset.py` | `Final_Hospital_Dataset.csv` |
| 5 | `pipeline.py` (340B matching) | `SST_v3.csv` |
| 6 | `pipeline.py` (demographics merge) | `SST_v4.csv` |
| 7 | `pipeline.py` (5-state + road dist) | `SST_v6.csv` |
| 8 | `build_baselines_json.py` | `CAH_REH_Baselines.json` |
| 9 | `pipeline.py` (archive + publish) | GitHub Release + capstone_project_1 sync |

---

## CAH/REH Calculator

`CAH_REH_Calculator_local.html` is a self-contained React app (no build step) for modeling
the financial impact of CAH → REH conversion. It fetches baselines from:

```
https://raw.githubusercontent.com/AbhilashBagde/hospital-pipeline/main/data/output/CAH_REH_Baselines.json
```

Open the HTML file directly in any browser. If the fetch fails (e.g. offline), use the
**Upload CSV** button to load `SST_v6.csv` as a fallback — the calculator will derive
the same baselines from the raw data client-side.

### Baselines JSON structure

```json
{
  "metadata": { "states": ["AR","LA","NM","OK","TX"], "total_hospitals_avg": 215, ... },
  "baseline_average":     [ /* one record per hospital — average across all CAH years */ ],
  "baseline_most_recent": [ /* one record per hospital — most recent CAH year only   */ ]
}
```

Each record includes: financial metrics, solvency score/status/flags, OP/IP revenue split,
SDOH community health percentiles, and road distance to the nearest hospital.

---

## Setup

### Prerequisites
- Python 3.11+
- Git with Git LFS (`brew install git-lfs && git lfs install`)
- GitHub account

### Step 1 — Clone and install

```bash
git clone https://github.com/AbhilashBagde/hospital-pipeline.git
cd hospital-pipeline
pip install -r requirements.txt

# Playwright browsers (needed for NASHP + REH scraping)
playwright install chromium
playwright install-deps chromium   # Linux only
```

> **Git LFS required.** All CSV and Excel files in this repo are stored via Git LFS.
> Run `git lfs pull` after cloning to download the actual files.

### Step 2 — Add manual data file

`data/manual/updated_df_road_dist.csv` contains pre-computed road distances (miles) from
each hospital to the nearest alternative hospital. This file is committed to LFS and is
pulled automatically on clone. It covers the 5-state region (AR, LA, NM, OK, TX).

To update road distances, replace this file and commit via LFS.

### Step 3 — Test locally

```bash
# Dry run (no downloads, no files written)
python pipeline.py --dry-run

# Full run
python pipeline.py

# Skip downloads (re-use cached data/raw/ files)
python pipeline.py --skip-download

# Limit to specific CMS years
python pipeline.py --years 2022 2023
```

### Step 4 — Add GitHub Secrets

**Settings → Secrets and variables → Actions → New repository secret**

| Secret | Required | Purpose |
|--------|----------|---------|
| `CAPSTONE_REPO_TOKEN` | Yes | PAT with `contents: write` on `AbhilashBagde/capstone_project_1` — used to push `SST_v6.csv` and `CAH_REH_Baselines.json` there after each run |
| `OPENAI_API_KEY` | Optional | Enables embedding-based 340B name matching (more accurate than TF-IDF fallback) |
| `AWS_ACCESS_KEY_ID` | Only if using S3 | S3 upload credentials |
| `AWS_SECRET_ACCESS_KEY` | Only if using S3 | S3 upload credentials |

### Step 5 — (Optional) Switch to S3 storage

**Settings → Secrets and variables → Actions → Variables**

| Variable | Value |
|----------|-------|
| `USE_S3` | `true` |
| `S3_BUCKET` | `your-bucket-name` |

---

## Schedule

| Trigger | When |
|---------|------|
| Automatic | 08:00 UTC on Jan 1, Apr 1, Jul 1, Oct 1 |
| Manual | GitHub Actions → Run workflow |
| On push | Changes to `pipeline.py`, `build_dataset.py`, `scrapers/**`, or the workflow YAML |

After each successful run, the workflow automatically:
1. Creates a GitHub Release with `SST_v6.csv`, `SST_v4.csv`, `SST_v3.csv`, and `CAH_REH_Baselines.json`
2. Pushes `SST_v6.csv` + `CAH_REH_Baselines.json` to `AbhilashBagde/capstone_project_1` (single git clone, no Contents API)

---

## Project Structure

```
hospital-pipeline/
├── .github/
│   └── workflows/
│       └── quarterly_update.yml        ← Quarterly schedule, CI validation, capstone sync
├── scrapers/
│   ├── __init__.py
│   ├── cms_scraper.py                  ← CMS cost reports (Socrata API)
│   ├── hrsa_340b_scraper.py            ← 340B entities (HRSA OPAIS API)
│   ├── nashp_scraper.py                ← NASHP Excel (Playwright headless)
│   ├── reh_scraper.py                  ← REH info (Playwright + Socrata)
│   └── places_scraper.py              ← CDC PLACES county health (Socrata API)
├── data/
│   ├── manual/
│   │   └── updated_df_road_dist.csv   ← Road distance to nearest hospital (LFS)
│   ├── raw/                           ← Downloaded each run (gitignored)
│   ├── output/                        ← Pipeline outputs (LFS)
│   │   ├── SST_v3.csv
│   │   ├── SST_v4.csv
│   │   ├── SST_v6.csv                 ← Primary output (5-state + road dist)
│   │   └── CAH_REH_Baselines.json     ← Calculator data
│   └── archive/                       ← Timestamped copies (LFS)
├── build_dataset.py                   ← CMS + NASHP merge (Steps 1–4)
├── build_baselines_json.py            ← Per-hospital baselines from SST_v6 (Step 8)
├── pipeline.py                        ← Orchestrator (Steps 1–9)
├── CAH_REH_Calculator_local.html      ← CAH→REH conversion calculator (open in browser)
├── requirements.txt
├── .gitattributes                     ← LFS tracking rules (*.csv, *.xlsx)
└── README.md
```

---

## Data Sources

| Source | URL | Frequency | Method |
|--------|-----|-----------|--------|
| CMS Hospital Cost Reports | data.cms.gov | Annual | Socrata API |
| NASHP Hospital Cost Tool | nashp.org/hospital-cost-tool | Annual (Dec) | Playwright |
| HRSA 340B Covered Entities | 340bopais.hrsa.gov/Reports | Daily | OPAIS JSON API |
| UNC Sheps Center REH list | shepscenter.unc.edu | As hospitals convert | Playwright |
| CMS Provider of Services | data.cms.gov | Quarterly | Socrata API |
| CDC PLACES County Health | data.cdc.gov | Annual | Socrata API |
| Road distance (manual) | `data/manual/updated_df_road_dist.csv` | As needed | Pre-computed, LFS |

---

## Maintenance Guide

### If CMS dataset IDs change
```bash
# 1. Go to data.cms.gov → search "hospital provider cost report"
# 2. Copy the Socrata dataset ID from the new dataset's URL
# 3. Update KNOWN_DATASET_IDS in scrapers/cms_scraper.py
```

### If NASHP download button changes
```python
# In scrapers/nashp_scraper.py, update _find_download_button()
# Add the new selector to the strategies list
```

Debug locally with a headful browser:
```bash
python3 -c "
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch(headless=False)
    page = b.new_page()
    page.goto('https://nashp.org/hospital-cost-tool/')
    input('Inspect the page, then press Enter...')
    b.close()
"
```

### If Sheps Center REH table changes
```python
# In scrapers/reh_scraper.py, update _infer_sheps_columns()
# Add new column name → standard name mappings
```

### If 340B OPAIS endpoint changes
```python
# In scrapers/hrsa_340b_scraper.py, update OPAIS_JSON_URL or OPAIS_XLSX_URL
# Current URLs: https://340bopais.hrsa.gov/Reports
```

### To update road distance data
Replace `data/manual/updated_df_road_dist.csv` with a new version containing at minimum
`CCN`, `Year`, and `nearest_building_road_dist_miles` columns, then commit via LFS:
```bash
git add data/manual/updated_df_road_dist.csv
git commit -m "chore: update road distance data"
git push
```
