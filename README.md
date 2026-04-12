# Hospital Data Pipeline

Automated quarterly pipeline that downloads, merges, and publishes U.S. hospital financial
data, producing **`SST_v3.csv`** — a panel dataset of ~28,000 hospital-years covering
Critical Access Hospitals, REH converters, payer mix, and 340B enrollment status (2020–present).

## Output

`SST_v3.csv` — ~28,000 rows × 111 columns. One row per hospital per year.

```python
import pandas as pd
url = "https://github.com/YOUR_ORG/hospital-pipeline/releases/latest/download/SST_v3.csv"
df = pd.read_csv(url)
```

---

## How It Works

```
CMS data.cms.gov ──────────────────────┐
  Cost Reports 2020–present             │   build_dataset.py
  (Socrata API)                          ├──────────────────► Final_Hospital_Dataset.csv
NASHP nashp.org ───────────────────────┤                              │
  HCT Excel (Playwright headless)        │                              │
UNC Sheps Center shepscenter.unc.edu ──┤  reh_scraper.py              │
  REH list table (Playwright headless)   │                              │
CMS Provider of Services ──────────────┘                              │
  REH/CAH CCN resolution (Socrata API)                                 ▼
                                                    pipeline.py (340B matching)
HRSA 340bopais.hrsa.gov ──────────────────────────────────────► SST_v3.csv
  Covered Entity Daily Export (OPAIS API)                               │
                                                                         ▼
                                                              GitHub Release / S3
```

**Four scrapers, all automated — no manual files needed:**

| Scraper | Source | Method |
|---------|--------|--------|
| `cms_scraper.py` | CMS Hospital Cost Reports | Socrata API |
| `nashp_scraper.py` | NASHP Hospital Cost Tool | Playwright headless (JS download button) |
| `reh_scraper.py` | UNC Sheps Center REH list + CMS POS | Playwright + Socrata API |
| `hrsa_340b_scraper.py` | HRSA 340B Covered Entities | OPAIS JSON/Excel API |

---

## Setup from Scratch

### Prerequisites
- Python 3.11+
- Git
- GitHub account

### Step 1 — Create a GitHub repo

```bash
# On github.com: click "New repository" → name it "hospital-pipeline" → Create

# Or via GitHub CLI:
gh repo create hospital-pipeline --public --clone
cd hospital-pipeline
```

### Step 2 — Add the pipeline files

```bash
# Copy all files from this zip/folder into the repo root
git add .
git commit -m "feat: initial pipeline setup"
```

### Step 3 — Install locally and test

```bash
pip install -r requirements.txt

# Install Playwright's Chromium browser (needed for NASHP + REH scraping)
playwright install chromium
playwright install-deps chromium   # Linux only — installs system libs

# Test a single scraper
python -m scrapers.nashp_scraper
python -m scrapers.reh_scraper

# Full dry run (no files downloaded)
python pipeline.py --dry-run

# Real run (downloads everything)
python pipeline.py
```

### Step 4 — Add GitHub Secrets

Go to your repo on GitHub:
**Settings → Secrets and variables → Actions → New repository secret**

| Secret | Required | Purpose |
|--------|----------|---------|
| `OPENAI_API_KEY` | Optional | Enables embedding-based 340B matching (more accurate). Without it, TF-IDF is used. |
| `AWS_ACCESS_KEY_ID` | Only if using S3 | S3 upload credentials |
| `AWS_SECRET_ACCESS_KEY` | Only if using S3 | S3 upload credentials |

### Step 5 — (Optional) Switch to S3 storage

If you prefer S3 over GitHub Releases, add these **Variables** (not secrets):

**Settings → Secrets and variables → Actions → Variables**

| Variable | Value |
|----------|-------|
| `USE_S3` | `true` |
| `S3_BUCKET` | `your-bucket-name` |

### Step 6 — Push and trigger first run

```bash
git push -u origin main
```

Then go to **Actions → Quarterly Hospital Data Pipeline → Run workflow** to trigger manually.

The pipeline will now also run automatically every quarter (Jan 1, Apr 1, Jul 1, Oct 1 at 08:00 UTC).

---

## Schedule

| Trigger | When |
|---------|------|
| Automatic | 08:00 UTC on Jan 1, Apr 1, Jul 1, Oct 1 |
| Manual | GitHub Actions → Run workflow |
| On push | Changes to `pipeline.py`, `build_dataset.py`, or `scrapers/**` |

---

## Maintenance Guide

### If CMS dataset IDs change
CMS occasionally publishes new cost report datasets with new Socrata IDs.

```bash
# 1. Go to: https://data.cms.gov → search "hospital provider cost report"
# 2. Open the new dataset, copy the ID from its URL
# 3. Update KNOWN_DATASET_IDS in scrapers/cms_scraper.py
```

### If NASHP download button changes
The NASHP Hospital Cost Tool is a WordPress site — its button label or URL can change.

```python
# In scrapers/nashp_scraper.py, update _find_download_button():
# Add the new selector to the `strategies` list at the top
```

You can debug locally:
```bash
python3 -c "
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch(headless=False)  # headful for debugging
    page = b.new_page()
    page.goto('https://nashp.org/hospital-cost-tool/')
    input('Inspect the page, then press Enter...')
    b.close()
"
```

### If Sheps Center REH table changes
The UNC Sheps Center may rename columns or restructure the REH table.

```python
# In scrapers/reh_scraper.py, update _infer_sheps_columns()
# Add new column name → standard name mappings
```

### If 340B OPAIS endpoint changes
```python
# In scrapers/hrsa_340b_scraper.py, update OPAIS_JSON_URL or OPAIS_XLSX_URL
# Check current URLs at: https://340bopais.hrsa.gov/Reports
```

---

## Project Structure

```
hospital-pipeline/
├── .github/
│   └── workflows/
│       └── quarterly_update.yml   ← Quarterly schedule + manual trigger
├── scrapers/
│   ├── __init__.py
│   ├── cms_scraper.py             ← CMS cost reports (Socrata API)
│   ├── hrsa_340b_scraper.py       ← 340B entities (HRSA OPAIS API)
│   ├── nashp_scraper.py           ← NASHP Excel (Playwright headless)
│   └── reh_scraper.py             ← REH info (Playwright + Socrata)
├── data/
│   ├── manual/                    ← Fallback files (committed to git)
│   ├── raw/                       ← gitignored; re-downloaded each run
│   ├── output/                    ← gitignored; published via Release/S3
│   └── archive/                   ← gitignored; timestamped CSVs
├── build_dataset.py               ← CMS + NASHP merge logic
├── pipeline.py                    ← Orchestrator (steps 1–6)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Data Sources

| Source | URL | Update frequency | Scrape method |
|--------|-----|-----------------|---------------|
| CMS Hospital Cost Reports | data.cms.gov | Annual | Socrata API |
| NASHP Hospital Cost Tool | nashp.org/hospital-cost-tool | Annual (Dec) | Playwright |
| HRSA 340B Covered Entities | 340bopais.hrsa.gov/Reports | Daily | OPAIS JSON API |
| UNC Sheps Center REH list | shepscenter.unc.edu | As hospitals convert | Playwright |
| CMS Provider of Services | data.cms.gov | Quarterly | Socrata API |
# hospital-pipeline
