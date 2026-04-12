"""
scrapers/hrsa_340b_scraper.py
=============================
Downloads the HRSA 340B Covered Entity daily export from OPAIS.

HRSA publishes two formats at https://340bopais.hrsa.gov/Reports:
  - Excel (.xlsx) — structured, multi-sheet, used by the SST_update notebook
  - JSON  — same data, machine-friendly, used here as primary method

Strategy:
  1. Try the JSON API endpoint → parse → write to .xlsx  (most reliable)
  2. Fall back to direct .xlsx download link if JSON fails

Usage:
    from scrapers.hrsa_340b_scraper import download_340b_entities
    path = download_340b_entities(output_dir="data/raw")
"""

import json
import logging
import time
from datetime import date
from pathlib import Path

import requests
import pandas as pd

logger = logging.getLogger(__name__)

# ── HRSA OPAIS endpoints ──────────────────────────────────────────────────────
# JSON export — returns all covered entities as a JSON array
OPAIS_JSON_URL = "https://340bopais.hrsa.gov/CoveredEntityExport/ExportCoveredEntities"

# Excel export — direct download (URL may require a browser-like User-Agent)
OPAIS_XLSX_URL = "https://340bopais.hrsa.gov/Reports/CoveredEntityDailyReport"

REQUEST_TIMEOUT = 180
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; hospital-pipeline/1.0; "
        "+https://github.com/your-org/hospital-pipeline)"
    ),
    "Accept": "application/json, */*",
}


def _get(url: str, **kwargs) -> requests.Response:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == RETRY_ATTEMPTS:
                raise
            wait = RETRY_BACKOFF * attempt
            logger.warning(f"  Attempt {attempt} failed ({exc}). Retrying in {wait}s…")
            time.sleep(wait)


def _download_via_json(dest: Path) -> bool:
    """
    Fetch the OPAIS JSON export and save as an Excel file with a
    'Covered Entities' sheet (matching what SST_update.ipynb expects).
    Returns True on success.
    """
    try:
        logger.info("  Attempting 340B JSON API download…")
        resp = _get(OPAIS_JSON_URL)
        data = resp.json()

        if isinstance(data, dict) and "coveredEntities" in data:
            records = data["coveredEntities"]
        elif isinstance(data, list):
            records = data
        else:
            logger.warning("  Unexpected JSON structure from OPAIS.")
            return False

        df = pd.json_normalize(records)
        logger.info(f"  JSON returned {len(df):,} covered entity records.")

        # Rename fields to match the column names SST_update.ipynb expects
        RENAME = {
            "entityName":    "Entity Name",
            "entityType":    "Entity Type",
            "id340B":        "340B ID",
            "streetAddress1": "Street Address 1",
            "streetCity":    "Street City",
            "streetState":   "Street State",
            "streetZip":     "Street Zip",
            "startDate":     "Start Date",
            "terminationDate": "Termination Date",
            "medicaidBillingNumber": "Medicaid Billing Number",
        }
        df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns})

        with pd.ExcelWriter(dest, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Covered Entities", index=False)

        logger.info(f"  ✓ 340B JSON → Excel saved to {dest}")
        return True

    except Exception as exc:
        logger.warning(f"  JSON download failed: {exc}")
        return False


def _download_via_xlsx(dest: Path) -> bool:
    """
    Direct .xlsx download from OPAIS Reports page.
    Returns True on success.
    """
    try:
        logger.info("  Attempting 340B direct Excel download…")
        resp = _get(OPAIS_XLSX_URL)

        content_type = resp.headers.get("Content-Type", "")
        if "excel" not in content_type and "spreadsheet" not in content_type and len(resp.content) < 1000:
            logger.warning("  Direct Excel download returned unexpected content.")
            return False

        dest.write_bytes(resp.content)
        logger.info(f"  ✓ 340B Excel saved to {dest}")
        return True

    except Exception as exc:
        logger.warning(f"  Excel download failed: {exc}")
        return False


def download_340b_entities(output_dir: str | Path = "data/raw") -> Path | None:
    """
    Download the HRSA 340B Covered Entity daily export.

    Returns the path to the downloaded .xlsx file, or None if all methods failed.
    In that case, the pipeline will fall back to any previously downloaded file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = date.today().strftime("%Y%m%d")
    dest = output_dir / f"340B_CoveredEntity_Daily_{today}.xlsx"

    # Skip if already downloaded today
    if dest.exists():
        logger.info(f"  340B file already downloaded today: {dest}")
        return dest

    # Try JSON first, then direct Excel
    if _download_via_json(dest):
        return dest
    if _download_via_xlsx(dest):
        return dest

    # Look for a previously downloaded file to use as fallback
    existing = sorted(output_dir.glob("340B_CoveredEntity_Daily_*.xlsx"), reverse=True)
    if existing:
        logger.warning(
            f"  All download attempts failed. Using most recent cached file: {existing[0]}"
        )
        return existing[0]

    logger.error(
        "  340B download failed and no cached file found.\n"
        "  Manual fallback: download from https://340bopais.hrsa.gov/Reports\n"
        "  and save to data/raw/340B_CoveredEntity_Daily_YYYYMMDD.xlsx"
    )
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    path = download_340b_entities(output_dir="data/raw")
    print(f"340B file: {path}")
