"""
scrapers/cms_scraper.py
=======================
Downloads Hospital Provider Cost Report CSVs from data.cms.gov.

CMS publishes these via the Socrata Open Data API (SODA).
Dataset IDs are discovered dynamically via the catalog API and
cached in KNOWN_DATASET_IDS as a reliable fallback.

Usage:
    from scrapers.cms_scraper import download_cms_cost_reports
    paths = download_cms_cost_reports(output_dir="data/raw", years=[2021, 2022, 2023])
"""

import time
import logging
from pathlib import Path
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

# ── Fallback dataset IDs (verify at data.cms.gov if downloads fail) ──────────
# Navigate to: https://data.cms.gov/provider-compliance/cost-report/hospital-provider-cost-report
# Each year is a separate dataset; copy the ID from the dataset URL.
KNOWN_DATASET_IDS: dict[int, str] = {
    2020: "fv3v-7x8v",
    2021: "6vnz-fwrk",
    2022: "fjfh-y76p",
    2023: "ulyc-65am",  # Most recent; may need updating when 2024 releases
}

CMS_CATALOG_URL = (
    "https://data.cms.gov/data-api/v1/dataset"
    "?keyword=hospital+provider+cost+report&size=20"
)
SODA_BASE = "https://data.cms.gov/resource/{dataset_id}.csv"

# SODA row limit per request (max 50k; cost reports have ~6k rows so one page is fine)
SODA_LIMIT = 50_000
REQUEST_TIMEOUT = 120  # seconds
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 5  # seconds


def _get(url: str, stream: bool = False, **kwargs) -> requests.Response:
    """GET with retry + exponential backoff."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, stream=stream, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == RETRY_ATTEMPTS:
                raise
            wait = RETRY_BACKOFF * attempt
            logger.warning(f"  Attempt {attempt} failed ({exc}). Retrying in {wait}s…")
            time.sleep(wait)


def _discover_dataset_ids() -> dict[int, str]:
    """
    Query the CMS catalog API to find the latest dataset IDs for each year.
    Returns a dict like {2023: "ulyc-65am", ...}.
    Falls back silently to KNOWN_DATASET_IDS on failure.
    """
    try:
        resp = _get(CMS_CATALOG_URL)
        datasets = resp.json().get("data", [])
        discovered: dict[int, str] = {}
        for ds in datasets:
            title = ds.get("title", "").lower()
            if "hospital provider cost report" in title:
                # Extract year from title, e.g. "Hospital Provider Cost Report FY2023"
                for yr in range(2019, datetime.now().year + 1):
                    if str(yr) in title:
                        discovered[yr] = ds["identifier"]
                        break
        if discovered:
            logger.info(f"  Discovered dataset IDs from catalog: {discovered}")
            return {**KNOWN_DATASET_IDS, **discovered}   # discovered overrides known
    except Exception as exc:
        logger.warning(f"  Catalog discovery failed ({exc}). Using known dataset IDs.")
    return KNOWN_DATASET_IDS


def download_cms_cost_reports(
    output_dir: str | Path = "data/raw",
    years: list[int] | None = None,
) -> dict[int, Path]:
    """
    Download CMS Hospital Provider Cost Report CSVs for the given years.

    Parameters
    ----------
    output_dir : str or Path
        Destination folder. Created if it doesn't exist.
    years : list[int] or None
        Which fiscal years to download. Defaults to all known years.

    Returns
    -------
    dict mapping year → local CSV path for each successfully downloaded file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_ids = _discover_dataset_ids()

    if years is None:
        years = sorted(dataset_ids.keys())

    results: dict[int, Path] = {}

    for year in years:
        dest = output_dir / f"Hospital_Provider_Cost_Report_{year}.csv"

        # Skip if already downloaded this run (idempotent)
        if dest.exists():
            logger.info(f"  {year}: already exists at {dest}, skipping download.")
            results[year] = dest
            continue

        if year not in dataset_ids:
            logger.warning(f"  {year}: no dataset ID known. Skipping.")
            continue

        dataset_id = dataset_ids[year]
        url = SODA_BASE.format(dataset_id=dataset_id)
        params = {"$limit": SODA_LIMIT, "$offset": 0}

        logger.info(f"  {year}: downloading from {url} …")
        rows_written = 0

        with dest.open("w", encoding="utf-8") as fh:
            first_chunk = True
            offset = 0

            while True:
                params["$offset"] = offset
                resp = _get(url, params=params)
                chunk = resp.text

                if not chunk.strip():
                    break

                lines = chunk.splitlines()

                if first_chunk:
                    fh.write("\n".join(lines) + "\n")
                    rows_written += len(lines) - 1  # subtract header
                    first_chunk = False
                else:
                    # Skip header row on subsequent pages
                    data_lines = lines[1:]
                    if not data_lines:
                        break
                    fh.write("\n".join(data_lines) + "\n")
                    rows_written += len(data_lines)

                if len(lines) - 1 < SODA_LIMIT:
                    break  # Last page
                offset += SODA_LIMIT

        logger.info(f"  {year}: ✓ {rows_written:,} rows → {dest}")
        results[year] = dest

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    paths = download_cms_cost_reports(output_dir="data/raw")
    for yr, p in paths.items():
        print(f"  {yr}: {p}")
