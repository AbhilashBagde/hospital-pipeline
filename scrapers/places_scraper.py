"""
scrapers/places_scraper.py
==========================
Downloads CDC PLACES county-level demographic data from data.cdc.gov via
the Socrata SODA API.

Discovery flow:
  1. Query the Socrata catalog API to find the most recent
     "PLACES: Local Data for Better Health, County Data" dataset.
  2. Download only the 6 measures used by the demographics pipeline step,
     using a SoQL $where filter to keep the download small (~18 K rows vs ~120 K+).

Usage:
    from scrapers.places_scraper import download_places_data
    path = download_places_data(output_dir=Path("data/raw"))
"""

import logging
import re
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

SOCRATA_CATALOG_URL = "https://api.us.socrata.com/api/catalog/v1"
CDC_DOMAIN          = "data.cdc.gov"
REQUEST_TIMEOUT     = 120  # seconds

# Exactly the six measures the demographic pipeline step uses.
DEMOGRAPHICS = [
    "Cancer (non-skin) or melanoma among adults",
    "Obesity among adults",
    "Food insecurity in the past 12 months among adults",
    "Current lack of health insurance among adults aged 18-64 years",
    "Any disability among adults",
    "Fair or poor self-rated health status among adults",
]


def _find_places_county_dataset_id() -> tuple[str, str]:
    """
    Return (dataset_id, dataset_name) for the most recent PLACES county dataset
    on data.cdc.gov.  Raises RuntimeError if nothing is found.
    """
    resp = requests.get(
        SOCRATA_CATALOG_URL,
        params={
            "q":       "PLACES Local Data Better Health County",
            "domains": CDC_DOMAIN,
            "limit":   20,
        },
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()

    results = resp.json().get("results", [])
    candidates: list[tuple[int, str, str]] = []  # (release_year, id, name)

    for r in results:
        resource = r.get("resource", {})
        name = resource.get("name", "")
        # Must mention County and PLACES; ignore sub-geographies (ZCTA, Census Tract)
        if "PLACES" not in name or "County" not in name:
            continue
        if any(x in name for x in ("ZCTA", "Census Tract", "Place")):
            continue
        dataset_id = resource.get("id", "")
        if not dataset_id:
            continue
        # Extract 4-digit release year from the name, e.g. "… 2025 release"
        match = re.search(r"\b(20\d{2})\b", name)
        year = int(match.group(1)) if match else 0
        candidates.append((year, dataset_id, name))

    if not candidates:
        raise RuntimeError(
            "Could not find a PLACES county dataset via Socrata catalog API.\n"
            "Check: https://data.cdc.gov/browse?category=500+Cities+%26+Places"
        )

    candidates.sort(reverse=True)
    _, dataset_id, name = candidates[0]
    logger.info(f"  Found PLACES county dataset: {name!r}  (id={dataset_id})")
    return dataset_id, name


def download_places_data(output_dir: Path) -> Path:
    """
    Download CDC PLACES county data filtered to the 6 demographic measures.
    Caches the result in output_dir; returns the path to the CSV file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_id, dataset_name = _find_places_county_dataset_id()
    out_path = output_dir / f"PLACES_county_{dataset_id}.csv"

    if out_path.exists():
        logger.info(f"  PLACES cache hit → {out_path}")
        return out_path

    # Build a SoQL IN() filter.  Single quotes must be escaped as '' in SoQL.
    def soql_str(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    where_clause = "measure in(" + ",".join(soql_str(m) for m in DEMOGRAPHICS) + ")"

    url = f"https://{CDC_DOMAIN}/resource/{dataset_id}.csv"
    params: dict = {
        "$limit":  100_000,
        "$offset": 0,
        "$where":  where_clause,
        "$select": "year,stateabbr,locationname,measure,data_value",
    }

    logger.info(f"  Downloading PLACES county data (6 measures) from {url} …")
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    out_path.write_bytes(resp.content)
    row_count = resp.text.count("\n") - 1
    logger.info(f"  Saved {row_count:,} rows → {out_path}")
    return out_path
