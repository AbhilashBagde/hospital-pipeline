"""
scrapers/cms_scraper.py
=======================
Downloads Hospital Provider Cost Report CSVs from data.cms.gov.

CMS migrated away from the Socrata SODA API (old per-year dataset IDs like
fv3v-7x8v are 410 Gone). They now serve direct CSV files discoverable via
a Drupal JSON:API endpoint that lists all year versions with their download URLs.

Discovery endpoint (no auth required):
  GET https://data.cms.gov/jsonapi/node/dataset
      ?include=field_ref_primary_data_file,field_ref_primary_data_file.field_media_file
      &filter[field_dataset_type.name]=Hospital Provider Cost Report
      &sort=-field_dataset_version

Each result includes a relative path like:
  /sites/default/files/2026-01/<uuid>/CostReport_2023_Final.csv

which resolves to:
  https://data.cms.gov/sites/default/files/2026-01/<uuid>/CostReport_2023_Final.csv

Usage:
    from scrapers.cms_scraper import download_cms_cost_reports
    paths = download_cms_cost_reports(output_dir="data/raw", years=[2021, 2022, 2023])
"""

import logging
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

CMS_BASE        = "https://data.cms.gov"
JSONAPI_URL     = (
    f"{CMS_BASE}/jsonapi/node/dataset"
    "?include=field_ref_primary_data_file"
    "%2Cfield_ref_primary_data_file.field_media_file"
    "&fields%5Bnode--dataset%5D=field_dataset_version%2Cfield_ref_primary_data_file"
    "&filter%5Bfield_dataset_type.name%5D=Hospital+Provider+Cost+Report"
    "&sort=-field_dataset_version"
)

REQUEST_TIMEOUT = 300   # seconds — the CSVs are ~30 MB each
RETRY_ATTEMPTS  = 3
RETRY_BACKOFF   = 10    # seconds


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


def _discover_year_urls() -> dict[int, str]:
    """
    Query the CMS Drupal JSON:API to build a {year: absolute_csv_url} mapping.
    Returns an empty dict on failure so the caller can decide how to handle it.
    """
    try:
        resp = _get(JSONAPI_URL, headers={"Accept": "application/vnd.api+json"})
        payload = resp.json()
    except Exception as exc:
        logger.error(f"  CMS JSON:API discovery failed: {exc}")
        return {}

    data     = payload.get("data", [])
    included = payload.get("included", [])

    # Build a lookup table: node/media/file id → included object
    inc_map: dict[str, dict] = {item["id"]: item for item in included}

    year_urls: dict[int, str] = {}

    for item in data:
        version = item.get("attributes", {}).get("field_dataset_version", "")
        try:
            year = int(version[:4])
        except (ValueError, TypeError):
            continue

        # Relationship: dataset → media node
        file_rel = (
            item.get("relationships", {})
                .get("field_ref_primary_data_file", {})
                .get("data", {})
        )
        if not isinstance(file_rel, dict):
            continue
        media = inc_map.get(file_rel.get("id", ""), {})

        # Relationship: media node → file entity
        file_data_rel = (
            media.get("relationships", {})
                 .get("field_media_file", {})
                 .get("data", {})
        )
        if not isinstance(file_data_rel, dict):
            continue
        file_node = inc_map.get(file_data_rel.get("id", ""), {})

        rel_url = (
            file_node.get("attributes", {})
                     .get("uri", {})
                     .get("url", "")
        )
        if not rel_url:
            continue

        year_urls[year] = CMS_BASE + rel_url

    if year_urls:
        logger.info(f"  Discovered {len(year_urls)} year files from CMS JSON:API: "
                    f"{sorted(year_urls.keys())}")
    else:
        logger.warning("  No year URLs found in CMS JSON:API response.")

    return year_urls


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
        Which fiscal years to download. Defaults to all available years.

    Returns
    -------
    dict mapping year → local CSV path for each successfully downloaded file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    year_urls = _discover_year_urls()
    if not year_urls:
        logger.error("  CMS discovery returned no file URLs. Cannot download.")
        return {}

    target_years = sorted(years) if years is not None else sorted(year_urls.keys())
    results: dict[int, Path] = {}

    for year in target_years:
        dest = output_dir / f"Hospital_Provider_Cost_Report_{year}.csv"

        if dest.exists():
            logger.info(f"  {year}: already exists at {dest}, skipping.")
            results[year] = dest
            continue

        if year not in year_urls:
            logger.warning(f"  {year}: no file URL found in CMS catalog. Skipping.")
            continue

        url = year_urls[year]
        logger.info(f"  {year}: downloading {url} …")

        try:
            resp = _get(url, stream=True)
            with dest.open("wb") as fh:
                bytes_written = 0
                for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                    fh.write(chunk)
                    bytes_written += len(chunk)
            logger.info(f"  {year}: ✓ {bytes_written / 1e6:.1f} MB → {dest}")
            results[year] = dest
        except Exception as exc:
            logger.error(f"  {year}: download failed — {exc}")
            if dest.exists():
                dest.unlink()  # remove partial file

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    paths = download_cms_cost_reports(output_dir="data/raw")
    for yr, p in paths.items():
        print(f"  {yr}: {p}")
