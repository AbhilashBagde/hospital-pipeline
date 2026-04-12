"""
scrapers/hrsa_340b_scraper.py
=============================
Downloads the HRSA 340B Covered Entity daily export from OPAIS.

The OPAIS site (https://340bopais.hrsa.gov/Reports) is now a Blazor
server-side app. Direct HTTP to the old endpoints returns 404.
We use Playwright to click the "Covered Entity Daily Export (JSON)"
button, which triggers a ZIP download containing OPA_CE_DAILY_PUBLIC.JSON.

The JSON is then normalised and saved as an Excel file whose column
names match what the 340B matching step in pipeline.py expects:
    Entity Name, Entity Type, 340B ID, Street Address 1,
    Street City, Street State, Street Zip, Start Date, Termination Date

Usage:
    from scrapers.hrsa_340b_scraper import download_340b_entities
    path = download_340b_entities(output_dir="data/raw")
"""

import json
import logging
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

OPAIS_REPORTS_URL = "https://340bopais.hrsa.gov/Reports"
BUTTON_TEXT       = "Covered Entity Daily Export (JSON)"
PAGE_LOAD_WAIT_MS = 8_000
DOWNLOAD_TIMEOUT  = 180_000   # ms — ZIP is ~27 MB


def download_340b_entities(
    output_dir: str | Path = "data/raw",
) -> Path | None:
    """
    Download the HRSA 340B Covered Entity daily export via headless Chromium.

    Returns path to the .xlsx file, or None if all methods fail.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = date.today().strftime("%Y%m%d")
    dest_xlsx = output_dir / f"340B_CoveredEntity_Daily_{today}.xlsx"

    if dest_xlsx.exists():
        logger.info(f"  340B file already downloaded today: {dest_xlsx}")
        return dest_xlsx

    # ── 1. Playwright download ────────────────────────────────────────────────
    try:
        zip_path = _playwright_download(output_dir)
        if zip_path and zip_path.exists():
            result = _zip_to_excel(zip_path, dest_xlsx)
            zip_path.unlink(missing_ok=True)   # remove the raw ZIP
            if result:
                return dest_xlsx
    except ImportError:
        logger.warning(
            "  playwright not installed — run: "
            "pip install playwright && playwright install chromium"
        )
    except Exception as exc:
        logger.warning(f"  Playwright 340B download failed: {exc}")

    # ── 2. Fallback: most recent cached file ──────────────────────────────────
    existing = sorted(output_dir.glob("340B_CoveredEntity_Daily_*.xlsx"), reverse=True)
    if existing:
        logger.warning(
            f"  All download attempts failed. Using cached file: {existing[0]}"
        )
        return existing[0]

    logger.error(
        "  340B download failed and no cached file found.\n"
        "  Manual fallback:\n"
        "    1. Visit https://340bopais.hrsa.gov/Reports\n"
        "    2. Click 'Covered Entity Daily Export (JSON)'\n"
        "    3. Extract OPA_CE_DAILY_PUBLIC.JSON from the ZIP\n"
        "    4. Run:  python -c \"from scrapers.hrsa_340b_scraper import "
        "_json_file_to_excel; _json_file_to_excel(Path('OPA_CE_DAILY_PUBLIC.JSON'), "
        "Path('data/raw/340B_CoveredEntity_Daily_YYYYMMDD.xlsx'))\""
    )
    return None


def _playwright_download(output_dir: Path) -> Path | None:
    """Use headless Chromium to click the JSON export button."""
    from playwright.sync_api import sync_playwright

    logger.info("  Launching Chromium headless for 340B download…")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        logger.info(f"  Navigating to {OPAIS_REPORTS_URL}…")
        page.goto(OPAIS_REPORTS_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(PAGE_LOAD_WAIT_MS)

        btn = page.get_by_text(BUTTON_TEXT, exact=True)
        if not btn.is_visible():
            logger.warning(f"  Button '{BUTTON_TEXT}' not visible on page.")
            browser.close()
            return None

        logger.info(f"  Clicking '{BUTTON_TEXT}' and waiting for download…")
        with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as dl_info:
            btn.click()

        dl = dl_info.value
        suggested = dl.suggested_filename or f"340B_export_{date.today()}.zip"
        zip_path = output_dir / suggested
        dl.save_as(zip_path)

        size_mb = zip_path.stat().st_size / 1e6
        logger.info(f"  ✓ Downloaded {zip_path.name} ({size_mb:.1f} MB)")
        browser.close()
        return zip_path


def _zip_to_excel(zip_path: Path, dest_xlsx: Path) -> bool:
    """
    Extract OPA_CE_DAILY_PUBLIC.JSON from the ZIP, normalise to a flat
    DataFrame with legacy column names, and save as Excel.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            json_names = [n for n in zf.namelist() if n.upper().endswith(".JSON")]
            if not json_names:
                logger.warning("  No JSON file found inside the 340B ZIP.")
                return False
            with zf.open(json_names[0]) as f:
                raw = json.load(f)
    except Exception as exc:
        logger.warning(f"  Failed to read ZIP: {exc}")
        return False

    return _json_file_to_excel(raw, dest_xlsx)


def _json_file_to_excel(raw: dict | list, dest_xlsx: Path) -> bool:
    """
    Convert the parsed JSON (dict with 'coveredEntities' key, or bare list)
    into a flat DataFrame and save as Excel.
    """
    try:
        records = raw["coveredEntities"] if isinstance(raw, dict) else raw
        logger.info(f"  340B JSON: {len(records):,} covered entity records.")

        rows = []
        for rec in records:
            addr = rec.get("streetAddress") or {}
            rows.append({
                "Entity Name":      rec.get("name", ""),
                "Entity Type":      rec.get("entityType", ""),
                "340B ID":          rec.get("id340B", ""),
                "Street Address 1": addr.get("addressLine1", ""),
                "Street City":      addr.get("city", ""),
                "Street State":     addr.get("state", ""),
                "Street Zip":       addr.get("zip", ""),
                "Start Date":       rec.get("participatingStartDate", ""),
                "Termination Date": rec.get("certifiedDecertifiedDate", ""),
                "Medicaid Billing Number": (
                    rec.get("medicaidNumbers", [{}])[0].get("medicaidNumber", "")
                    if rec.get("medicaidNumbers") else ""
                ),
                "Participating":    rec.get("participating", ""),
            })

        df = pd.DataFrame(rows)
        with pd.ExcelWriter(dest_xlsx, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Covered Entities", index=False)

        logger.info(f"  ✓ 340B Excel saved: {dest_xlsx} ({len(df):,} rows)")
        return True

    except Exception as exc:
        logger.warning(f"  JSON→Excel conversion failed: {exc}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    path = download_340b_entities(output_dir="data/raw")
    print(f"340B file: {path}")
