"""
scrapers/nashp_scraper.py
=========================
Downloads the NASHP Hospital Cost Tool Excel data using a headless Playwright
browser. NASHP serves the download via a JavaScript-rendered button click —
plain requests cannot get it.

Strategy:
  1. Launch Chromium headless via Playwright
  2. Navigate to https://nashp.org/hospital-cost-tool/
  3. Intercept the network download triggered by the "Download the data" button
  4. Save the file
"""

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

NASHP_URL     = "https://nashp.org/hospital-cost-tool/"
TIMEOUT_MS    = 60_000   # 60s page/navigation timeout
DOWNLOAD_WAIT = 90_000   # 90s to wait for the download to complete


def download_nashp_data(
    output_dir: str | Path = "data/raw",
    manual_dir: str | Path = "data/manual",
) -> Path | None:
    """
    Download the NASHP Hospital Cost Tool Excel file via headless Chromium.
    Falls back to any manually committed file if scraping fails.
    """
    output_dir = Path(output_dir)
    manual_dir = Path(manual_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Try Playwright headless browser
    try:
        path = _playwright_download(output_dir)
        if path:
            return path
    except ImportError:
        logger.warning(
            "  playwright not installed — run: pip install playwright && "
            "playwright install chromium"
        )
    except Exception as exc:
        logger.warning(f"  Playwright NASHP download failed: {exc}")

    # 2. Fall back to committed manual file
    manual_candidates = sorted(
        list(manual_dir.glob("NASHP*.xlsx")) +
        list(manual_dir.glob("nashp*.xlsx")) +
        list(manual_dir.glob("*HCT*.xlsx")),
        reverse=True,
    )
    if manual_candidates:
        logger.warning(f"  Using manually committed file: {manual_candidates[0]}")
        return manual_candidates[0]

    # 3. Check raw dir for a previous download
    raw_candidates = sorted(output_dir.glob("NASHP*.xlsx"), reverse=True)
    if raw_candidates:
        logger.warning(f"  Using cached raw NASHP file: {raw_candidates[0]}")
        return raw_candidates[0]

    logger.error(
        "\n" + "=" * 60 + "\n"
        "NASHP DATA NOT FOUND — MANUAL ACTION REQUIRED\n"
        "1. Visit: https://nashp.org/hospital-cost-tool/\n"
        "2. Click 'Download the data' → save as NASHP_HCT_Data_latest.xlsx\n"
        "3. Place in data/manual/ and commit.\n"
        "=" * 60
    )
    return None


def _playwright_download(output_dir: Path) -> Path | None:
    """Core Playwright logic. Raises ImportError if playwright isn't installed."""
    from playwright.sync_api import sync_playwright

    logger.info("  Launching Chromium headless for NASHP download…")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        logger.info(f"  Navigating to {NASHP_URL}…")
        page.goto(NASHP_URL, wait_until="networkidle", timeout=TIMEOUT_MS)
        _dismiss_cookie_banner(page)

        download_button = _find_download_button(page)
        if download_button is None:
            logger.warning("  Could not locate NASHP download button on page.")
            browser.close()
            return None

        logger.info("  Clicking download button and waiting for file…")
        with page.expect_download(timeout=DOWNLOAD_WAIT) as dl_info:
            download_button.click()

        download = dl_info.value
        suggested = download.suggested_filename or "NASHP_HCT_Data_latest.xlsx"
        dest = output_dir / suggested
        download.save_as(dest)

        size_kb = dest.stat().st_size / 1024
        logger.info(f"  ✓ NASHP file saved: {dest} ({size_kb:.0f} KB)")
        browser.close()
        return dest


def _dismiss_cookie_banner(page) -> None:
    """Attempt to dismiss common cookie/GDPR banners."""
    for sel in [
        "button:has-text('Accept')", "button:has-text('Accept All')",
        "button:has-text('I Accept')", "button:has-text('OK')",
        ".cookie-accept", "#cookie-accept",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2_000):
                btn.click()
                logger.info(f"  Dismissed cookie banner via '{sel}'")
                time.sleep(0.5)
                return
        except Exception:
            continue


def _find_download_button(page):
    """Find the NASHP 'Download the data' button/link using multiple strategies."""
    strategies = [
        "a:has-text('Download the data')",
        "button:has-text('Download the data')",
        "a:has-text('Download Data')",
        "a[href*='.xlsx']",
        "a[href*='download']",
        "a:has-text('download')",
        "button:has-text('download')",
    ]
    for sel in strategies:
        try:
            locator = page.locator(sel).first
            if locator.is_visible(timeout=3_000):
                logger.info(f"  Found download element via: '{sel}'")
                return locator
        except Exception:
            continue

    # Fallback: scan all links
    for link in page.locator("a[href]").all():
        try:
            href = link.get_attribute("href") or ""
            text = (link.inner_text() or "").strip().lower()
            if ".xlsx" in href.lower() or "download" in text:
                logger.info(f"  Fallback link found: {text!r} → {href}")
                return link
        except Exception:
            continue

    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    path = download_nashp_data(output_dir="data/raw", manual_dir="data/manual")
    print(f"NASHP file: {path}")
