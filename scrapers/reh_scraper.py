"""
scrapers/reh_scraper.py
=======================
Builds REH_Info_Cleaned.csv automatically from two public sources:

  Source A — UNC Sheps Center REH list (live, quarterly updated)
    URL: https://www.shepscenter.unc.edu/programs-projects/rural-health/rural-emergency-hospitals/
    Contains: Hospital name, state, previous payment type, effective REH date
    Method: Playwright headless (JavaScript-rendered table)

  Source B — CMS Provider of Services (POS) file (quarterly updated)
    URL: data.cms.gov Socrata API
    Contains: CCN, facility type, termination/effective dates
    Method: Socrata API → filter for REH facility type codes
    Used to: (a) resolve CCNs, (b) find Pre-REH CCNs via a second POS query

The two sources are merged on hospital name + state to produce the same
schema that build_dataset.py expects in REH_Info_Cleaned.csv:
  Pre-REH CCN, Post-REH CCN, Hospital Name, State, REH Conversion Date,
  Current Status, Previous Medicare Payment Type

Usage:
    from scrapers.reh_scraper import download_reh_info
    path = download_reh_info(output_dir="data/raw")
"""

import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── CMS Provider of Services (POS) Socrata dataset ────────────────────────────
# Hospital & Non-Hospital Facilities file — updated quarterly
# Browse at: https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/
#            provider-of-services-file-hospital-non-hospital-facilities
POS_DATASET_ID = "395p-9vve"          # verify at data.cms.gov if downloads fail
POS_SODA_URL   = f"https://data.cms.gov/resource/{POS_DATASET_ID}.csv"

# REH facility type code in the POS file (PRVDR_CTGRY_CD)
REH_CATEGORY_CODE = "16"              # CMS facility category for REH

# CAH facility type code — used to find pre-conversion records
CAH_CATEGORY_CODE = "11"

# ── Sheps Center REH page ─────────────────────────────────────────────────────
SHEPS_REH_URL = (
    "https://www.shepscenter.unc.edu/"
    "programs-projects/rural-health/rural-emergency-hospitals/"
)

REQUEST_TIMEOUT = 120
PLAYWRIGHT_TIMEOUT = 60_000
PLAYWRIGHT_WAIT    = 15_000   # wait for table to render after page load


def download_reh_info(
    output_dir: str | Path = "data/raw",
    manual_dir: str | Path = "data/manual",
) -> Path | None:
    """
    Build and save REH_Info_Cleaned.csv.
    Returns the path, or None if both sources fail.
    """
    output_dir = Path(output_dir)
    manual_dir = Path(manual_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dest = output_dir / "REH_Info_Cleaned.csv"

    # ── Source A: Sheps Center list ───────────────────────────────────────────
    sheps_df = _scrape_sheps_center()

    # ── Source B: CMS POS file ────────────────────────────────────────────────
    pos_reh_df = _fetch_pos_reh()
    pos_cah_df = _fetch_pos_cah()      # needed to map pre-REH CCNs

    # ── Merge ─────────────────────────────────────────────────────────────────
    combined = _merge_sources(sheps_df, pos_reh_df, pos_cah_df)

    if combined is None or len(combined) == 0:
        # Fall back to previously committed file
        for candidate_dir in [manual_dir, output_dir]:
            candidates = sorted(candidate_dir.glob("REH_Info*.csv"), reverse=True)
            if candidates:
                logger.warning(
                    f"  Scraping yielded no rows. Using cached: {candidates[0]}"
                )
                return candidates[0]
        logger.error(
            "\n" + "=" * 60 + "\n"
            "REH INFO NOT FOUND — MANUAL ACTION REQUIRED\n"
            "1. Download REH list from:\n"
            "   https://www.shepscenter.unc.edu/programs-projects/\n"
            "   rural-health/rural-emergency-hospitals/\n"
            "2. Save as data/manual/REH_Info_Cleaned.csv\n"
            "   with columns: Pre-REH CCN, Post-REH CCN, Hospital Name,\n"
            "   State, REH Conversion Date, Current Status,\n"
            "   Previous Medicare Payment Type\n"
            "=" * 60
        )
        return None

    combined.to_csv(dest, index=False)
    logger.info(f"  ✓ REH_Info_Cleaned.csv saved: {len(combined)} hospitals → {dest}")
    return dest


# ─────────────────────────────────────────────────────────────────────────────
# Source A: UNC Sheps Center
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_sheps_center() -> pd.DataFrame:
    """
    Scrape the REH list table from the UNC Sheps Center page via Playwright.
    Returns a DataFrame with columns: Hospital Name, State, REH Conversion Date,
    Previous Medicare Payment Type, Current Status.
    """
    logger.info("  Scraping UNC Sheps Center REH list…")
    try:
        return _playwright_sheps()
    except ImportError:
        logger.warning("  playwright not installed — skipping Sheps Center scrape.")
    except Exception as exc:
        logger.warning(f"  Sheps Center scrape failed: {exc}")
    return pd.DataFrame()


def _playwright_sheps() -> pd.DataFrame:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page()
        logger.info(f"  Loading {SHEPS_REH_URL}…")
        page.goto(SHEPS_REH_URL, wait_until="networkidle", timeout=PLAYWRIGHT_TIMEOUT)

        # Wait for the data table to appear
        try:
            page.wait_for_selector("table", timeout=PLAYWRIGHT_WAIT)
        except Exception:
            logger.warning("  No <table> found on Sheps REH page.")
            browser.close()
            return pd.DataFrame()

        # Extract all tables on the page and pick the one with REH data
        tables = pd.read_html(page.content())
        browser.close()

    if not tables:
        logger.warning("  No tables parsed from Sheps REH page.")
        return pd.DataFrame()

    # Find the table most likely to contain REH conversion data
    reh_table = None
    for tbl in tables:
        cols_lower = [str(c).lower() for c in tbl.columns]
        if any(kw in " ".join(cols_lower) for kw in ["hospital", "reh", "conversion", "date", "state"]):
            reh_table = tbl
            break

    if reh_table is None:
        reh_table = tables[0]  # best guess

    logger.info(f"  Sheps table shape: {reh_table.shape}")
    logger.debug(f"  Columns: {list(reh_table.columns)}")

    # Normalize column names
    reh_table.columns = [str(c).strip() for c in reh_table.columns]
    rename_map = _infer_sheps_columns(reh_table.columns.tolist())
    reh_table = reh_table.rename(columns=rename_map)

    # Keep only the columns we need
    keep = [c for c in ["Hospital Name", "State", "REH Conversion Date",
                         "Previous Medicare Payment Type", "Current Status"]
            if c in reh_table.columns]
    reh_table = reh_table[keep].copy()
    reh_table = reh_table.dropna(how="all")

    logger.info(f"  ✓ Sheps Center: {len(reh_table)} REH records")
    return reh_table


def _infer_sheps_columns(cols: list[str]) -> dict[str, str]:
    """
    Map whatever column names Sheps uses to our standard names.
    This is intentionally flexible because Sheps may rename columns.
    """
    mapping: dict[str, str] = {}
    for col in cols:
        cl = col.lower()
        if "hospital" in cl or "facility" in cl or "name" in cl:
            mapping[col] = "Hospital Name"
        elif "state" in cl and "address" not in cl:
            mapping[col] = "State"
        elif "date" in cl or "effective" in cl or "conversion" in cl:
            mapping[col] = "REH Conversion Date"
        elif "payment" in cl or "previous" in cl or "prior" in cl or "type" in cl:
            mapping[col] = "Previous Medicare Payment Type"
        elif "status" in cl:
            mapping[col] = "Current Status"
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
# Source B: CMS Provider of Services
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_pos_for_category(category_code: str) -> pd.DataFrame:
    """Fetch POS records for a given facility category code via Socrata."""
    url = POS_SODA_URL
    params = {
        "$where": f"PRVDR_CTGRY_CD='{category_code}'",
        "$select": (
            "PRVDR_NUM,FAC_NAME,ST_ADR,CITY_NAME,STATE_CD,ZIP_CD,"
            "PRVDR_CTGRY_CD,PRVDR_CTGRY_SBTYP_CD,"
            "CRTFCTN_DT,TRMNTN_EXPRTN_DT"
        ),
        "$limit": 10_000,
    }
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        df = pd.read_csv(
            __import__("io").StringIO(resp.text),
            dtype={"PRVDR_NUM": str, "ZIP_CD": str},
            low_memory=False,
        )
        logger.info(f"  CMS POS category {category_code}: {len(df):,} records")
        return df
    except Exception as exc:
        logger.warning(f"  CMS POS fetch for category {category_code} failed: {exc}")
        return pd.DataFrame()


def _fetch_pos_reh() -> pd.DataFrame:
    logger.info("  Fetching CMS POS — REH facilities…")
    return _fetch_pos_for_category(REH_CATEGORY_CODE)


def _fetch_pos_cah() -> pd.DataFrame:
    logger.info("  Fetching CMS POS — CAH facilities (for Pre-REH CCN mapping)…")
    return _fetch_pos_for_category(CAH_CATEGORY_CODE)


# ─────────────────────────────────────────────────────────────────────────────
# Merge logic
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_name(name) -> str:
    if pd.isna(name):
        return ""
    name = str(name).lower()
    name = re.sub(r"[^\w\s]", " ", name)
    # Remove common noise words
    for noise in ["hospital", "medical", "center", "health", "regional", "rural",
                  "emergency", "community", "general", "memorial", "critical", "access"]:
        name = name.replace(noise, " ")
    return re.sub(r"\s+", " ", name).strip()


def _merge_sources(
    sheps_df: pd.DataFrame,
    pos_reh_df: pd.DataFrame,
    pos_cah_df: pd.DataFrame,
) -> pd.DataFrame | None:
    """
    Merge Sheps Center REH list with CMS POS data.

    Logic:
    - pos_reh_df gives us the current REH CCN (PRVDR_NUM) and certification date
    - pos_cah_df gives us the old CAH CCN for hospitals that changed CCN
    - sheps_df gives us Previous Medicare Payment Type and Current Status
    - We match on normalized hospital name + state
    """
    if pos_reh_df.empty and sheps_df.empty:
        return None

    # ── Build REH base from POS ───────────────────────────────────────────────
    if not pos_reh_df.empty:
        reh = pos_reh_df.rename(columns={
            "PRVDR_NUM":           "Post-REH CCN",
            "FAC_NAME":            "Hospital Name",
            "STATE_CD":            "State",
            "CRTFCTN_DT":          "REH Conversion Date",
            "TRMNTN_EXPRTN_DT":    "_Termination",
        }).copy()
        reh["Current Status"] = reh["_Termination"].apply(
            lambda x: "Active" if pd.isna(x) or str(x).strip() == "" else "Terminated"
        )
        reh = reh[["Post-REH CCN", "Hospital Name", "State",
                   "REH Conversion Date", "Current Status"]].copy()
    else:
        reh = pd.DataFrame(columns=["Post-REH CCN", "Hospital Name", "State",
                                    "REH Conversion Date", "Current Status"])

    # ── Merge with Sheps to get Previous Payment Type ─────────────────────────
    if not sheps_df.empty and not reh.empty:
        reh["_name_norm"]   = reh["Hospital Name"].apply(_normalize_name)
        reh["_state_norm"]  = reh["State"].str.upper().str.strip()
        sheps_df["_name_norm"]  = sheps_df.get("Hospital Name", pd.Series(dtype=str)).apply(_normalize_name)
        sheps_df["_state_norm"] = sheps_df.get("State", pd.Series(dtype=str)).str.upper().str.strip()

        reh = reh.merge(
            sheps_df[["_name_norm", "_state_norm", "Previous Medicare Payment Type"]].dropna(),
            on=["_name_norm", "_state_norm"],
            how="left",
        )
        reh = reh.drop(columns=["_name_norm", "_state_norm"], errors="ignore")
    elif not sheps_df.empty:
        # POS failed — use Sheps data only (no CCNs)
        reh = sheps_df.copy()
        reh["Post-REH CCN"] = None

    # ── Try to find Pre-REH CCN from CAH POS ──────────────────────────────────
    if not pos_cah_df.empty and not reh.empty:
        cah = pos_cah_df.rename(columns={
            "PRVDR_NUM": "Pre-REH CCN",
            "FAC_NAME":  "_cah_name",
            "STATE_CD":  "_cah_state",
        }).copy()
        cah["_name_norm"]  = cah["_cah_name"].apply(_normalize_name)
        cah["_state_norm"] = cah["_cah_state"].str.upper().str.strip()
        # Keep only the most recent CAH record per name+state (some have multiple entries)
        cah = cah.sort_values("CRTFCTN_DT", ascending=False).drop_duplicates(
            subset=["_name_norm", "_state_norm"], keep="first"
        )

        if "_name_norm" not in reh.columns:
            reh["_name_norm"]  = reh["Hospital Name"].apply(_normalize_name)
            reh["_state_norm"] = reh["State"].str.upper().str.strip()

        reh = reh.merge(
            cah[["Pre-REH CCN", "_name_norm", "_state_norm"]],
            on=["_name_norm", "_state_norm"],
            how="left",
        )
        reh = reh.drop(columns=["_name_norm", "_state_norm"], errors="ignore")

    # ── Fill defaults ─────────────────────────────────────────────────────────
    if "Pre-REH CCN" not in reh.columns:
        reh["Pre-REH CCN"] = None
    if "Previous Medicare Payment Type" not in reh.columns:
        reh["Previous Medicare Payment Type"] = None

    # ── Reorder to match build_dataset.py expectations ────────────────────────
    final_cols = [
        "Pre-REH CCN", "Post-REH CCN", "Hospital Name", "State",
        "REH Conversion Date", "Current Status", "Previous Medicare Payment Type",
    ]
    for col in final_cols:
        if col not in reh.columns:
            reh[col] = None

    return reh[final_cols].dropna(subset=["Hospital Name"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    path = download_reh_info(output_dir="data/raw", manual_dir="data/manual")
    print(f"REH info file: {path}")
