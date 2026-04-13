"""
scrapers/reh_scraper.py
=======================
Builds REH_Info_Cleaned.csv automatically from two public sources:

  Source A — CMS Hospital Enrollments dataset (primary)
    UUID: f6f6505c-e8b0-4d57-b258-e2b94133aaf2
    Contains: REH CCN, Pre-REH CCN, conversion date, REH conversion flag
    Method: CMS data-api (JSON) — no browser needed

  Source B — UNC Sheps Center REH list (supplemental)
    URL: https://www.shepscenter.unc.edu/.../rural-emergency-hospitals/
    Contains: Previous Medicare Payment Type, Current Status
    Method: Playwright headless (JS-rendered table)

The two sources are merged on hospital name + state to produce the schema
that build_dataset.py expects in REH_Info_Cleaned.csv:
  Pre-REH CCN, Post-REH CCN, Hospital Name, State, REH Conversion Date,
  Current Status, Previous Medicare Payment Type

Usage:
    from scrapers.reh_scraper import download_reh_info
    path = download_reh_info(output_dir="data/raw")
"""

import io
import logging
import re
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── CMS Hospital Enrollments dataset ─────────────────────────────────────────
# https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments
CMS_ENROLLMENTS_UUID = "f6f6505c-e8b0-4d57-b258-e2b94133aaf2"
CMS_ENROLLMENTS_URL  = (
    f"https://data.cms.gov/data-api/v1/dataset/{CMS_ENROLLMENTS_UUID}/data"
)

# ── Sheps Center REH page ─────────────────────────────────────────────────────
SHEPS_REH_URL      = (
    "https://www.shepscenter.unc.edu/"
    "programs-projects/rural-health/rural-emergency-hospitals/"
)
PLAYWRIGHT_TIMEOUT = 60_000
PLAYWRIGHT_WAIT    = 12_000

REQUEST_TIMEOUT = 120


def download_reh_info(
    output_dir: str | Path = "data/raw",
    manual_dir: str | Path = "data/manual",
) -> Path | None:
    """
    Build and save REH_Info_Cleaned.csv.
    Returns the path, or None if all sources fail.
    """
    output_dir = Path(output_dir)
    manual_dir = Path(manual_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dest = output_dir / "REH_Info_Cleaned.csv"

    # ── Source A: CMS Hospital Enrollments (primary — most reliable) ──────────
    enrollments_df = _fetch_cms_enrollments()

    # ── Source B: Sheps Center (supplemental — adds payment type + status) ────
    sheps_df = _scrape_sheps_center()

    # ── Merge ─────────────────────────────────────────────────────────────────
    combined = _merge_sources(enrollments_df, sheps_df)

    if combined is None or len(combined) == 0:
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
    logger.info(f"  ✓ REH_Info_Cleaned.csv: {len(combined)} hospitals → {dest}")
    return dest


# ─────────────────────────────────────────────────────────────────────────────
# Source A: CMS Hospital Enrollments
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_cms_enrollments() -> pd.DataFrame:
    """
    Fetch all REH records from the CMS Hospital Enrollments dataset.
    Uses keyword search for 'rural emergency hospital' which matches
    PROVIDER TYPE TEXT = 'PART A PROVIDER - RURAL EMERGENCY HOSPITAL (REH)'.
    """
    logger.info("  Fetching CMS Hospital Enrollments — REH records…")
    try:
        resp = requests.get(
            CMS_ENROLLMENTS_URL,
            params={"size": 1000, "keyword": "rural emergency hospital"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        records = resp.json()
        if not isinstance(records, list):
            logger.warning("  Unexpected response format from CMS Enrollments API.")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        logger.info(f"  CMS Enrollments: {len(df):,} REH records")
        return df

    except Exception as exc:
        logger.warning(f"  CMS Enrollments fetch failed: {exc}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Source B: UNC Sheps Center
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_sheps_center() -> pd.DataFrame:
    """
    Scrape the REH table from the UNC Sheps Center page via Playwright.
    Returns a DataFrame with Previous Medicare Payment Type and Current Status.
    Falls back silently to an empty DataFrame on any failure.
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
        page.goto(SHEPS_REH_URL, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT)
        page.wait_for_timeout(PLAYWRIGHT_WAIT)

        try:
            page.wait_for_selector("table", timeout=10_000)
        except Exception:
            logger.warning("  No <table> found on Sheps REH page after wait.")
            browser.close()
            return pd.DataFrame()

        tables = pd.read_html(io.StringIO(page.content()))
        browser.close()

    if not tables:
        logger.warning("  No tables parsed from Sheps REH page.")
        return pd.DataFrame()

    # Pick the table most likely to have REH conversion data
    reh_table = None
    for tbl in tables:
        cols_lower = " ".join(str(c).lower() for c in tbl.columns)
        if any(kw in cols_lower for kw in ["hospital", "reh", "conversion", "date", "state"]):
            reh_table = tbl
            break
    if reh_table is None:
        reh_table = tables[0]

    reh_table.columns = [str(c).strip() for c in reh_table.columns]
    rename_map = _infer_sheps_columns(reh_table.columns.tolist())
    reh_table = reh_table.rename(columns=rename_map)

    keep = [c for c in ["Hospital Name", "State", "REH Conversion Date",
                         "Previous Medicare Payment Type", "Current Status"]
            if c in reh_table.columns]
    reh_table = reh_table[keep].dropna(how="all")
    logger.info(f"  ✓ Sheps Center: {len(reh_table)} REH records")
    return reh_table


def _infer_sheps_columns(cols: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for col in cols:
        cl = col.lower()
        if ("hospital" in cl or "facility" in cl) and "name" not in mapping.values():
            mapping[col] = "Hospital Name"
        elif "state" in cl and "address" not in cl and "State" not in mapping.values():
            mapping[col] = "State"
        elif ("date" in cl or "effective" in cl or "conversion" in cl) and "REH Conversion Date" not in mapping.values():
            mapping[col] = "REH Conversion Date"
        elif ("payment" in cl or "previous" in cl or "prior" in cl or "type" in cl) and "Previous Medicare Payment Type" not in mapping.values():
            mapping[col] = "Previous Medicare Payment Type"
        elif "status" in cl and "Current Status" not in mapping.values():
            mapping[col] = "Current Status"
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
# Merge logic
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_name(name) -> str:
    if pd.isna(name):
        return ""
    name = str(name).lower()
    name = re.sub(r"[^\w\s]", " ", name)
    for noise in ["hospital", "medical", "center", "health", "regional", "rural",
                  "emergency", "community", "general", "memorial", "critical", "access"]:
        name = name.replace(noise, " ")
    return re.sub(r"\s+", " ", name).strip()


def _merge_sources(
    enrollments_df: pd.DataFrame,
    sheps_df: pd.DataFrame,
) -> pd.DataFrame | None:
    """
    Build the final REH table from CMS Enrollments + Sheps Center data.

    CMS Enrollments provides:
        CCN              → Post-REH CCN
        CAH OR HOSPITAL CCN → Pre-REH CCN
        REH CONVERSION DATE → REH Conversion Date
        ORGANIZATION NAME   → Hospital Name
        STATE               → State
        REH CONVERSION FLAG → Previous Medicare Payment Type proxy

    Sheps supplements:
        Previous Medicare Payment Type (exact label)
        Current Status
    """
    if enrollments_df.empty and sheps_df.empty:
        return None

    # ── Build base from CMS Enrollments ──────────────────────────────────────
    if not enrollments_df.empty:
        reh = pd.DataFrame()
        reh["Post-REH CCN"]        = enrollments_df.get("CCN", pd.Series(dtype=str))
        reh["Pre-REH CCN"]         = enrollments_df.get("CAH OR HOSPITAL CCN", pd.Series(dtype=str))
        reh["Hospital Name"]       = enrollments_df.get("ORGANIZATION NAME", pd.Series(dtype=str))
        reh["State"]               = enrollments_df.get("STATE", enrollments_df.get("ENROLLMENT STATE", pd.Series(dtype=str)))
        reh["REH Conversion Date"] = enrollments_df.get("REH CONVERSION DATE", pd.Series(dtype=str))

        # Derive Previous Medicare Payment Type from conversion flag when available
        flag_col = enrollments_df.get("REH CONVERSION FLAG", pd.Series(dtype=str))
        reh["Previous Medicare Payment Type"] = flag_col.map(
            lambda x: "CAH" if str(x).upper() == "Y" else ("Short-Term Hospital" if pd.notna(x) and x else None)
        )
        reh["Current Status"] = "Active"   # all enrollments records are active

        # Clean up CCN columns — drop rows where both CCNs are blank
        reh["Post-REH CCN"] = pd.to_numeric(reh["Post-REH CCN"], errors="coerce")
        reh["Pre-REH CCN"]  = pd.to_numeric(reh["Pre-REH CCN"],  errors="coerce")
        reh = reh.dropna(subset=["Post-REH CCN"])

        logger.info(f"  Enrollments base: {len(reh)} REH hospitals")
    else:
        reh = pd.DataFrame(columns=[
            "Post-REH CCN", "Pre-REH CCN", "Hospital Name", "State",
            "REH Conversion Date", "Current Status", "Previous Medicare Payment Type",
        ])

    # ── Supplement with Sheps Center ─────────────────────────────────────────
    if not sheps_df.empty and not reh.empty:
        reh["_name_norm"]  = reh["Hospital Name"].apply(_normalize_name)
        reh["_state_norm"] = reh["State"].str.upper().str.strip()

        sheps_df = sheps_df.copy()
        if "Hospital Name" in sheps_df.columns:
            sheps_df["_name_norm"]  = sheps_df["Hospital Name"].apply(_normalize_name)
        if "State" in sheps_df.columns:
            sheps_df["_state_norm"] = sheps_df["State"].str.upper().str.strip()

        merge_cols = [c for c in ["_name_norm", "_state_norm", "Previous Medicare Payment Type",
                                   "Current Status", "REH Conversion Date"]
                      if c in sheps_df.columns]

        if "_name_norm" in merge_cols and "_state_norm" in merge_cols:
            reh = reh.merge(
                sheps_df[merge_cols].dropna(subset=["_name_norm"]),
                on=["_name_norm", "_state_norm"],
                how="left",
                suffixes=("", "_sheps"),
            )
            # Prefer Sheps values where CMS value is missing
            for col in ["Previous Medicare Payment Type", "Current Status", "REH Conversion Date"]:
                sheps_col = col + "_sheps"
                if sheps_col in reh.columns:
                    reh[col] = reh[col].combine_first(reh[sheps_col])
                    reh = reh.drop(columns=[sheps_col])

        reh = reh.drop(columns=["_name_norm", "_state_norm"], errors="ignore")

    elif not sheps_df.empty:
        # CMS Enrollments failed — use Sheps data only (no CCNs)
        reh = sheps_df.copy()
        reh["Post-REH CCN"] = None
        reh["Pre-REH CCN"]  = None

    # ── Final column ordering ─────────────────────────────────────────────────
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
