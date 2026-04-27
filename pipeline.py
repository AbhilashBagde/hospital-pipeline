"""
pipeline.py
===========
Orchestrates the full quarterly data pipeline:

  Step 1  — Download CMS cost reports (all available years)
  Step 2  — Download HRSA 340B covered entity file
  Step 3  — Download NASHP hospital cost data
  Step 4  — Build Final_Hospital_Dataset.csv  (build_dataset.py logic)
  Step 5  — Match 340B enrollment flags        (SST_update logic)
  Step 6  — Write SST_v3.csv and a timestamped archive copy

Usage:
    python pipeline.py                      # full run
    python pipeline.py --skip-download      # re-run build steps only
    python pipeline.py --years 2022 2023    # limit CMS years
    python pipeline.py --dry-run            # show plan without executing
"""

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
DATA_RAW    = BASE_DIR / "data" / "raw"
DATA_MANUAL = BASE_DIR / "data" / "manual"
DATA_OUT    = BASE_DIR / "data" / "output"
ARCHIVE_DIR = BASE_DIR / "data" / "archive"

for d in (DATA_RAW, DATA_MANUAL, DATA_OUT, ARCHIVE_DIR):
    d.mkdir(parents=True, exist_ok=True)

FINAL_DATASET_PATH  = DATA_OUT / "Final_Hospital_Dataset.csv"
SST_V3_PATH         = DATA_OUT / "SST_v3.csv"
SST_V4_PATH         = DATA_OUT / "SST_v4.csv"
BASELINES_JSON_PATH = DATA_OUT / "CAH_REH_Baselines.json"

# CDC PLACES demographic measures and their short output column names
_DEMO_MEASURES = [
    "Cancer (non-skin) or melanoma among adults",
    "Obesity among adults",
    "Food insecurity in the past 12 months among adults",
    "Current lack of health insurance among adults aged 18-64 years",
    "Any disability among adults",
    "Fair or poor self-rated health status among adults",
]
_DEMO_SHORT = {
    "Cancer (non-skin) or melanoma among adults":                   "Cancer",
    "Obesity among adults":                                         "Obesity",
    "Food insecurity in the past 12 months among adults":           "Food_Insecurity",
    "Current lack of health insurance among adults aged 18-64 years": "Uninsured",
    "Any disability among adults":                                  "Disability",
    "Fair or poor self-rated health status among adults":           "Poor_Health",
}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1-3: Download raw files
# ─────────────────────────────────────────────────────────────────────────────

def run_downloads(years: list[int] | None = None) -> dict:
    """Download all source files. Returns dict of resolved paths."""
    from scrapers import (
        download_cms_cost_reports,
        download_340b_entities,
        download_nashp_data,
    )

    logger.info("=" * 60)
    logger.info("STEP 1: Downloading CMS Hospital Provider Cost Reports")
    logger.info("=" * 60)
    cms_paths = download_cms_cost_reports(output_dir=DATA_RAW, years=years)
    if not cms_paths:
        logger.error("No CMS files downloaded. Cannot continue.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("STEP 2: Downloading HRSA 340B Covered Entity file")
    logger.info("=" * 60)
    path_340b = download_340b_entities(output_dir=DATA_RAW)
    if path_340b is None:
        logger.error("340B file unavailable. Cannot continue.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("STEP 3: Downloading NASHP Hospital Cost Tool data")
    logger.info("=" * 60)
    path_nashp = download_nashp_data(output_dir=DATA_RAW, manual_dir=DATA_MANUAL)
    if path_nashp is None:
        logger.error("NASHP file unavailable. See instructions above.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("STEP 3b: Scraping REH conversion info (Sheps Center + CMS POS)")
    logger.info("=" * 60)
    from scrapers import download_reh_info
    path_reh = download_reh_info(output_dir=DATA_RAW, manual_dir=DATA_MANUAL)
    if path_reh is None:
        logger.error("REH info unavailable. See instructions above.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("STEP 3c: Downloading CDC PLACES county demographic data")
    logger.info("=" * 60)
    from scrapers import download_places_data
    try:
        path_places = download_places_data(output_dir=DATA_RAW)
    except Exception as exc:
        logger.warning(f"  CDC PLACES download failed: {exc}")
        logger.warning("  Demographics step will be skipped.")
        path_places = None

    return {
        "cms": cms_paths,
        "path_340b": path_340b,
        "path_nashp": path_nashp,
        "path_reh": path_reh,
        "path_places": path_places,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Build Final_Hospital_Dataset.csv
# ─────────────────────────────────────────────────────────────────────────────

def run_build_dataset(file_paths: dict) -> Path:
    """
    Run build_dataset.py as a subprocess with file paths passed via BD_* env vars.
    Using subprocess (same pattern as run_build_baselines) avoids all import/reload
    timing issues: the child process starts with a clean slate and reads env vars
    before any module-level code touches the filesystem.
    """
    logger.info("=" * 60)
    logger.info("STEP 4: Building Final_Hospital_Dataset.csv")
    logger.info("=" * 60)

    import json

    env = os.environ.copy()
    env["BD_CMS_FILES"]   = json.dumps({yr: str(p) for yr, p in file_paths["cms"].items()})
    env["BD_NASHP_FILE"]  = str(file_paths["path_nashp"])
    env["BD_REH_FILE"]    = str(_find_reh_file())
    env["BD_OUTPUT_FILE"] = str(FINAL_DATASET_PATH)

    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "build_dataset.py")],
        env=env,
        cwd=str(BASE_DIR),
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError("build_dataset.py failed — see output above for details")

    logger.info(f"  Final_Hospital_Dataset.csv → {FINAL_DATASET_PATH}")
    return FINAL_DATASET_PATH


def _find_reh_file() -> Path:
    """Find REH Info Cleaned CSV; prefer manual/ then raw/."""
    for candidate in [
        DATA_MANUAL / "REH_Info_Cleaned.csv",
        DATA_RAW / "REH_Info_Cleaned.csv",
    ]:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "REH_Info_Cleaned.csv not found.\n"
        "This file must be committed manually to data/manual/REH_Info_Cleaned.csv\n"
        "It tracks hospitals that converted from CAH to REH status."
    )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Add 340B enrollment flag → SST_v3.csv
# ─────────────────────────────────────────────────────────────────────────────

def run_340b_matching(path_final: Path, path_340b: Path) -> Path:
    """
    Match hospitals to 340B covered entities. Dispatches to CCN-based
    date-overlap matching when the file is the OPAIS Export format (has a
    'Medicare Provider Number' column in the 'Covered Entity Details' sheet);
    otherwise falls back to TF-IDF + optional OpenAI embedding rerank.
    Saves result as SST_v3.csv.
    """
    logger.info("=" * 60)
    logger.info("STEP 5: Matching 340B enrollment flags")
    logger.info("=" * 60)

    import pandas as pd

    HOSPITAL_ENTITY_TYPES = {"CAH", "SCH", "DSH", "PED", "CAN", "RRC", "SOL"}

    logger.info("  Loading Final_Hospital_Dataset…")
    sst_df = pd.read_csv(path_final, dtype=str, low_memory=False)
    logger.info(f"  SST rows: {len(sst_df):,}")

    # ── Detect 340B file format ───────────────────────────────────────────────
    # Export format : sheet='Covered Entity Details', skiprows=4, has 'Medicare Provider Number'
    # Daily format  : sheet='Covered Entities', no skiprows, name/address columns only
    entity_df = None
    use_ccn = False
    try:
        entity_df = pd.read_excel(
            path_340b, sheet_name="Covered Entity Details", dtype=str, skiprows=4
        )
        entity_df = entity_df[entity_df["Entity Type"].isin(HOSPITAL_ENTITY_TYPES)].copy()
        use_ccn = "Medicare Provider Number" in entity_df.columns
    except Exception:
        pass

    if use_ccn:
        logger.info("  OPAIS Export format detected — using CCN-based date-overlap matching.")
        logger.info(f"  340B hospital rows: {len(entity_df):,}")
        return _run_340b_ccn(sst_df, entity_df)

    # Fall back to daily download format
    logger.info("  Daily download format — using TF-IDF name-similarity matching.")
    if entity_df is None:
        try:
            entity_df = pd.read_excel(path_340b, sheet_name="Covered Entities", dtype=str)
        except Exception:
            entity_df = pd.read_excel(path_340b, dtype=str)
    logger.info(f"  340B rows: {len(entity_df):,}")
    return _run_340b_tfidf(sst_df, entity_df)


def _run_340b_ccn(sst_df, entity_df) -> Path:
    """
    CCN-based date-overlap matching (notebook SST_340B_addition.ipynb approach).
    Requires entity_df to have 'Medicare Provider Number', 'Participating Start Date',
    'Termination Date' columns.
    """
    import pandas as pd
    from collections import defaultdict
    from dateutil import parser as dateparser

    def to_ccn6(val):
        if pd.isna(val):
            return None
        s = str(val).strip().split(".")[0]
        return s.zfill(6)

    def parse_date(val):
        if pd.isna(val) or str(val).strip() == "":
            return None
        try:
            return dateparser.parse(str(val)).date()
        except Exception:
            return None

    sst_df = sst_df.copy()
    sst_df["CCN6"] = sst_df["CCN"].apply(to_ccn6)
    entity_df["CCN6"] = entity_df["Medicare Provider Number"].apply(to_ccn6)

    # SST fiscal year: NASHP preferred, CMS fallback
    sst_df["_fy_begin"] = sst_df.apply(
        lambda r: parse_date(r.get("FY_Begin_NASHP"))
        if r.get("Data_Source") != "CMS only"
        else parse_date(r.get("FY_Begin_CMS")),
        axis=1,
    )
    sst_df["_fy_end"] = sst_df.apply(
        lambda r: parse_date(r.get("FY_End_NASHP"))
        if r.get("Data_Source") != "CMS only"
        else parse_date(r.get("FY_End_CMS")),
        axis=1,
    )

    entity_df["_start"] = entity_df["Participating Start Date"].apply(parse_date)
    entity_df["_term"]  = entity_df["Termination Date"].apply(parse_date)

    # Build CCN → list of (enroll_start, term) lookup
    ccn_periods: dict = {}
    from collections import defaultdict
    ccn_periods = defaultdict(list)
    for _, row in entity_df.iterrows():
        if row["CCN6"]:
            ccn_periods[row["CCN6"]].append((row["_start"], row["_term"]))

    def overlaps(fy_begin, fy_end, enroll_start, term_date):
        if fy_begin is None or fy_end is None:
            return True   # can't rule out overlap without dates
        if enroll_start is None:
            return True   # no start date → assume enrolled
        if term_date is not None and term_date < fy_begin:
            return False  # terminated before FY started
        if enroll_start > fy_end:
            return False  # enrolled after FY ended
        return True

    labels = []
    for _, row in sst_df.iterrows():
        enrolled = 0
        for (start, term) in ccn_periods.get(row["CCN6"], []):
            if overlaps(row["_fy_begin"], row["_fy_end"], start, term):
                enrolled = 1
                break
        labels.append(enrolled)

    sst_df["Is_340B_Enrolled"] = labels
    sst_df["CCN"] = sst_df["CCN6"]   # overwrite with zero-padded string
    sst_df = sst_df.drop(columns=["CCN6", "_fy_begin", "_fy_end"])

    enrolled_count = sum(labels)
    unique_ccns    = sst_df["CCN"].dropna().unique()
    matched_ccns   = [c for c in unique_ccns if c in ccn_periods]
    logger.info(f"  340B enrolled: {enrolled_count:,} / {len(sst_df):,} rows")
    logger.info(
        f"  CCNs matched: {len(matched_ccns):,} / {len(unique_ccns):,} unique "
        f"({len(matched_ccns)/max(len(unique_ccns),1):.1%})"
    )

    sst_df.to_csv(SST_V3_PATH, index=False)
    logger.info(f"  ✓ SST_v3.csv → {SST_V3_PATH}")
    return SST_V3_PATH


def _run_340b_tfidf(sst_df, entity_df) -> Path:
    """
    TF-IDF name-similarity matching with optional OpenAI embedding rerank.
    Used when the 340B file is the daily download format (no CCN column).
    """
    import pandas as pd
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    NOISE = re.compile(r"\b(hospital|center|medical|health|system|regional|community)\b")

    def normalize(text):
        if pd.isna(text):
            return ""
        text = str(text).lower()
        text = re.sub(r"[^\w\s]", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def build_sst_text(row):
        return normalize(
            f"{row.get('Hospital_Name','')} {row.get('City','')} "
            f"{row.get('State','')} {row.get('Address','')}"
        )

    def build_entity_text(row):
        return normalize(
            f"{row.get('Entity Name','')} {row.get('Street City','')} "
            f"{row.get('Street State','')} {row.get('Street Address 1','')}"
        )

    sst_texts    = [NOISE.sub("", build_sst_text(r)).strip()    for _, r in sst_df.iterrows()]
    entity_texts = [NOISE.sub("", build_entity_text(r)).strip() for _, r in entity_df.iterrows()]

    logger.info("  Building TF-IDF shortlists…")
    TFIDF_TOP_K = 10
    corpus = sst_texts + entity_texts
    vect = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 4), min_df=1)
    vect.fit(corpus)
    sst_mat    = vect.transform(sst_texts)
    entity_mat = vect.transform(entity_texts)

    candidates: list[list[int]] = []
    chunk = 500
    for i in range(0, len(sst_texts), chunk):
        sims = cosine_similarity(sst_mat[i : i + chunk], entity_mat)
        for row_sims in sims:
            candidates.append(row_sims.argsort()[-TFIDF_TOP_K:][::-1].tolist())

    api_key = os.environ.get("OPENAI_API_KEY")
    EMBED_THRESHOLD  = 0.82
    TFIDF_THRESHOLD  = 0.40
    labels: list[int] = []

    if api_key:
        logger.info("  OPENAI_API_KEY found — using embedding rerank…")
        from openai import OpenAI
        import numpy as np_
        client = OpenAI(api_key=api_key)

        def embed_batch(texts):
            resp = client.embeddings.create(model="text-embedding-3-small", input=texts)
            return [e.embedding for e in resp.data]

        entity_name_texts = [normalize(str(r.get("Entity Name", ""))) for _, r in entity_df.iterrows()]
        sst_name_texts    = [normalize(str(r.get("Hospital_Name", ""))) for _, r in sst_df.iterrows()]

        entity_embeddings = []
        for i in range(0, len(entity_name_texts), 512):
            entity_embeddings.extend(embed_batch(entity_name_texts[i : i + 512]))
            time.sleep(0.05)
        entity_embed_mat = np_.array(entity_embeddings)

        for i, (_, cand_indices) in enumerate(zip(sst_df.itertuples(), candidates)):
            if not cand_indices:
                labels.append(0)
                continue
            sst_vec  = np_.array(embed_batch([sst_name_texts[i]])[0]).reshape(1, -1)
            cand_mat = entity_embed_mat[cand_indices]
            best_sim = cosine_similarity(sst_vec, cand_mat)[0].max()
            labels.append(1 if best_sim >= EMBED_THRESHOLD else 0)
            if i % 500 == 0:
                logger.info(f"    Matched {i}/{len(sst_df):,}…")
            time.sleep(0.05)
    else:
        logger.info(
            "  OPENAI_API_KEY not set — TF-IDF score only "
            "(set key for higher accuracy via embedding rerank)."
        )
        for i, cand_indices in enumerate(candidates):
            if not cand_indices:
                labels.append(0)
                continue
            sims = cosine_similarity(sst_mat[i : i + 1], entity_mat[cand_indices])[0]
            labels.append(1 if sims.max() >= TFIDF_THRESHOLD else 0)

    sst_df = sst_df.copy()
    sst_df["Is_340B_Enrolled"] = labels
    enrolled_count = sum(labels)
    logger.info(f"  340B enrolled: {enrolled_count:,} / {len(sst_df):,} rows")

    sst_df.to_csv(SST_V3_PATH, index=False)
    logger.info(f"  ✓ SST_v3.csv → {SST_V3_PATH}")
    return SST_V3_PATH


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Merge CDC PLACES demographic data → SST_v4.csv
# ─────────────────────────────────────────────────────────────────────────────

def run_demographics_matching(sst_path: Path, places_path: Path | None) -> Path | None:
    """
    Join county-level CDC PLACES demographic data onto SST_v3.
    Adds 12 columns (6 raw % values + 6 percentile ranks) and writes SST_v4.csv.
    Returns the output path, or None if places_path is None/missing.
    """
    if places_path is None or not Path(places_path).exists():
        logger.warning("  No PLACES file — skipping demographics step.")
        return None

    logger.info("=" * 60)
    logger.info("STEP 6: Merging county-level demographic data (CDC PLACES)")
    logger.info("=" * 60)

    import pandas as pd

    sst = pd.read_csv(sst_path, dtype=str, low_memory=False)
    places = pd.read_csv(places_path, low_memory=False)

    # Normalise column names to lowercase for robustness across download formats
    places.columns = [c.strip().lower() for c in places.columns]
    # Map expected names (Socrata returns lowercase; direct export may vary)
    measure_col  = "measure"
    state_col    = "stateabbr"
    location_col = "locationname"
    year_col     = "year"
    value_col    = "data_value"

    places_filtered = places[places[measure_col].isin(_DEMO_MEASURES)].copy()
    if places_filtered.empty:
        logger.warning("  No matching measures found in PLACES file — skipping.")
        return None

    # Pivot wide, keeping the most recent year for each county × measure
    places_wide = (
        places_filtered[[year_col, state_col, location_col, measure_col, value_col]]
        .sort_values(year_col, ascending=False)
        .drop_duplicates(subset=[state_col, location_col, measure_col])
        .pivot(index=[year_col, state_col, location_col], columns=measure_col, values=value_col)
        .reset_index()
    )
    places_wide.columns.name = None
    places_wide.drop(columns=[year_col], inplace=True)

    # Rename long measure names to short column names
    places_wide.rename(columns=_DEMO_SHORT, inplace=True)

    # Compute national percentile for each measure (higher = worse for all six)
    for short in _DEMO_SHORT.values():
        if short in places_wide.columns:
            places_wide[f"{short}_PCTL"] = places_wide[short].rank(pct=True) * 100

    # Case-insensitive county merge
    sst["_county_key"] = sst["County"].str.strip().str.lower()
    places_wide["_county_key"] = places_wide[location_col].str.strip().str.lower()

    merged = pd.merge(
        sst,
        places_wide,
        left_on=["State", "_county_key"],
        right_on=[state_col, "_county_key"],
        how="left",
    )

    # Drop internal merge helpers and places join keys
    drop_cols = ["_county_key", state_col, location_col]
    merged.drop(columns=[c for c in drop_cols if c in merged.columns], inplace=True)

    # Coverage report
    first_demo = list(_DEMO_SHORT.values())[0]
    n_matched = int(merged[first_demo].notna().sum()) if first_demo in merged.columns else 0
    logger.info(f"  Rows with demographic data: {n_matched:,} / {len(merged):,}")
    for short in _DEMO_SHORT.values():
        if short in merged.columns:
            miss = int(merged[short].isna().sum())
            logger.info(f"    {short}: {miss:,} missing ({miss/len(merged):.1%})")

    merged.to_csv(SST_V4_PATH, index=False)
    logger.info(f"  ✓ SST_v4.csv → {SST_V4_PATH}")
    return SST_V4_PATH


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: Build CAH_REH_Baselines.json
# ─────────────────────────────────────────────────────────────────────────────

def run_build_baselines(sst_path: Path) -> Path:
    """Run build_baselines_json.py to produce CAH_REH_Baselines.json."""
    logger.info("  Running build_baselines_json.py…")
    result = subprocess.run(
        [sys.executable,
         str(BASE_DIR / "build_baselines_json.py"),
         str(sst_path),
         str(BASELINES_JSON_PATH)],
        capture_output=True, text=True, cwd=str(BASE_DIR),
    )
    for line in result.stdout.splitlines():
        logger.info(f"  {line}")
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("build_baselines_json.py failed")
    logger.info(f"  ✓ CAH_REH_Baselines.json → {BASELINES_JSON_PATH}")
    return BASELINES_JSON_PATH


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: Archive outputs
# ─────────────────────────────────────────────────────────────────────────────

def archive_outputs():
    """Copy SST_v3/v4.csv and CAH_REH_Baselines.json to archive with a timestamp suffix."""
    today = date.today().strftime("%Y%m%d")

    archive_v3 = ARCHIVE_DIR / f"SST_v3_{today}.csv"
    shutil.copy2(SST_V3_PATH, archive_v3)
    logger.info(f"  Archived: {archive_v3}")

    if SST_V4_PATH.exists():
        archive_v4 = ARCHIVE_DIR / f"SST_v4_{today}.csv"
        shutil.copy2(SST_V4_PATH, archive_v4)
        logger.info(f"  Archived: {archive_v4}")

    if BASELINES_JSON_PATH.exists():
        archive_json = ARCHIVE_DIR / f"CAH_REH_Baselines_{today}.json"
        shutil.copy2(BASELINES_JSON_PATH, archive_json)
        logger.info(f"  Archived: {archive_json}")

    manifest = DATA_OUT / "pipeline_manifest.txt"
    with manifest.open("w") as f:
        f.write(f"Pipeline run: {datetime.now().isoformat()}\n")
        f.write(f"SST_v3.csv rows: {_count_csv_rows(SST_V3_PATH):,}\n")
        f.write(f"Archive: {archive_v3.name}\n")
        if SST_V4_PATH.exists():
            f.write(f"SST_v4.csv rows: {_count_csv_rows(SST_V4_PATH):,}\n")
        if BASELINES_JSON_PATH.exists():
            f.write(f"CAH_REH_Baselines.json: yes\n")
    logger.info(f"  Manifest: {manifest}")


def _count_csv_rows(path: Path) -> int:
    with path.open() as f:
        return sum(1 for _ in f) - 1  # subtract header


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Hospital data pipeline — quarterly update"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip scraping steps; use whatever is already in data/raw/",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        metavar="YEAR",
        help="Limit CMS download to specific years, e.g. --years 2022 2023",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print plan without executing anything",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start = time.time()

    if args.dry_run:
        logger.info("DRY RUN — no files will be downloaded or modified.")
        logger.info(f"  Would download CMS years: {args.years or 'all known'}")
        logger.info("  Would download 340B from HRSA OPAIS")
        logger.info("  Would scrape NASHP hospital cost tool page")
        logger.info("  Would download CDC PLACES county demographic data")
        logger.info("  Would run build_dataset.py → Final_Hospital_Dataset.csv")
        logger.info("  Would run 340B matching → SST_v3.csv")
        logger.info("  Would run demographics merge → SST_v4.csv")
        logger.info("  Would run build_baselines_json.py → CAH_REH_Baselines.json")
        logger.info("  Would archive outputs")
        return

    # ── Download phase ────────────────────────────────────────────────────────
    if args.skip_download:
        logger.info("Skipping downloads (--skip-download).")
        # Resolve existing raw files for the build phase
        cms_paths = {
            int(p.stem.split("_")[-1]): p
            for p in DATA_RAW.glob("Hospital_Provider_Cost_Report_*.csv")
        }
        path_340b = sorted(DATA_RAW.glob("340B_CoveredEntity_Daily_*.xlsx"), reverse=True)
        path_340b = path_340b[0] if path_340b else None
        path_nashp = (
            sorted(DATA_RAW.glob("NASHP*.xlsx"), reverse=True) or
            sorted(DATA_MANUAL.glob("NASHP*.xlsx"), reverse=True)
        )
        path_nashp = path_nashp[0] if path_nashp else None
        path_places_list = sorted(DATA_RAW.glob("PLACES_county_*.csv"), reverse=True)
        path_places = path_places_list[0] if path_places_list else None
        file_paths = {
            "cms": cms_paths,
            "path_340b": path_340b,
            "path_nashp": path_nashp,
            "path_places": path_places,
        }

        if not cms_paths:
            logger.error("No CMS files in data/raw/. Run without --skip-download first.")
            sys.exit(1)
        if not path_340b:
            logger.error("No 340B file in data/raw/. Run without --skip-download first.")
            sys.exit(1)
        if not path_nashp:
            logger.error("No NASHP file found. See MANUAL FALLBACK in scrapers/nashp_scraper.py.")
            sys.exit(1)
    else:
        file_paths = run_downloads(years=args.years)

    # ── Build phase ───────────────────────────────────────────────────────────
    path_final = run_build_dataset(file_paths)

    # ── 340B matching phase ───────────────────────────────────────────────────
    run_340b_matching(path_final, file_paths["path_340b"])

    # ── Demographics merge phase ──────────────────────────────────────────────
    run_demographics_matching(SST_V3_PATH, file_paths.get("path_places"))

    # ── Baselines JSON ────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 7: Building CAH_REH_Baselines.json")
    logger.info("=" * 60)
    run_build_baselines(SST_V3_PATH)

    # ── Archive ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 8: Archiving outputs")
    logger.info("=" * 60)
    archive_outputs()

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.0f}s")
    logger.info(f"  Output: {SST_V3_PATH}")
    if SST_V4_PATH.exists():
        logger.info(f"  Output: {SST_V4_PATH}  (with demographics)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
