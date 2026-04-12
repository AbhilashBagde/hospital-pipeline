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

FINAL_DATASET_PATH = DATA_OUT / "Final_Hospital_Dataset.csv"
SST_V3_PATH        = DATA_OUT / "SST_v3.csv"


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

    return {
        "cms": cms_paths,
        "path_340b": path_340b,
        "path_nashp": path_nashp,
        "path_reh": path_reh,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Build Final_Hospital_Dataset.csv
# ─────────────────────────────────────────────────────────────────────────────

def run_build_dataset(file_paths: dict) -> Path:
    """
    Run build_dataset.py as a subprocess with file paths passed via env vars.
    This avoids the module-reload problem (reload re-executes module-level code,
    overwriting any patches).
    """
    logger.info("=" * 60)
    logger.info("STEP 4: Building Final_Hospital_Dataset.csv")
    logger.info("=" * 60)

    env = os.environ.copy()
    env["BD_CMS_FILES"]   = json.dumps({str(yr): str(p) for yr, p in file_paths["cms"].items()})
    env["BD_NASHP_FILE"]  = str(file_paths["path_nashp"])
    env["BD_REH_FILE"]    = str(_find_reh_file())
    env["BD_OUTPUT_FILE"] = str(FINAL_DATASET_PATH)

    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "build_dataset.py")],
        env=env,
        cwd=str(BASE_DIR),
    )
    if result.returncode != 0:
        logger.error("build_dataset.py exited with non-zero status.")
        sys.exit(result.returncode)

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
    Match hospitals to 340B covered entities using TF-IDF + embedding approach
    from SST_update.ipynb. Saves result as SST_v3.csv.
    """
    logger.info("=" * 60)
    logger.info("STEP 5: Matching 340B enrollment flags")
    logger.info("=" * 60)

    import numpy as np
    import pandas as pd
    import re as _re
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    # ── Load data ─────────────────────────────────────────────────────────────
    logger.info("  Loading Final_Hospital_Dataset…")
    sst_df = pd.read_csv(path_final, dtype=str, low_memory=False)
    logger.info(f"  SST rows: {len(sst_df):,}")

    logger.info("  Loading 340B entity file…")
    entity_df = pd.read_excel(path_340b, dtype=str)
    logger.info(f"  340B rows: {len(entity_df):,}")

    # ── Text normalization ────────────────────────────────────────────────────
    NOISE = re.compile(r"\b(hospital|center|medical|health|system|regional|community)\b")

    def normalize(text):
        if pd.isna(text):
            return ""
        text = str(text).lower()
        text = re.sub(r"[^\w\s]", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def build_sst_text(row):
        return normalize(f"{row.get('Hospital_Name','')} "
                         f"{row.get('City','')} {row.get('State','')} "
                         f"{row.get('Address','')}")

    def build_entity_text(row):
        return normalize(f"{row.get('Entity Name','')} "
                         f"{row.get('Street City','')} {row.get('Street State','')} "
                         f"{row.get('Street Address 1','')}")

    sst_texts    = [build_sst_text(row)    for _, row in sst_df.iterrows()]
    entity_texts = [build_entity_text(row) for _, row in entity_df.iterrows()]

    # Strip high-frequency noise words before TF-IDF
    sst_texts    = [NOISE.sub("", t).strip() for t in sst_texts]
    entity_texts = [NOISE.sub("", t).strip() for t in entity_texts]

    # ── TF-IDF shortlist ──────────────────────────────────────────────────────
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
        sims = cosine_similarity(sst_mat[i: i + chunk], entity_mat)
        for row_sims in sims:
            top_k = row_sims.argsort()[-TFIDF_TOP_K:][::-1].tolist()
            candidates.append(top_k)

    # ── Embedding rerank via OpenAI (optional) ────────────────────────────────
    # If OPENAI_API_KEY is not set, we skip embedding and use TF-IDF score alone.
    api_key = os.environ.get("OPENAI_API_KEY")
    EMBED_THRESHOLD = 0.82

    labels: list[int] = []

    if api_key:
        logger.info("  OPENAI_API_KEY found — using embedding rerank…")
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        def embed_batch(texts: list[str]) -> list:
            resp = client.embeddings.create(
                model="text-embedding-3-small",
                input=texts,
            )
            return [e.embedding for e in resp.data]

        # Build entity name-only embeddings once
        entity_name_texts = [normalize(str(row.get("Entity Name", "")))
                             for _, row in entity_df.iterrows()]
        sst_name_texts    = [normalize(str(row.get("Hospital_Name", "")))
                             for _, row in sst_df.iterrows()]

        entity_embeddings = []
        for i in range(0, len(entity_name_texts), 512):
            entity_embeddings.extend(embed_batch(entity_name_texts[i:i+512]))
            time.sleep(0.05)

        import numpy as np_
        entity_embed_mat = np_.array(entity_embeddings)

        for i, (sst_row, cand_indices) in enumerate(zip(sst_df.itertuples(), candidates)):
            if not cand_indices:
                labels.append(0)
                continue

            sst_emb = embed_batch([sst_name_texts[i]])[0]
            sst_vec = np_.array(sst_emb).reshape(1, -1)
            cand_mat = entity_embed_mat[cand_indices]

            sims = cosine_similarity(sst_vec, cand_mat)[0]
            best_sim = sims.max()
            labels.append(1 if best_sim >= EMBED_THRESHOLD else 0)

            if i % 500 == 0:
                logger.info(f"    Matched {i}/{len(sst_df):,}…")
            time.sleep(0.05)

    else:
        logger.info(
            "  OPENAI_API_KEY not set — using TF-IDF score only "
            "(slightly lower accuracy; set secret for full matching)."
        )
        TFIDF_THRESHOLD = 0.40
        for i, cand_indices in enumerate(candidates):
            if not cand_indices:
                labels.append(0)
                continue
            chunk_start = (i // chunk) * chunk
            row_idx = i - chunk_start
            sims = cosine_similarity(
                sst_mat[i: i + 1], entity_mat[cand_indices]
            )[0]
            labels.append(1 if sims.max() >= TFIDF_THRESHOLD else 0)

    # ── Save output ───────────────────────────────────────────────────────────
    sst_df["Is_340B_Enrolled"] = labels
    enrolled_count = sum(labels)
    logger.info(f"  340B enrolled: {enrolled_count:,} / {len(sst_df):,} rows")

    sst_df.to_csv(SST_V3_PATH, index=False)
    logger.info(f"  ✓ SST_v3.csv → {SST_V3_PATH}")
    return SST_V3_PATH


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Archive outputs
# ─────────────────────────────────────────────────────────────────────────────

def archive_outputs():
    """Copy SST_v3.csv to archive with a timestamp suffix."""
    today = date.today().strftime("%Y%m%d")
    archive_path = ARCHIVE_DIR / f"SST_v3_{today}.csv"
    shutil.copy2(SST_V3_PATH, archive_path)
    logger.info(f"  Archived: {archive_path}")

    # Write a pipeline run manifest
    manifest = DATA_OUT / "pipeline_manifest.txt"
    with manifest.open("w") as f:
        f.write(f"Pipeline run: {datetime.now().isoformat()}\n")
        f.write(f"SST_v3.csv rows: {_count_csv_rows(SST_V3_PATH):,}\n")
        f.write(f"Archive: {archive_path.name}\n")
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
        logger.info("  Would run build_dataset.py → Final_Hospital_Dataset.csv")
        logger.info("  Would run 340B matching → SST_v3.csv")
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
        file_paths = {"cms": cms_paths, "path_340b": path_340b, "path_nashp": path_nashp}

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

    # ── Archive ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 6: Archiving outputs")
    logger.info("=" * 60)
    archive_outputs()

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.0f}s")
    logger.info(f"  Output: {SST_V3_PATH}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
