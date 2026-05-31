"""
tasks/ingestion.py
==================
Step 1 – Ingestion
------------------
This process ingest raw CSV files from the source storage into the landing/ zone.
The source originals are left untouched at this stage.

Usage
-----
    # The default storage source (C:/Users/ajkay/Downloads/Suntex/input_files)
    python tasks/ingestion.py

    # Override source at runtime
    python tasks/ingestion.py --source "D:/data/raw"

What it does
------------
  1. Validates that all EXPECTED_FILES are present in the source storage.
  2. Copies each file to landing/ (overwrites if already present).
  3. Logs a clear summary of every action taken.
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path
import hashlib

# ── Allow running from project root or from tasks/ directly ─────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tasks.config import (
    DEFAULT_SOURCE_DIR,
    LANDING_DIR,
    EXPECTED_FILES,
    ensure_dirs,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [INGEST]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CORE LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def validate_source(source_dir: Path) -> list[Path]:
    """
    Check that every expected file exists in source_dir.
    Returns list of resolved source Paths.
    Raises FileNotFoundError if any file is missing.
    """
    missing = []
    found   = []
    for fname in EXPECTED_FILES:
        fpath = source_dir / fname
        if fpath.exists():
            found.append(fpath)
            log.info(f"  ✔  Found: {fpath}")
        else:
            missing.append(fname)
            log.warning(f"  ✘  Missing: {fpath}")

    if missing:
        raise FileNotFoundError(
            f"Ingestion aborted – the following files were not found in {source_dir}:\n"
            + "\n".join(f"  • {f}" for f in missing)
        )
    return found

def file_hash(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def copy_to_landing(source_files: list[Path]) -> list[Path]:
    """
    Copy each file from source into landing/.
    Source originals are preserved; archiving is deferred to transform.py.
    Returns list of landed destination Paths.
    """
    landed = []

    for src in source_files:
        dest = LANDING_DIR / src.name

        # Check if the same file is being ingested
        if dest.exists() and file_hash(dest) == file_hash(src):
            log.info(f"  Skipped {src.name} (duplicate content)")
            continue

        shutil.copy2(src, dest)
        log.info(f"  Copied  {src.name}  →  landing/")
        landed.append(dest)

    return landed


def run(source_dir: Path) -> None:
    """Ingestion: validate source → copy to landing/. Archive deferred to transform."""
    ensure_dirs()

    log.info("=" * 60)
    log.info("INGESTION  –  START")
    log.info(f"  Source  : {source_dir}")
    log.info(f"  Landing : {LANDING_DIR}")
    log.info("=" * 60)

    log.info("Step 1/2  Validating source files …")
    source_files = validate_source(source_dir)

    log.info("Step 2/2  Copying files to landing/ …")
    copy_to_landing(source_files)

    log.info("=" * 60)
    log.info("INGESTION  –  COMPLETE")
    log.info(f"  {len(source_files)} file(s) landed : {LANDING_DIR}")
    log.info("  Source originals preserved    : archive happens after transform")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="StreamFlix Ingestion – copy raw CSVs from source storage to landing/"
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"Storage containing raw CSV files (default: {DEFAULT_SOURCE_DIR})",
    )
    args = parser.parse_args()

    try:
        run(source_dir=args.source)
    except FileNotFoundError as exc:
        log.error(str(exc))
        sys.exit(1)
