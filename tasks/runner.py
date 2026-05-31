"""
tasks/runner.py
===============
Pipeline Orchestrator
---------------------
Runs the three pipeline stages in the correct dependency order:

    [1] ingestion.py   →  SOURCE → landing/             (source originals untouched)
    [2] transform.py   →  landing/ → staged_files/      (archives landing/ on success)
    [3] analytics.py   →  staged_files/ → analytics/

Each stage is imported and called as a Python function (no subprocess),
which means a single SparkSession lifecycle is respected and any failure
in a stage immediately halts the pipeline with a clear error message.

Usage
-----
    # Full pipeline with default source (C:/Users/ajkay/Downloads)
    python tasks/runner.py

    # Override source directory
    python tasks/runner.py --source "D:/data/raw"

    # Run only specific stages (useful for reruns/debugging)
    python tasks/runner.py --stages ingest transform
    python tasks/runner.py --stages transform analytics
    python tasks/runner.py --stages analytics

Flags
-----
    --source PATH     Source directory for raw CSVs (default: C:/Users/ajkay/Downloads)
    --stages STAGES   Space-separated subset of: ingest transform analytics
                      (default: run all three in order)

Exit codes
----------
    0  – all selected stages completed successfully
    1  – a stage failed; error is logged with the failing stage name
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tasks.config import DEFAULT_SOURCE_DIR

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [RUNNER]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# STAGE DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────

STAGE_ORDER = ["ingest", "transform", "analytics"]

STAGE_DESCRIPTIONS = {
    "ingest":    "Ingest raw CSVs from source → landing/",
    "transform": "Clean, enrich & SCD-price  landing/ → staged_files/  + archive landing/",
    "analytics": "Aggregate analytics tables  staged_files/ → analytics/",
}


def _run_ingest(source_dir: Path) -> None:
    from tasks.ingestion import run as ingest_run
    ingest_run(source_dir=source_dir)


def _run_transform(_source_dir) -> None:
    from tasks.transform import run as transform_run
    transform_run()


def _run_analytics(_source_dir) -> None:
    from tasks.analytics import run as analytics_run
    analytics_run()


STAGE_FNS = {
    "ingest":    _run_ingest,
    "transform": _run_transform,
    "analytics": _run_analytics,
}


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run(source_dir: Path, stages: list[str]) -> None:
    # Validate stage names
    unknown = [s for s in stages if s not in STAGE_ORDER]
    if unknown:
        log.error(f"Unknown stage(s): {unknown}. Valid stages are: {STAGE_ORDER}")
        sys.exit(1)

    # Preserve correct dependency order even if user supplied stages out of order
    ordered = [s for s in STAGE_ORDER if s in stages]

    log.info("╔" + "═" * 63 + "╗")
    log.info("║           SUNTEX-MARINA  PIPELINE  RUNNER                    ║")
    log.info("╠" + "═" * 63 + "╣")
    log.info(f"║  Source   : {str(source_dir):<50} ║")
    log.info(f"║  Stages   : {', '.join(ordered):<50} ║")
    log.info("╚" + "═" * 63 + "╝")

    pipeline_start = time.time()
    results        = {}

    for stage in ordered:
        log.info("")
        log.info("┌" + "─" * 63 + "┐")
        log.info(f"│  STAGE: {stage.upper():<54} │")
        log.info(f"│  {STAGE_DESCRIPTIONS[stage]:<61} │")
        log.info("└" + "─" * 63 + "┘")

        stage_start = time.time()
        try:
            STAGE_FNS[stage](source_dir)
            elapsed = time.time() - stage_start
            results[stage] = ("✔  SUCCESS", elapsed)
            log.info(f"  Stage '{stage}' completed in {elapsed:.1f}s")

        except Exception as exc:
            elapsed = time.time() - stage_start
            results[stage] = ("✘  FAILED", elapsed)
            log.error(f"  Stage '{stage}' FAILED after {elapsed:.1f}s")
            log.error(f"  Error: {exc}", exc_info=True)

            # Print summary up to failure point then exit
            _print_summary(results, pipeline_start)
            sys.exit(1)

    _print_summary(results, pipeline_start)


def _print_summary(results: dict, pipeline_start: float) -> None:
    total = time.time() - pipeline_start
    log.info("")
    log.info("╔" + "═" * 63 + "╗")
    log.info("║                    PIPELINE SUMMARY                          ║")
    log.info("╠" + "═" * 63 + "╣")
    for stage, (status, elapsed) in results.items():
        log.info(f"║  {stage:<12}  {status:<20}  {elapsed:>6.1f}s               ║")
    log.info("╠" + "═" * 63 + "╣")
    log.info(f"║  Total time : {total:.1f}s{' ' * 47}║")
    log.info("╚" + "═" * 63 + "╝")


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Suntex-Marina Pipeline Runner – orchestrates ingest → transform → analytics"
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"Source directory for raw CSVs (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=STAGE_ORDER,
        default=STAGE_ORDER,
        metavar="STAGE",
        help=f"Stages to run, in order. Choices: {STAGE_ORDER} (default: all)",
    )
    args = parser.parse_args()

    run(source_dir=args.source, stages=args.stages)
