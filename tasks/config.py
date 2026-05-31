"""
tasks/config.py
===============
Central configuration for the Suntex-Marina pipeline.
All tasks import from here – change paths in one place only.

Project root is resolved at runtime relative to this file so the
config works regardless of where Python is invoked from.
"""

import os
from pathlib import Path

# ── Project root (one level up from tasks/) ─────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent   # …/Suntex-Marina/

# ── Pipeline zone directories ────────────────────────────────────────────────
LANDING_DIR      = PROJECT_ROOT / "landing"
STAGED_DIR       = PROJECT_ROOT / "staged_files"
ANALYTICS_DIR    = PROJECT_ROOT / "analytics"
ARCHIVE_DIR      = PROJECT_ROOT / "processed_file_archive"   # post-transform archive (written by transform.py on success)

# ── Default source (overridable via CLI --source flag) ───────────────────────
DEFAULT_SOURCE_DIR = Path("C:/Users/ajkay/Downloads/Suntex/input_files")

# ── Expected input files ─────────────────────────────────────────────────────
EXPECTED_FILES = [
    "subscribers.csv",
    "plans.csv",
    "subscriptions.csv",
]

# ── Staged file names (written by transform, read by analytics) ──────────────
STAGED_FILES = {
    "clean_subscribers":    "clean_subscribers.csv",
    "plan_price_history":   "plan_price_history.csv",
    "enriched_subscriptions": "enriched_subscriptions.csv",
}

# ── Analytics output file names ───────────────────────────────────────────────
ANALYTICS_FILES = {
    "subscriber_summary": "subscriber_summary.csv",
    "monthly_revenue":    "monthly_revenue.csv",
}


def get_spark(app_name: str):
    """
    Create (or reuse) a SparkSession with consistent settings.
    Single-node defaults; tune spark.sql.shuffle.partitions for cluster use.
    """
    import os
    import sys

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def ensure_dirs():
    """Create all pipeline zone directories if they don't already exist."""
    for d in [LANDING_DIR, STAGED_DIR, ANALYTICS_DIR, ARCHIVE_DIR]:
        d.mkdir(parents=True, exist_ok=True)

def write_single_csv(df, path: Path) -> None:
    """
    Coalesce to 1 partition and write a clean single-file CSV.
    Spark writes to a temp directory first then the part file is renamed.
    """
    import os
    import glob, shutil

    os.environ.get("HADOOP_HOME")
    os.popen("where winutils").read()
    tmp = str(path) + "_tmp"
    df.printSchema()
    print(glob.glob(f"{tmp}/*"))
    try:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise
    parts = glob.glob(f"{tmp}/part-*.csv")
    print(glob.glob(f"{tmp}/*"))
    if parts:
        if os.path.exists(str(path)):
            os.remove(str(path))
        shutil.move(parts[0], str(path))
    shutil.rmtree(tmp, ignore_errors=True)