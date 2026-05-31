"""
tasks/analytics.py
==================
Step 3 – Analytics
------------------
Reads the enriched intermediate files from staged_files/ and produces
two final reporting tables written to analytics/.
    Report:
        analytics/subscriber_summary.csv on billing_country and plan_tier
        analytics/monthly_revenue.csv on subscription_month, billing_country, and plan_tier
Usage
-----
    python tasks/analytics.py

"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tasks.config import (
    STAGED_DIR,
    ANALYTICS_DIR,
    STAGED_FILES,
    ANALYTICS_FILES,
    ensure_dirs,
    get_spark,
    write_single_csv,
)

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [ANALYTICS]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

# def write_single_csv(df, path: Path) -> None:
#     """Coalesce to 1 partition and write a clean single-file CSV."""
#     import os
#     import glob, shutil
#
#     os.environ.get("HADOOP_HOME")
#     os.popen("where winutils").read()
#
#     tmp = str(path) + "_tmp"
#     df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
#     parts = glob.glob(f"{tmp}/part-*.csv")
#     if parts:
#         shutil.move(parts[0], str(path))
#     shutil.rmtree(tmp, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS TABLES
# ─────────────────────────────────────────────────────────────────────────────

def build_subscriber_summary(enriched):
    """
    subscriber_summary
    """
    return (
        enriched
        .groupBy("billing_country", "plan_tier")
        .agg(
            F.countDistinct("subscriber_id")          .alias("num_subscribers"),
            F.round(F.sum("actual_monthly_price"), 2) .alias("total_revenue"),
            F.round(F.avg("satisfaction_rating"),  2) .alias("avg_satisfaction"),
            F.count("subscription_id")                .alias("num_subscriptions"),
        )
        .orderBy("billing_country", "plan_tier")
    )


def build_monthly_revenue(enriched):
    """
    monthly_revenue
    """
    return (
        enriched
        .groupBy("subscription_month", "billing_country", "plan_tier")
        .agg(
            F.round(F.sum("actual_monthly_price"), 2) .alias("total_revenue"),
            F.count("subscription_id")                .alias("num_active_subscriptions"),
            F.round(F.avg("actual_monthly_price"), 2) .alias("avg_price_in_effect"),
        )
        .orderBy("subscription_month", "billing_country", "plan_tier")
    )


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    ensure_dirs()
    spark = get_spark("StreamFlix-Analytics")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 65)
    log.info("ANALYTICS  –  START")
    log.info(f"  Input  : {STAGED_DIR}")
    log.info(f"  Output : {ANALYTICS_DIR}")
    log.info("=" * 65)

    # ── Load enriched fact from staged ───────────────────────────────────────
    enriched_path = str(STAGED_DIR / STAGED_FILES["enriched_subscriptions"])
    log.info(f"Reading enriched_subscriptions from staged_files/ …")
    enriched_schema = StructType([
        StructField("subscription_id",         StringType(),  True),
        StructField("plan_id",                 StringType(),  True),
        StructField("subscriber_id",           StringType(),  True),
        StructField("subscription_start_date", DateType(),    True),
        StructField("subscription_end_date",   DateType(),    True),
        StructField("status",                  StringType(),  True),
        StructField("billing_country",         StringType(),  True),
        StructField("monthly_charge",          DoubleType(),  True),
        StructField("satisfaction_rating",     DoubleType(),  True),
        StructField("plan_tier",               StringType(),  True),
        StructField("actual_monthly_price",    DoubleType(),  True),
        StructField("current_price",           DoubleType(),  True),
        StructField("total_subscriptions",     IntegerType(), True),
        StructField("duration_days",           IntegerType(), True),
        StructField("long_term_subscriber",    BooleanType(), True),
        StructField("subscription_month",      StringType(),  True),
        StructField("price_variance",          DoubleType(),  True),
        StructField("name",                    StringType(),  True),
        StructField("email",                   StringType(),  True),
        StructField("country",                 StringType(),  True),
        StructField("age",                     IntegerType(), True),
        StructField("signup_date",             DateType(),    True),
    ])
    enriched = spark.read.csv(enriched_path, header=True, schema=enriched_schema)
    log.info(f"Loaded {enriched.count()} rows")

    # ── Build analytics tables ───────────────────────────────────────────────
    log.info("\nBuilding Table 1: subscriber_summary …")
    subscriber_summary = build_subscriber_summary(enriched)
    subscriber_summary.show(truncate=False)

    log.info("\nBuilding Table 2: monthly_revenue …")
    monthly_revenue = build_monthly_revenue(enriched)
    monthly_revenue.show(50, truncate=False)

    # ── Write to analytics/ ──────────────────────────────────────────────────
    log.info("\nWriting analytics outputs …")
    write_single_csv(subscriber_summary, ANALYTICS_DIR / ANALYTICS_FILES["subscriber_summary"])
    write_single_csv(monthly_revenue,    ANALYTICS_DIR / ANALYTICS_FILES["monthly_revenue"])
    log.info(f"  subscriber_summary.csv  → analytics/")
    log.info(f"  monthly_revenue.csv     → analytics/")

    log.info("=" * 65)
    log.info("ANALYTICS  –  COMPLETE")
    log.info("=" * 65)

    spark.stop()


if __name__ == "__main__":
    run()
