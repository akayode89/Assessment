"""
tasks/transform.py
==================
Step 2 – Transformation
-----------------------
Reads raw CSVs from landing/, apply business logic to clean and enrich the data

Staged outputs
--------------
  staged_files/clean_subscribers.csv      – cleaned subscriber dimension
  staged_files/plan_price_history.csv     – SCD Type 2 plan pricing table
  staged_files/enriched_subscriptions.csv – fully enriched subscription facts

Archive output (on success only)
---------------------------------
  processed_file_archive/<YYYY-MM-DD_HH-MM-SS>/
      subscribers.csv, plans.csv, subscriptions.csv

Usage
-----
    python tasks/transform.py
"""

import logging
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tasks.config import (
    LANDING_DIR,
    STAGED_DIR,
    ARCHIVE_DIR,
    STAGED_FILES,
    EXPECTED_FILES,
    ensure_dirs,
    get_spark,
    write_single_csv,
)

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [TRANSFORM]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS  (shared column-level transformations)
# ─────────────────────────────────────────────────────────────────────────────

def country_expr(col_name: str) -> F.Column:
    """
    Pure Catalyst expression: maps raw country strings to canonical names.
    NULL / empty → "Unknown". Runs entirely inside the JVM – no UDF overhead.
    """
    c = F.trim(F.col(col_name))
    return (
        F.when(c.isNull() | (c == ""), F.lit("Unknown"))
        .when(F.lower(c).isin("usa", "us", "united states"), F.lit("United States"))
        .when(F.lower(c).isin("u.k.", "england"),            F.lit("United Kingdom"))
        .when(F.lower(c).isin("canada", "can"),              F.lit("Canada"))
        .otherwise(c)
    )


@F.udf(returnType=StringType())
def normalise_phone_udf(val):
    """Reformat phone to XXX-XXX-XXXX; returns None when not exactly 10 digits."""
    if not val:
        return None
    digits = re.sub(r"\D", "", str(val))
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else None


@F.udf(returnType=BooleanType())
def is_valid_email_udf(val):
    """Basic RFC-style email pattern check."""
    if not val or str(val).strip() == "":
        return False
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(val).strip()))


# def write_single_csv(df, path: Path) -> None:
#     """
#     Coalesce to 1 partition and write a clean single-file CSV.
#     Spark writes to a temp directory first then the part file is renamed.
#     """
#     import os
#     import glob, shutil
#
#     os.environ.get("HADOOP_HOME")
#     os.popen("where winutils").read()
#     tmp = str(path) + "_tmp"
#     df.printSchema()
#     print(glob.glob(f"{tmp}/*"))
#     try:
#         df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         raise
#     parts = glob.glob(f"{tmp}/part-*.csv")
#     print(glob.glob(f"{tmp}/*"))
#     if parts:
#         if os.path.exists(str(path)):
#             os.remove(str(path))
#         shutil.move(parts[0], str(path))
#     shutil.rmtree(tmp, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMATION STEPS
# ─────────────────────────────────────────────────────────────────────────────

def clean_subscribers(subscribers_raw):
    """
    Cleaning Subscriber raw
    Output:
    """
    log.info("Validating emails …")
    subs = subscribers_raw.withColumn("email_valid", is_valid_email_udf(F.col("email")))
    bad  = [r.subscriber_id for r in subs.filter(~F.col("email_valid")).select("subscriber_id").collect()]
    log.info(f" Dropping invalid-email rows: {bad}")
    subs = subs.filter(F.col("email_valid")).drop("email_valid")

    log.info("Remove duplicate email (keep earliest signup_date) …")
    subs = subs.withColumn("signup_date", F.to_date("signup_date"))
    w    = Window.partitionBy("email").orderBy(F.col("signup_date").asc())
    subs = subs.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    log.info(f" After dedup: {subs.count()} rows")

    log.info("Filling missing name/country …")
    subs = (
        subs
        .withColumn("name",    F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""), F.lit("Unknown")).otherwise(F.col("name")))
        .withColumn("country", country_expr("country"))
    )

    log.info("Normalising phone numbers …")
    subs_df = subs.withColumn("phone", normalise_phone_udf(F.col("phone")))

    log.info(f" Clean subscribers: {subs_df.count()} rows")
    return subs_df


def clean_plans(plans_raw):
    """
    Cleaning plans raw
    Output: plan_history_df, current_prices_df
    """
    log.info("Removing rows with null/blank plan_id …")
    plans = plans_raw.filter(F.col("plan_id").isNotNull() & (F.trim(F.col("plan_id")) != ""))
    log.info(f" Rows after plan_id filter: {plans.count()}")

    log.info("Removing rows with null or negative price …")
    plans = plans.filter(F.col("price_per_month").isNotNull() & (F.col("price_per_month") >= 0))
    log.info(f" Rows after price filter: {plans.count()}")

    log.info("Parsing effective_date / end_date (SCD sentinel = 9999-12-31) …")
    plans = (
        plans
        .withColumn("effective_date",
                    F.coalesce(
                        F.to_date(F.col("effective_date"), "M/d/yyyy"),
                        F.to_date(F.col("effective_date"), "yyyy-MM-dd")
                    ))
        .withColumn("end_date",
                    F.when(
                        F.col("end_date").isNull(),
                        F.lit("9999-12-31").cast("date")
                    ).otherwise(
                        F.coalesce(
                            F.to_date(F.col("end_date"), "M/d/yyyy"),
                            F.to_date(F.col("end_date"), "yyyy-MM-dd")
                        )
                    ))
        .withColumn("is_active",
                    F.when(F.col("end_date") >= F.current_date(), F.lit('Y')).otherwise(F.lit('N'))
                    )
    )

    plan_history_df = plans.select("plan_id", "plan_tier", "price_per_month", "effective_date", "end_date", "is_active")

    # Current price = row with the latest effective_date per plan
    w = Window.partitionBy("plan_id").orderBy(F.col("effective_date").desc())
    current_prices_df = (
        plan_history_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(F.col("plan_id").alias("cp_plan_id"), F.col("price_per_month").alias("current_price"))
    )

    log.info(f" SCD plan_history rows: {plan_history_df.count()}")
    return plan_history_df, current_prices_df


def clean_subscriptions(sub_raw, subs_df, plan_history_df):
    """
    Functionality: Subscription data clean up
    Output: final_sub_df
    """
    sub = sub_raw

    log.info("Filling missing monthly_charge with 0 …")
    sub = sub.withColumn("monthly_charge", F.coalesce(F.col("monthly_charge").cast("double"), F.lit(0.0)))

    log.info("Parsing subscription dates …")
    sub = (
        sub
        .withColumn("subscription_start_date", F.to_date(F.col("subscription_start_date"), "M/d/yyyy"))
        .withColumn("subscription_end_date",   F.to_date(F.col("subscription_end_date"),   "M/d/yyyy"))
    )

    log.info("Removing orphan subscriber_ids …")
    valid_sub_ids = subs_df.select("subscriber_id")
    dropped = [r.subscription_id for r in sub.join(valid_sub_ids, "subscriber_id", "left_anti").select("subscription_id").collect()]
    log.info(f" Dropped: {dropped}")
    sub = sub.join(valid_sub_ids, "subscriber_id", "inner")

    log.info("Removing orphan plan_ids …")
    valid_plan_ids = plan_history_df.select("plan_id").distinct()
    dropped = [r.subscription_id for r in sub.join(valid_plan_ids, "plan_id", "left_anti").select("subscription_id").collect()]
    log.info(f" Dropped: {dropped}")
    sub = sub.join(valid_plan_ids, "plan_id", "inner")

    log.info("Removing cancelled subscriptions …")
    n_cancelled = sub.filter(F.col("status") == "cancelled").count()
    log.info(f" Dropped {n_cancelled} cancelled rows")
    sub = sub.filter(F.col("status") != "cancelled")

    log.info("Normalising billing_country …")
    final_sub_df = sub.withColumn("billing_country", country_expr("billing_country"))

    log.info(f" Clean subscriptions: {final_sub_df.count()} rows")
    return final_sub_df


def resolve_scd_prices(final_sub_df, plan_history_df, current_prices_df):
    """
    Resolve pricing discrepancies for subscriptions
    Output: sub_priced_df
    """
    log.info("SCD Type 2 range-join: resolving historically accurate prices …")
    ph = plan_history_df.select(
        F.col("plan_id").alias("ph_plan_id"),
        F.col("plan_tier"),
        F.col("price_per_month").alias("actual_monthly_price"),
        F.col("effective_date"),
        F.col("end_date"),
    )

    sub_priced = final_sub_df.join(
        F.broadcast(ph),
        on=(
                (F.col("plan_id") == F.col("ph_plan_id")) &
                (F.col("subscription_start_date") >= F.col("effective_date")) &
                (F.col("subscription_start_date") <= F.col("end_date"))
        ),
        how="left",
    ).drop("ph_plan_id", "effective_date", "end_date")

    sub_priced_df = sub_priced.join(
        F.broadcast(current_prices_df),
        on=(F.col("plan_id") == F.col("cp_plan_id")),
        how="left",
    ).drop("cp_plan_id")

    unresolved = sub_priced_df.filter(F.col("actual_monthly_price").isNull()).count()
    if unresolved:
        log.warning(f"  {unresolved} subscription(s) have no resolvable historical price!")
    else:
        log.info(f" All {sub_priced_df.count()} subscriptions have a resolved historical price")

    return sub_priced_df


def enrich_and_join(sub_priced_df, subs_df):
    """
    Build master dataset with subscriber, plans, and subscription
    Output: enriched_df
    """
    log.info("Calculate total_subscriptions per subscriber …")
    w = Window.partitionBy("subscriber_id")
    sub_priced = sub_priced_df.withColumn("total_subscriptions", F.count("subscription_id").over(w))

    log.info("Identify long-term subscribers (> 183 days) …")
    sub_priced = (
        sub_priced
        .withColumn("end_for_calc", F.coalesce(F.col("subscription_end_date"), F.current_date()))
        .withColumn("duration_days", F.datediff(F.col("end_for_calc"), F.col("subscription_start_date")))
        .withColumn("long_term_subscriber", F.col("duration_days") > 183)
        .drop("end_for_calc")
    )

    log.info("Derive subscription year_month (YYYY-MM) …")
    sub_priced = sub_priced.withColumn("subscription_month", F.date_format("subscription_start_date", "yyyy-MM").cast("string")
                                       )

    log.info("Calculating price_variance …")
    sub_priced = sub_priced.withColumn("price_variance", F.round(F.col("actual_monthly_price") - F.col("current_price"), 2))

    log.info("Inner joining enriched subscriptions to subscribers …")
    enriched_df = sub_priced.join(
        subs_df.select("subscriber_id", "name", "email", "country", "age", "signup_date"),
        on="subscriber_id",
        how="inner",
    )
    log.info(f" Enriched fact rows: {enriched_df.count()}")
    return enriched_df


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    ensure_dirs()
    spark = get_spark("StreamFlix-Transform")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 65)
    log.info("TRANSFORM  –  START")
    log.info(f"Input  : {LANDING_DIR}")
    log.info(f"Output : {STAGED_DIR}")
    log.info("=" * 65)

    # ── Load from landing ────────────────────────────────────────────────────
    log.info("Loading raw CSVs from landing/ …")
    subscribers_raw = spark.read.csv(str(LANDING_DIR / "subscribers.csv"),   header=True, inferSchema=True)
    plans_raw = spark.read.csv(str(LANDING_DIR / "plans.csv"),         header=True, inferSchema=True)
    subscriptions_raw = spark.read.csv(str(LANDING_DIR / "subscriptions.csv"), header=True, inferSchema=True)
    log.info(f"subscribers={subscribers_raw.count()}  plans={plans_raw.count()}  subscriptions={subscriptions_raw.count()}")

    # ── Transform ────────────────────────────────────────────────────────────
    log.info("\nCleaning subscribers …")
    subs = clean_subscribers(subscribers_raw)

    log.info("\nCleaning plans (SCD dimension) …")
    plan_history, current_prices = clean_plans(plans_raw)

    log.info("\nCleaning subscriptions …")
    sub = clean_subscriptions(subscriptions_raw, subs, plan_history)

    log.info("\nResolving SCD Type 2 prices …")
    sub_priced = resolve_scd_prices(sub, plan_history, current_prices)

    log.info("\nEnrichment + master join …")
    enriched = enrich_and_join(sub_priced, subs)

    # ── Write staged files ───────────────────────────────────────────────────
    log.info("\nWriting staged files …")
    write_single_csv(subs,         STAGED_DIR / STAGED_FILES["clean_subscribers"])
    write_single_csv(plan_history, STAGED_DIR / STAGED_FILES["plan_price_history"])
    write_single_csv(enriched,     STAGED_DIR / STAGED_FILES["enriched_subscriptions"])
    log.info(f"clean_subscribers.csv       → staged_files/")
    log.info(f"plan_price_history.csv      → staged_files/")
    log.info(f"enriched_subscriptions.csv  → staged_files/")

    # ── Archive landing files ─────────────────────────────────────────────────
    # Only reached if ALL staged writes above succeeded.
    # Moving landing files to archive here prevents re-processing on the next run
    # while guaranteeing we never archive a file that failed to transform.
    log.info("\nArchiving consumed landing files …")
    archive_path = archive_landing()
    log.info(f"Landing files archived to: {archive_path}")

    log.info("=" * 65)
    log.info("TRANSFORM  –  COMPLETE")
    log.info("=" * 65)

    spark.stop()


def archive_landing() -> Path:
    """
    Move all processed files from landing/ into a timestamped subfolder
    under processed_file_archive/.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_run = ARCHIVE_DIR / timestamp
    archive_run.mkdir(parents=True, exist_ok=True)

    for fname in EXPECTED_FILES:
        src = LANDING_DIR / fname
        if src.exists():
            shutil.move(str(src), archive_run / fname)
            log.info(f"Moved  landing/{fname}  →  processed_file_archive/{timestamp}/")
        else:
            log.warning(f"landing/{fname} not found during archive – skipping")

    return archive_run


if __name__ == "__main__":
    run()
