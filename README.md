# Assessment Data Pipeline

A PySpark-based data pipeline for StreamFlix that ingests raw subscriber, plan, and subscription data. Applies SCD Type 2 to pricing, fully clean and enrich the data to produce two tables for analytic purpose.

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Pipeline Overview](#2-pipeline-overview)
3. [Data Flow](#3-data-flow)
4. [File Reference](#4-file-reference)
   - [config.py](#configpy)
   - [ingestion.py](#ingestionpy)
   - [transform.py](#transformpy)
   - [analytics.py](#analyticspy)
   - [runner.py](#runnerpy)
5. [Business Logic](#5-business-logic)
   - [Subscriber Cleaning](#subscriber-cleaning-step-2)
   - [Plan Cleaning & SCD Type 2](#plan-cleaning--scd-type-2-step-3)
   - [Subscription Cleaning](#subscription-cleaning-step-4)
   - [SCD Price Resolution](#scd-type-2-price-resolution-step-5)
   - [Enrichment & Master Join](#enrichment--master-join-steps-6--7)
6. [Analytics Outputs](#6-analytics-outputs)
7. [Archive Strategy](#7-archive-strategy)
8. [How to Run](#8-how-to-run)
9. [Design Decisions](#10-design-decisions)

---

## 1. Project Structure

```
Assessment/
├── analytics/                        ← Final reporting CSVs (written by analytics.py)
│   ├── subscriber_summary.csv
│   └── monthly_revenue.csv
│
├── landing/                          ← Raw CSVs copied here by ingestion.py
│   ├── subscribers.csv
│   ├── plans.csv
│   └── subscriptions.csv
│
├── staged_files/                     ← Intermediate outputs written by transform.py
│   ├── clean_subscribers.csv
│   ├── plan_price_history.csv
│   └── enriched_subscriptions.csv
│
├── processed_file_archive/           ← Landing files archived here after successful transform
│   └── YYYY-MM-DD_HH-MM-SS/
│       ├── subscribers.csv
│       ├── plans.csv
│       └── subscriptions.csv
│
├── tasks/
│    ├── config.py                     ← Central config: all paths, SparkSession factory
│    ├── ingestion.py                  ← Stage 1: source → landing/
│    ├── transform.py                  ← Stage 2: landing/ → staged_files/ + archive
│    ├── analytics.py                  ← Stage 3: staged_files/ → analytics/
│    └── runner.py                     ← Orchestrator: runs all stages in order
└──PIPELINE_DOC.md
```

---

## 2. Pipeline Overview

The pipeline is split into three independent executable staps:

| Step              | File | Responsibility |
|-------------------|------|----------------|
| **1 – Ingest**    | `ingestion.py` | Validate source files exist; copy to `landing/` |
| **2 – Transform** | `transform.py` | Clean, enrich, SCD-price; write to `staged_files/`; archive `landing/` |
| **3 – Analytics** | `analytics.py` | Aggregate enriched facts; write final CSVs to `analytics/` |

`runner.py` orchestrates all steps in sequence, failing fast with a summary if any stage errors.

---

## 3. Data Flow

```
C:/Users/xxxx/Downloads/          (or --source override)
    subscribers.csv
    plans.csv
    subscriptions.csv
         │
         │  ingestion.py: validate + copy (source originals untouched)
         ▼
    landing/
         │
         │  transform.py: clean → SCD price → enrich → join
         ▼
    staged_files/
    ├── clean_subscribers.csv
    ├── plan_price_history.csv
    └── enriched_subscriptions.csv
         │
         │  transform.py: archive_landing() — only after all staged writes succeed
         ▼
    processed_file_archive/YYYY-MM-DD_HH-MM-SS/
         │
         │  analytics.py: aggregate
         ▼
    analytics/
    ├── subscriber_summary.csv
    └── monthly_revenue.csv
```

> **Archive timing** — landing files are moved to `processed_file_archive/` by `transform.py` as its very last action, only after every staged write has succeeded. If the transform fails at any point, `landing/` is left intact for a safe retry.

---

## 4. File Reference

### `config.py`

Central configuration module. Every other task imports from here — paths are never hardcoded elsewhere.

**Key exports:**

| Name | Type | Description                                                                     |
|------|------|---------------------------------------------------------------------------------|
| `PROJECT_ROOT` | `Path` | Resolved at runtime from `config.py` location; works from any working directory |
| `LANDING_DIR` | `Path` | `<project_root>/landing`                                                        |
| `STAGED_DIR` | `Path` | `<project_root>/staged_files`                                                   |
| `ANALYTICS_DIR` | `Path` | `<project_root>/analytics`                                                      |
| `ARCHIVE_DIR` | `Path` | `<project_root>/processed_file_archive` — post-transform archive                |
| `DEFAULT_SOURCE_DIR` | `Path` | `C:/Users/xxxx/Downloads`                                                       |
| `EXPECTED_FILES` | `list[str]` | `["subscribers.csv", "plans.csv", "subscriptions.csv"]`                         |
| `STAGED_FILES` | `dict` | Maps logical names to staged filenames                                          |
| `ANALYTICS_FILES` | `dict` | Maps logical names to analytics filenames                                       |
| `get_spark(app_name)` | `func` | Returns a configured `SparkSession`                                             |
| `ensure_dirs()` | `func` | Creates all zone directories if they don't exist                                |
| `write_single_csv()` | `func` | Write a single file to target and assign appropriate name on mapping            |

**SparkSession settings:**

```python
spark.sql.shuffle.partitions = 8    # suitable for single-node; increase for cluster
spark.driver.memory           = 2g
```

---

### `ingestion.py`

**Step 1.** Copies raw CSVs from the source directory into `landing/`. Source originals are never moved or deleted at this stage.

**Functions:**

| Function | Description |
|----------|-------------|
| `validate_source(source_dir)` | Checks all `EXPECTED_FILES` exist in `source_dir`; raises `FileNotFoundError` listing any missing files |
| `copy_to_landing(source_files)` | Copies each file to `landing/` using `shutil.copy2` (preserves metadata timestamps) |
| `run(source_dir)` | Orchestrates validate → copy; callable from `runner.py` or as `__main__` |

**CLI:**
```bash
python tasks/ingestion.py
python tasks/ingestion.py --source "D:/data/raw"
```

---

### `transform.py`

**Step 2.** The core of the pipeline. Reads from `landing/`, applies all cleaning and enrichment logic, writes three intermediate files to `staged_files/`, and only then archives the consumed `landing/` files.

**Helper functions:**

| Function | Description |
|----------|-------------|
| `country_expr(col_name)` | Pure Catalyst `when/otherwise` chain — no UDF overhead; maps raw country strings to canonical names |
| `normalise_phone_udf(val)` | PySpark UDF — strips non-digits, reformats to `XXX-XXX-XXXX`; returns `None` if not 10 digits |
| `is_valid_email_udf(val)` | PySpark UDF — basic RFC-style regex email check |

**Transformation functions:**

| Function | Steps covered |
|----------|---------------|
| `clean_subscribers(subscribers_raw)` | email validation, dedup, fill nulls, phone normalisation |
| `clean_plans(plans_raw)` | drop invalid plan_id/price, parse SCD dates, build `plan_history` and `current_prices` |
| `clean_subscriptions(sub_raw, subs, plan_history)` | fill charges, parse dates, drop orphans and cancelled rows, normalise country |
| `resolve_scd_prices(sub, plan_history, current_prices)` | SCD Type 2 range-join |
| `enrich_and_join(sub_priced, subs)` | derived columns + inner join to subscriber dimension |
| `archive_landing()` | Moves `landing/` files to `processed_file_archive/<timestamp>/` |

---

### `analytics.py`

**Step 3.** Reads `enriched_subscriptions.csv` from `staged_files/` and produces the two final reporting tables. Has no knowledge of raw data or cleaning rules — cleanly decoupled from upstream logic.

**Functions:**

| Function | Output |
|----------|--------|
| `build_subscriber_summary(enriched)` | Grouped by `billing_country × plan_tier` |
| `build_monthly_revenue(enriched)` | Grouped by `subscription_month × billing_country × plan_tier` |

> All revenue aggregations use `actual_monthly_price` (the SCD-resolved historical price), not the raw `monthly_charge` field.

---

### `runner.py`

**Orchestrator.** Imports and calls each stage function in dependency order. Fails fast on any error — prints a summary table showing pass/fail and elapsed time per step before exiting with code `1`.

**Stage descriptions displayed at runtime:**

```
[INGEST]    Ingest raw CSVs from source → landing/
[TRANSFORM] Clean, enrich & SCD-price  landing/ → staged_files/  + archive landing/
[ANALYTICS] Aggregate analytics tables  staged_files/ → analytics/
```

**CLI:**
```bash
# Full pipeline
python tasks/runner.py

# Custom source
python tasks/runner.py --source "D:/data/raw"

# Partial run (stages always execute in correct order regardless of input order)
python tasks/runner.py --stages ingest transform
python tasks/runner.py --stages transform analytics
python tasks/runner.py --stages analytics
```

**Exit codes:**

| Code | Meaning |
|------|---------|
| `0` | All selected stages completed successfully |
| `1` | A stage failed; error and partial summary logged |

---

## 5. Business Logic

### Subscriber Cleaning (Step 2)

| Sub-step | Rule |
|----------|------|
| **2a Email validation** | Rows with null, blank, or malformed email are dropped. Pattern: `^[^@\s]+@[^@\s]+\.[^@\s]+$` |
| **2b Deduplication** | On `email` — keep the row with the earliest `signup_date` using `Window.partitionBy("email").orderBy(signup_date.asc())` + `row_number()` |
| **2c Null fills** | `name`: null or blank → `"Unknown"`. `country`: null or blank → `"Unknown"`, standard variants normalised (e.g. `"USA"` → `"United States"`) |
| **2d Phone normalisation** | Strips all non-digits; reformats to `XXX-XXX-XXXX` if exactly 10 digits remain; otherwise set to `null` |

### Plan Cleaning & SCD Type 2 (Step 3)

| Sub-step | Rule |
|----------|------|
| **3a plan_id** | Rows with null or blank `plan_id` are dropped |
| **3b price** | Rows with null or negative `price_per_month` are dropped |
| **3c Dates** | `effective_date` and `end_date` parsed with `coalesce(to_date(..., "M/d/yyyy"), to_date(..., "yyyy-MM-dd"))`. Null `end_date` filled with `9999-12-31` (open-window sentinel) |

The resulting `plan_history` table is the SCD Type 2 dimension: each row represents one price version, valid from `effective_date` through `end_date` inclusive.

`current_prices` is a derived lookup of the most recent price per plan (highest `effective_date`), used to compute `price_variance`.

### Subscription Cleaning (Step 4)

| Sub-step | Rule |
|----------|------|
| **4a monthly_charge** | Null → `0.0` |
| **4b Dates** | `subscription_start_date` and `subscription_end_date` parsed from `M/d/yyyy` |
| **4c Orphan subscriber_ids** | Rows referencing a `subscriber_id` not present in the clean subscribers table are dropped |
| **4d Orphan plan_ids** | Rows referencing a `plan_id` not present in the clean plans table are dropped |
| **4e Cancelled** | Rows where `status = 'cancelled'` are excluded |
| **4f billing_country** | Same normalisation mapping applied as for subscriber `country` |

### SCD Type 2 Price Resolution (Step 5)

For each subscription, the historically accurate price is resolved by finding the plan price row where:

```
plan_id = subscription.plan_id
AND effective_date <= subscription_start_date <= end_date
```

Implemented as a non-equi range join in PySpark:

```python
sub.join(
    F.broadcast(plan_history),
    on=(
        (F.col("plan_id") == F.col("ph_plan_id")) &
        (F.col("subscription_start_date") >= F.col("effective_date")) &
        (F.col("subscription_start_date") <= F.col("end_date"))
    ),
    how="left"
)
```

`plan_history` is broadcast (small table) to avoid a shuffle. The result is stored in `actual_monthly_price`.

**Why this matters:** A subscription starting in January for a plan that was $9.99 at the time but is now $11.99 will correctly show $9.99 in all revenue aggregations. Using current prices would overstate early-period revenue by up to 20%.

### Enrichment & Master Join (Steps 6–7)

| Column | Derivation |
|--------|------------|
| `total_subscriptions` | `count("subscription_id")` over `Window.partitionBy("subscriber_id")` — no separate groupBy/join needed |
| `long_term_subscriber` | `duration_days > 183` (~6 months); open-ended subscriptions use `current_date()` as end |
| `duration_days` | `datediff(end_for_calc, subscription_start_date)` |
| `subscription_month` | `date_format(subscription_start_date, "yyyy-MM")` |
| `price_variance` | `round(actual_monthly_price - current_price, 2)` — negative values indicate subscriber is on a legacy lower price |

Master join: enriched subscriptions are inner-joined to the clean subscriber dimension on `subscriber_id`, bringing in `name`, `email`, `country`, `age`, and `signup_date`.

---

## 6. Analytics Outputs

### `subscriber_summary.csv`

Aggregated by `billing_country × plan_tier`.

| Column | Description |
|--------|-------------|
| `billing_country` | Normalised country name |
| `plan_tier` | e.g. Basic, Standard, Premium |
| `num_subscribers` | Distinct subscriber count (`countDistinct`) |
| `total_revenue` | Sum of `actual_monthly_price` (historical prices) |
| `avg_satisfaction` | Average `satisfaction_rating`, rounded to 2dp |
| `num_subscriptions` | Total subscription row count |

### `monthly_revenue.csv`

Aggregated by `subscription_month × billing_country × plan_tier`.

| Column | Description |
|--------|-------------|
| `subscription_month` | `YYYY-MM` of `subscription_start_date` |
| `billing_country` | Normalised country name |
| `plan_tier` | e.g. Basic, Standard, Premium |
| `total_revenue` | Sum of `actual_monthly_price` for that month |
| `num_active_subscriptions` | Count of subscriptions started that month |
| `avg_price_in_effect` | Average historical price for that month/tier/country |

---

## 7. Archive Strategy

The archive step is owned by `transform.py`, not `ingestion.py`. This is intentional:

```
ingestion.py   →  copy source to landing/        (source untouched)
transform.py   →  process landing/               (if this fails → landing/ intact, safe retry)
               →  write all staged files          (all three must succeed)
               →  archive_landing()              ← only reached on full success
```

Each archive run creates a timestamped subfolder:
```
processed_file_archive/
└── 2026-05-30_10-45-00/
    ├── subscribers.csv
    ├── plans.csv
    └── subscriptions.csv
```

This gives a full audit trail of every pipeline run while ensuring files are never archived prematurely.

---

## 8. How to Run

### Prerequisites

- Python 3.10+
- PySpark (`pip install pyspark`)
- Java 8 or 11 (required by Spark)

### Run the full pipeline

```bash
# From the Assessment project root
python tasks/runner.py
```

### Override the source directory

```bash
python tasks/runner.py --source "D:/data/raw"
```

### Run individual stages

```bash
# Stage 1 only
python tasks/ingestion.py
python tasks/ingestion.py --source "D:/data/raw"

# Stage 2 only (reads from landing/)
python tasks/transform.py

# Stage 3 only (reads from staged_files/)
python tasks/analytics.py
```

### Rerun from a specific stage (e.g. after fixing a transform bug)

```bash
python tasks/runner.py --stages transform analytics
python tasks/runner.py --stages analytics
```

### Expected console output (full pipeline)

```
╔═══════════════════════════════════════════════════════════════╗
║           ASSESSMENT  PIPELINE  RUNNER                    ║
╠═══════════════════════════════════════════════════════════════╣
║  Source   : C:/Users/xxxx/Downloads                         ║
║  Stages   : ingest, transform, analytics                     ║
╚═══════════════════════════════════════════════════════════════╝

┌───────────────────────────────────────────────────────────────┐
│  STAGE: INGEST                                                │
│  Ingest raw CSVs from source → landing/                       │
└───────────────────────────────────────────────────────────────┘
  ...
┌───────────────────────────────────────────────────────────────┐
│  STAGE: TRANSFORM                                             │
│  Clean, enrich & SCD-price  landing/ → staged_files/  + archive landing/ │
└───────────────────────────────────────────────────────────────┘
  ...
╔═══════════════════════════════════════════════════════════════╗
║                    PIPELINE SUMMARY                          ║
╠═══════════════════════════════════════════════════════════════╣
║  ingest       ✔  SUCCESS               3.2s                  ║
║  transform    ✔  SUCCESS              47.8s                  ║
║  analytics    ✔  SUCCESS              12.1s                  ║
╠═══════════════════════════════════════════════════════════════╣
║  Total time : 63.1s                                          ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## 9. Design Decisions

**SCD Type 2 was implemented.** This decision was made because the raw data already contained attributes that support historical tracking.

**Country normalisation uses a Catalyst `when/otherwise` chain, not a UDF.** Mapping logic that can be expressed as a static lookup runs entirely inside the JVM optimiser with no Python serialisation overhead. UDFs are reserved for logic that genuinely requires Python (email regex, phone reformatting).

**Email deduplication uses `Window + row_number()`.** Partitioning by email, ordering by `signup_date` ascending, and keeping rank 1 is a single-pass operation. The alternative (groupBy min → join back) costs an extra shuffle stage.

**SCD price resolution uses a non-equi range join with `F.broadcast()`.** The `plan_history` table is small enough to broadcast on every worker, making the range join (`start_date BETWEEN effective_date AND end_date`) a local operation with no shuffle. On very large plan history tables, a sort-merge join with date bucketing would be preferred.

**`total_subscriptions` is computed via `Window.partitionBy("subscriber_id")`**, not a separate `groupBy` + join. This keeps the lineage to a single DataFrame and avoids an extra shuffle stage.

**Archiving is the last action in `transform.py`.** If any cleaning step, SCD join, enrichment, or staged write fails, `landing/` is untouched and the pipeline can safely retry from the transform stage without re-ingesting.

**`write_single_csv()` uses `coalesce(1)` + part-file rename.** Spark always writes to a directory with part files. Coalescing to 1 partition then renaming the single part file produces a clean, directly openable CSV without breaking the distributed write path.
