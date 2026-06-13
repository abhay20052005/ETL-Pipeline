# =============================================================================
# Big Apple Scalability Challenge — Architecture Documentation
# =============================================================================
# File   : docs/architecture.md
# Author : Data Engineering Team
# =============================================================================

# Architecture Overview

## Big Apple Scalability Challenge
### End-to-End NYC Taxi Trip Data Engineering Pipeline

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Component Descriptions](#component-descriptions)
4. [ETL Workflow](#etl-workflow)
5. [Partitioning Strategy](#partitioning-strategy)
6. [Data Quality Strategy](#data-quality-strategy)
7. [Technology Choices](#technology-choices)
8. [Deployment Options](#deployment-options)

---

## System Overview

The **Big Apple Scalability Challenge** is a production-grade data engineering pipeline designed to process millions of NYC Taxi trip records using Apache Spark. The system ingests raw Parquet files from the NYC Taxi & Limousine Commission (TLC), applies data quality validation, transforms the data into a Star Schema, and loads it into a cloud data warehouse for analytics.

**Key Design Goals:**
- **Scalability** — Spark distributed processing handles datasets from GB to TB
- **Data Quality** — Every record is validated; bad data is isolated and audited
- **Analytics-Ready** — Star Schema enables fast BI queries with partition pruning
- **Modularity** — Each concern (ingest, transform, validate, load) is decoupled

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      BIG APPLE SCALABILITY CHALLENGE                        │
│                    End-to-End Data Engineering Pipeline                      │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐
  │  DATA SOURCE │   NYC TLC Public Dataset (Parquet files)
  │  nyc.gov/tlc │   ~100 MB – 500 MB per month, 12+ years of data
  └──────┬───────┘
         │ download / copy
         ▼
  ┌──────────────┐
  │  data/raw/   │   Landing zone — raw Parquet files, immutable
  │  (Bronze)    │   e.g. yellow_tripdata_2022-06.parquet
  └──────┬───────┘
         │ spark.read.parquet()
         ▼
  ┌──────────────────────────────────────────────────────┐
  │                  SPARK ETL ENGINE                     │
  │  ┌────────────┐  ┌──────────────┐  ┌──────────────┐  │
  │  │  Ingest    │→ │  Transform   │→ │   Validate   │  │
  │  │ (etl.py)   │  │(transform.py)│  │(quality_.py) │  │
  │  └────────────┘  └──────────────┘  └──────┬───────┘  │
  └────────────────────────────────────────────┼──────────┘
                                               │
                          ┌────────────────────┼──────────────────────┐
                          │                    │                      │
                          ▼                    ▼                      ▼
                   ┌─────────────┐    ┌────────────────┐    ┌──────────────┐
                   │data/rejected│    │ data/processed/ │    │  Validation  │
                   │  (quarantine)│   │    (Silver)     │    │   Report     │
                   │             │   │ Partitioned by  │    │  (console)   │
                   └─────────────┘   │ pickup_year/    │    └──────────────┘
                                     │ pickup_month    │
                                     └───────┬─────────┘
                                             │ spark.read.parquet()
                                             ▼
                                   ┌──────────────────────────┐
                                   │   STAR SCHEMA BUILDER    │
                                   │   (warehouse_loader.py)  │
                                   │                          │
                                   │  Dim_Date                │
                                   │  Dim_Geography           │
                                   │  Dim_Payment             │
                                   │  Fact_Trips              │
                                   └───────────┬──────────────┘
                                               │
                          ┌────────────────────┼────────────────────┐
                          │                    │                    │
                          ▼                    ▼                    ▼
                   ┌────────────┐    ┌──────────────────┐   ┌─────────────┐
                   │ Snowflake  │    │   BigQuery       │   │  Redshift   │
                   │  COPY INTO │    │   bq load        │   │  COPY FROM  │
                   └────────────┘    └──────────────────┘   └─────────────┘
```

---

## Component Descriptions

### `src/ingest.py` — Data Ingestion
Handles data acquisition from the NYC TLC public dataset or generates synthetic sample data for offline testing.

| Feature | Detail |
|---------|--------|
| Download | `urllib.request` — no external dependency |
| Sample generation | `pandas` + `numpy` with injected bad records |
| Output | `data/raw/*.parquet` |

### `src/transformations.py` — Data Transformation
Applies a deterministic chain of column-level transformations:

| Step | Transformation |
|------|---------------|
| 1 | Rename `tpep_*` → `pickup_datetime`, `dropoff_datetime` |
| 2 | Cast to `TimestampType` |
| 3 | Derive `pickup_date`, `pickup_year`, `pickup_month`, `pickup_hour` |
| 4 | Compute `trip_duration` (seconds) |
| 5 | Generate `trip_id` (monotonically increasing) |
| 6 | Map `payment_type` codes → human-readable labels |
| 7 | Fill non-critical nulls with `0` defaults |
| 8 | Drop superseded raw columns |

### `src/quality_checks.py` — Data Validation
Evaluates 5 quality rules per record:

| Rule | Condition | Action |
|------|-----------|--------|
| `non_positive_duration` | `trip_duration ≤ 0` | Reject |
| `negative_fare` | `fare_amount < 0` | Reject |
| `negative_distance` | `trip_distance < 0` | Reject |
| `time_inversion` | `dropoff < pickup` | Reject |
| `null_required_fields` | critical column is NULL | Reject |

### `src/etl.py` — Pipeline Orchestrator
Sequences all stages, manages SparkSession lifecycle, and prints the validation report.

### `src/warehouse_loader.py` — Star Schema + Warehouse Loading
Builds dimension and fact DataFrames and generates SQL COPY scripts for all three major cloud warehouses.

---

## ETL Workflow

```
Step 1: READ
  spark.read.parquet("data/raw/")
  → Schema auto-inferred from Parquet metadata
  → Single DataFrame representing all raw files

Step 2: TRANSFORM  [transformations.py]
  → rename columns → cast types → derive date fields
  → compute trip_duration → assign trip_id
  → map payment codes → fill nulls → drop junk columns

Step 3: VALIDATE  [quality_checks.py]
  → apply 5 quality rules as boolean flag columns
  → split into valid_df and rejected_df
  → enrich rejected_df with rejection_reason

Step 4: WRITE VALID  [etl.py → TaxiDataWriter]
  → valid_df.write.partitionBy("pickup_year","pickup_month").parquet(...)
  → Produces folder hierarchy: data/processed/pickup_year=2022/pickup_month=6/

Step 5: WRITE REJECTED  [etl.py → TaxiDataWriter]
  → rejected_df.write.parquet("data/rejected/")
  → Preserved for audit, reprocessing, or root-cause analysis

Step 6: REPORT
  → Print validation summary to console
  → Exit code 1 if bad-data rate > 10%
```

---

## Partitioning Strategy

### Why Partition?

Without partitioning, every query must scan the **entire dataset** — even if it only needs one month. On a 5-year dataset (60+ month-partitions), this means scanning 60× more data than necessary.

### How Partitioning Works

```
data/processed/
  pickup_year=2019/
    pickup_month=1/  ← ~50 MB
    pickup_month=2/
    ...
  pickup_year=2020/
    ...
  pickup_year=2022/
    pickup_month=6/  ← only this directory is opened for WHERE year=2022 AND month=6
```

### Partition Pruning in Action

```sql
-- SQL query with predicate
SELECT AVG(fare_amount)
FROM processed_trips
WHERE pickup_year = 2022 AND pickup_month = 6;

-- Spark physical plan shows:
-- PartitionFilters: [isnotnull(pickup_year), (pickup_year = 2022),
--                   isnotnull(pickup_month), (pickup_month = 6)]
-- → Only 1 of 60 partitions is opened (98% I/O reduction)
```

### Configuration That Enables This

```python
.config("spark.sql.parquet.filterPushdown", "true")   # ← partition pruning
.config("spark.sql.adaptive.enabled", "true")          # ← runtime optimization
```

### Partitioning Options

| Strategy | Columns | Best For |
|----------|---------|----------|
| **Year + Month** | `pickup_year, pickup_month` | Monthly dashboards, billing (recommended) |
| **Date** | `pickup_date` | Day-level drilldown, operational reports |
| **Year only** | `pickup_year` | Coarser granularity, fewer directories |

---

## Data Quality Strategy

```
                    ┌─────────────────────┐
                    │    Raw Records      │
                    │   (all ingested)    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Quality Checker    │
                    │  5 validation rules │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴───────────────┐
              │                                │
    ┌─────────▼──────────┐          ┌──────────▼────────────┐
    │   VALID records    │          │  REJECTED records      │
    │                    │          │  + rejection_reason    │
    │  data/processed/   │          │  (comma-sep rule names)│
    │  (partitioned)     │          │  data/rejected/        │
    └────────────────────┘          └───────────────────────┘
```

**Rejection reason** column example:
```
"negative_fare,time_inversion"
```
This enables analysts to triage bad data by rule type without re-running the pipeline.

---

## Technology Choices

| Layer | Technology | Reason |
|-------|-----------|--------|
| Processing | Apache Spark (PySpark) | Distributed, fault-tolerant, handles TB-scale |
| Storage format | Parquet | Columnar, compressed, predicate push-down |
| Orchestration | CLI (`argparse`) | Simple; plug into Airflow/Prefect as operator |
| Warehouse | Snowflake / BigQuery / Redshift | Industry-standard cloud DWH |
| Testing | pytest + Spark local mode | Fast, no cluster needed |
| Containerisation | Docker + docker-compose | Reproducible dev environment |

---

## Deployment Options

### Local Development
```bash
python src/ingest.py --sample          # generate test data
python src/etl.py --input data/raw/   # run pipeline
pytest tests/ -v                       # run tests
```

### Docker
```bash
docker-compose up                      # spin up Spark + pipeline
```

### Cloud (Production)
- **AWS EMR** — submit as a `spark-submit` job, S3 as data lake
- **Google Dataproc** — submit to Dataproc cluster, GCS as data lake
- **Azure HDInsight / Databricks** — Azure Blob Storage or ADLS
- **Snowflake Snowpark** — run Spark transformations natively in Snowflake
