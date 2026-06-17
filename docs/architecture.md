We needed a **real‑world** data pipeline that could take the massive NYC TLC Yellow‑Taxi parquet dump, clean it, and spit out a tidy **star schema** ready for any BI platform. The goal was to keep the code **modular**, **testable**, and **fast** on a Windows laptop.

---

## 1️⃣ System Overview
| Piece | Why it matters |
|------|----------------|
| **Raw data** (`data/raw/`) | Parquet files straight from the NYC TLC website – immutable, source‑of‑truth. |
| **Spark ETL engine** (`etl_run.py`) | Handles billions of rows in parallel, runs locally on your laptop (or any cluster). |
| **Transformations** (`transformations.py`) | Computes `trip_duration`, adds time dimensions, maps payment codes. |
| **Quality checks** (`quality_checks.py`) | Filters out impossible rows and records why they were rejected. |
| **Warehouse** (`data/warehouse/`) | Star‑schema tables (`dim_date`, `dim_payment`, `dim_zone`, `fact_trips`). Partitioned by `pickup_year`/`pickup_month` for fast queries. |
| **Rejected bucket** (`data/rejected/`) | Bad rows with a `rejection_reason` column – handy for audits. |
| **DDL generator** (`ddl_generator.py`) | Auto‑creates Snowflake‑compatible `CREATE TABLE` statements. |
| **Analytics demo** (`generate_report.py`) | Shows a quick quality report and sample analytics (avg fare, top zones). |

---

## 2️⃣ Architecture Diagram (text‑only, keep it simple)
```
+-------------------+   +---------------------+   +-------------------+
|  data/raw/        | → | Spark ETL (etl_run) | → | data/warehouse/   |
|  (source parquet) |   |  - transforms       |   |  (star schema)    |
+-------------------+   |  - validates        |   +-------------------+
                        |  - loads            |
                        +---------------------+
                                 |
                                 v
                         +-------------------+
                         | data/rejected/    |
                         | (quarantined bad |
                         |   records)        |
                         +-------------------+
```

---

## 3️⃣ Component Descriptions (what each file does)
- **`etl_run.py`** – the *main* orchestrator. Boots a `SparkSession`, applies the env fixes for Windows, and calls the other modules in order.
- **`transformations.py`** – pure Spark helpers:
  - `compute_trip_duration` – seconds between pickup & dropoff.
  - `add_time_dimensions` – creates `pickup_date`, `pickup_year`, `pickup_month`.
  - `map_payment_types` – turns numeric payment codes into human‑readable strings.
- **`quality_checks.py`** – validates each row:
  - `trip_duration > 0`
  - `fare_amount >= 0`
  - `trip_distance >= 0`
  - Returns a **valid** DataFrame and a **rejected** DataFrame with a `rejection_reason` column.
- **`warehouse_loader.py`** – builds the dimension tables (`dim_date`, `dim_payment`, `dim_zone`) and the fact table (`fact_trips`). Writes everything as parquet, partitioning the fact by year+month.
- **`ddl_generator.py`** – inspects the Spark DataFrames and spits out Snowflake‑compatible `CREATE TABLE` DDL. Saved to `sql/schema_report.sql`.
- **`generate_report.py`** – reads the star schema back, prints a quick data‑quality summary and runs a few demo analytics queries.

---

## 4️⃣ ETL Workflow (step‑by‑step)
```text
1. READ   → spark.read.parquet('data/raw/')
2. TRANSFORM → compute_trip_duration → add_time_dimensions → map_payment_types
3. VALIDATE → split_valid_invalid_records (quality_checks)
4. WRITE VALID → warehouse_loader writes dim tables + fact_trips (partitioned)
5. WRITE REJECTED → rejected rows go to data/rejected/
6. REPORT → prints a quality summary & optional DDL generation
```

---

## 5️⃣ Partitioning Strategy (why we do it)
We store `fact_trips` **by year and month**:
```
data/warehouse/fact_trips/
  ├─ pickup_year=2025/
  │    └─ pickup_month=01/
  │    └─ pickup_month=02/
  │    …
```
When a query filters on `pickup_year = 2025 AND pickup_month = 01`, Spark only opens that one folder – a huge I/O win (often > 95 % reduction). This is called **partition pruning** and is the secret sauce for fast BI dashboards.

---

## 6️⃣ Data‑Quality Strategy (how we keep data clean)
```text
Raw rows → quality_checks →
  ✅ valid → go to warehouse
  ❌ invalid → go to data/rejected/ with `rejection_reason`
```
The `rejection_reason` column concatenates the rule names that failed (e.g. `"negative_fare,time_inversion"`). That makes downstream debugging painless.

---

## 7️⃣ Tech Stack (what we actually use)
| Layer | Tool | Reason |
|-------|------|--------|
| Processing | **Apache Spark (PySpark)** | Distributed, handles TB‑scale data, built‑in Parquet support |
| Storage format | **Parquet** | Columnar, compressed, predicate push‑down |
| Orchestration | **CLI (`etl_run.py`)** | Simple, easy to wrap in Airflow/Prefect later |
| Warehouse (optional) | **Redshift / Snowflake / BigQuery** | Industry‑standard analytical DWH |
| Testing | **pytest + Spark local mode** | Fast, no cluster required |
| Containerisation | **Docker** (provided in `Dockerfile`/`docker‑compose.yml`) | Reproducible dev environment |

---

## 8️⃣ Deployment Options (how to run it)
- **Local dev** (what most of us use):
  ```bash
  pip install -r requirements.txt
  pytest tests/ -v   # sanity check
  python etl_run.py   # run the pipeline
  ```
- **Docker** – spin up a container with Spark pre‑installed:
  ```bash
  docker compose up   # builds & runs the ETL inside a container
  ```
- **Cloud** – drop the same code onto EMR, Dataproc, or Azure HDInsight. Just point `config.py` at an S3/GCS bucket and let Spark read/write from there.

---

## 9️⃣ What you can safely delete (optional cleanup)
- `etl_pipeline.py` – a thin deprecation shim, not needed after you commit to `etl_run.py`.
- `.git/`, `.gitignore`, `.pytest_cache/`, `__pycache/` – version‑control and test caches.
- `requirements.txt` – only needed the first time you install dependencies.
- `jars/snowflake-jdbc-3.13.17.jar`, `jars/spark-snowflake_2.13-3.1.9.jar` – required only if you actually write to Snowflake.

---

## 🎉 That’s all, folks!
If you need to add more quality rules, drop a new dimension, or point the loader at a cloud warehouse – just edit the appropriate module. The code is deliberately small and well‑tested, so you can iterate quickly. Happy hacking! 🚀
