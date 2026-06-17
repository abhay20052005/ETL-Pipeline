In this project i have created a data pipeline which works as a real world pipeline it takes various data in form of parquent files , it cleans it and divides the file into two folders one which is used to examine or analysis and one in rejected section and it converts it into a star schema ready for platforms. The code written follows modularity , testable and is fast.


## System Overview
| Files | Why it matters |
|------|----------------|
| (`data/raw/`) | Contains parquent files in form of data to be analyzed |
| (`etl_run.py`) | Has all the combined values from different files and if we run this pipeline will simply start working |
| (`transformations.py`) | Computes `trip_duration`, adds time dimensions, maps payment codes. |
| (`quality_checks.py`) | Filters out impossible rows and records why they were rejected. |
| (`data/warehouse/`) | Star‑schema tables (`dim_date`, `dim_payment`, `dim_zone`, `fact_trips`). Partitioned by `pickup_year`/`pickup_month` for fast queries. |
| (`data/rejected/`) | Bad rows with a `rejection_reason`. |
| (`ddl_generator.py`) | Creates Snowflake‑compatible `CREATE TABLE` statements. |
| (`generate_report.py`) | Shows a quick quality report of the pipeline basically that has been asked for. |

## Architecture Diagram
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

## Component Descriptions
- **`etl_run.py`** – the *main* file. Performs a `SparkSession`, applies the env fixes for Windows, and call everyfunction imported form different py files
- **`transformations.py`** – Spark helpers:
  - `compute_trip_duration` – calculates time between pickup & dropoff in seconds.
  - `add_time_dimensions` – creates `pickup_date`, `pickup_year`, `pickup_month`.
  - `map_payment_types` – differentiate between the type of payments that are been processed.
- **`quality_checks.py`** – validates each row:
  - `trip_duration > 0`
  - `fare_amount >= 0`
  - `trip_distance >= 0`
  - Returns a **valid** DataFrame and a **rejected** DataFrame with a `rejection_reason` column.
- **`warehouse_loader.py`** – builds tables (`dim_date`, `dim_payment`, `dim_zone`) and the fact table (`fact_trips`). This is use in partitioning the fact by year+month.
- **`ddl_generator.py`** – Checks Spark DataFrames and spits out Snowflake‑compatible `CREATE TABLE` DDL. Saved to `sql/schema_report.sql`.
- **`generate_report.py`** – reads the star schema back, gives a quick data‑quality summary and also shows analytics.

## ETL Workflow
```text
1. READ   → read parquent files using spark from ('data/raw/')
2. TRANSFORM → trip_duration → time → payment
3. VALIDATE → valid_invalid
4. WRITE VALID → warehouse_loader writes dim tables + fact_trips
5. WRITE REJECTED → rejected rows go to data/rejected/
6. REPORT → Gives a quality report
```

## Partitioning Strategy
We store `fact_trips` **by year and month**:
```
data/warehouse/fact_trips/
  ├─ pickup_year=2025/
  │    └─ pickup_month=01/
  │    └─ pickup_month=02/
  │    …
```
When a query asks for data from **January 2025** (`pickup_year = 2025 AND pickup_month = 01`), Spark reads only the folder for **January 2025** instead of scanning all the data. This greatly reduces the amount of data that needs to be read, making queries much faster. This technique is called **partition pruning**, and it helps improve performance and save processing time.

## Data‑Quality Strategy
```text
Raw rows → quality_checks →
valid → go to warehouse
invalid → go to data/rejected/ with `rejection_reason`
```
The `rejection_reason` column stores the reasons on basis of which the files have been rejected to validate (e.g. `"negative_fare,time_inversion"`).

## Tech Stack
| Layer | Tool |
|-------|------|--------|
| Processing | **Apache Spark (PySpark)** | 
| Storage format | **Parquet** | 
| Orchestration | **CLI (`etl_run.py`)** | 
| Warehouse (optional) | **Redshift / Snowflake / BigQuery** | 
| Testing | **pytest + Spark local mode** | 
| Containerisation | **Docker** |

---

## Deployment Options
- **Local dev**:
  ```bash
  pip install -r requirements.txt
  pytest tests/ -v   # it checks wheather all the test are working and it is completely optional to run
  python etl_run.py   # this is our main file and we have to runt this
  ```
- **Docker** – with Spark pre‑installed:
  ```bash
  docker compose up   # This gives you everything annd you just have to run this it will have all dependies
  ```

## What you can safely delete
* `etl_pipeline.py` – an older file kept for compatibility. Once you start using `etl_run.py`, this file is no longer needed.
* `.git/`, `.gitignore/`, `.pytest_cache/`, `__pycache__/` – files and folders used for version control, testing, and caching. They are not part of the actual ETL logic.
* `requirements.txt` – contains the list of Python packages needed for the project. It is mainly used when setting up the project for the first time.
* `jars/snowflake-jdbc-3.13.17.jar` and `jars/spark-snowflake_2.13-3.1.9.jar` – connector files that allow Spark to communicate with Snowflake. They are only required if data is being read from or written to Snowflake.

