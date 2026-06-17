# NYC Taxi ETL – quick‑start guide (by a 2‑yr engineer)

## What this repo does
Hey! 👋 This is a tiny but **real‑world** data‑engineering pipeline that reads the NYC TLC Yellow‑Taxi parquet files, cleans them up and writes them out as a **star‑schema** ready for BI tools (Redshift, Snowflake, BigQuery … you name it).

### TL;DR
- **Ingest** raw parquet files from `data/raw/`
- **Transform** timestamps, compute `trip_duration`, add time dimensions and map payment codes
- **Validate** rows (negative duration/fare/distance are tossed into `data/rejected/` with a `rejection_reason` column)
- **Load** a Star Schema (`dim_date`, `dim_payment`, `dim_zone`, `fact_trips`) into `data/warehouse/` – partitioned by `pickup_year`/`pickup_month`
- **Optional**: generate Snowflake DDL and run a quick analytics report

---

## Repo layout (after we moved everything out of `src/`)
```
project-root/
│
├─ data/                     # data lake folders
│   ├─ raw/                  # drop the original TLC parquet files here
│   ├─ warehouse/            # happy, clean star‑schema tables
│   └─ rejected/             # quarantine for bad rows (with reasons)
│
├─ transformations.py        # pure Spark helpers (duration, time dims, payment map)
├─ quality_checks.py         # validation + split logic
├─ warehouse_loader.py       # build dim/fact tables and write parquet
├─ ddl_generator.py          # auto‑generate Snowflake‑compatible DDL
├─ config.py                 # env vars, Windows winutils handling, Spark config
├─ etl_run.py                # **entry‑point** – orchestrates everything
├─ etl_pipeline.py           # thin deprecation wrapper (feel free to delete later)
├─ generate_report.py        # tiny script that shows a quality report & sample queries
│
├─ tests/                    # pytest suite (4 tests, all pass)
│   └─ test_pipeline.py
│
├─ sql/                      # generated DDL (`schema_report.sql`) + samples
│   └─ schema_report.sql
│
├─ docs/                     # docs – architecture explained in plain English
│   └─ architecture.md
│
├─ requirements.txt          # Python deps (pyspark, python‑dotenv, pytest)
└─ README.md                 # you are reading it!
```

---

## How to get it running (Windows friendly)
1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Windows‑only prep**
   - Install Java 21+ (Spark needs it). If you see a `SecurityManager` error, we already add `-Djava.security.manager=allow`.
   - Download `winutils.exe` (and `hadoop.dll`) for Hadoop 3.3.x from the
     [cdarlint/winutils](https://github.com/cdarlint/winutils) repo.
   - Put them in `C:\hadoop\bin\`. `config.py` will pick them up automatically.

3. **Run the test suite** (makes sure our Spark functions behave as expected)
   ```bash
   pytest tests/ -v
   ```
   You should see **4 passed**.

4. **Drop some raw data**
   Grab a few months of NYC Yellow‑Taxi data (e.g. Jan‑Mar 2025) from the
   [TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and unzip the `.parquet` files into `data/raw/`.

5. **Run the pipeline**
   ```bash
   python etl_run.py
   ```
   You’ll see a friendly log, a data‑quality summary and the star‑schema written under `data/warehouse/`.

6. **Optional – peek at the warehouse**
   ```bash
   python generate_report.py
   ```
    This prints a quick analytics snapshot (average fare, top zones, etc.) and also writes the Snowflake DDL to `sql/schema_report.sql`.

---

## Why the partitioning matters
We store `fact_trips` **by year & month** (`pickup_year=2025/pickup_month=01/…`).
When a BI tool asks *“average fare for March 2025”* Spark only opens that single folder – that’s **partition pruning** and saves a ton of I/O.
If you ever grow to a decade of data, the difference is night‑and‑day.

---

## Data‑quality in a nutshell
Bad rows are filtered out by three simple rules:
- `trip_duration > 0`
- `fare_amount >= 0`
- `trip_distance >= 0`
If any rule fails we add a `rejection_reason` string (e.g. `"negative_fare,time_inversion"`) and dump the record into `data/rejected/` for later audit.

---

## Gotchas / what you can delete safely
- `etl_pipeline.py` is just a thin wrapper that prints a deprecation warning – you can remove it once you’re comfortable calling `etl_run.py` directly.
- `.git/`, `.gitignore`, `.pytest_cache/`, `__pycache__/`, `requirements.txt` (if you’ve already installed deps) are not needed for the pipeline to run.
- The JARs in `jars/snowflake-jdbc-3.13.17.jar` and `jars/spark-snowflake_2.13-3.1.9.jar` are only required if you actually write to Snowflake.

---

## That’s it!
Feel free to tweak the modules, add more quality rules or point the loader at a cloud warehouse. Happy hacking! 🚀
