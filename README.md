This repository contains solution to the NYC Taxi Data Engineering pipeline challenge. The goal of this project is to process a massive, multi-year archive of raw NYC TLC taxi trip records using Apache Spark, ensuring data quality, optimal partitioning, and readiness for a modern Star Schema Data Warehouse.


# What it contains

# 1. Pipeline Construction
A PySpark ETL pipeline (`etl_pipeline.py`) that seamlessly ingests raw Parquet files from local storage (`data/raw/`). The Spark engine handles the massive datasets efficiently, mapping the data to fact and dimension fields ready for a warehouse like Snowflake, BigQuery, or Redshift.

# 2. Data Modeling (Star Schema)
The data is structured to be loaded into a Kimball Star Schema for analytical reporting.
`Fact_Trips`: The central table containing the transactional trip data (fares, distances, durations).
* **Dimensions**: Separated into `Dim_Date`, `Dim_Geography` (Pickup/Dropoff Zones), and `Dim_Payment`.
* *See `docs/star_schema.md` for the full architecture diagram and `sql/` for the table creation scripts.*

### 3. Storage Optimization (Partitioning Strategy)
To ensure analytical dashboards don't trigger full table scans on a 10-year dataset, the pipeline dynamically partitions the `Fact_Trips` table by `pickup_year` and `pickup_month`.
* **Example:** `data/warehouse/fact_trips/pickup_year=2025/pickup_month=1/`
* **Result:** A query for "last month's average fare" will only scan that specific folder (Partition Pruning), saving immense amounts of time and compute cost.

### 4. Data Quality Checks
Automated quality filters isolate "impossible" trips to prevent bad data from ruining analytics.
* We reject records where: `trip_duration <= 0`, `fare_amount < 0`, or `trip_distance < 0`.
* Good data goes to `data/warehouse/`.
* Bad data is quarantined in `data/rejected/` with an attached `rejection_reason` column for auditing.

# Repository Structure

# project file 
│
├── data/
│   ├── raw/               ← Place downloaded TLC Parquet files here
│   ├── warehouse/         ← Pipeline outputs Star Schema here (dim_date, dim_payment, fact_trips)
│   └── rejected/          ← Quarantined bad data
│
├── config.py              ← Shared environment config (Windows/Java fixes)
├── etl_pipeline.py        ← The main PySpark ETL script
├── generate_report.py     ← Script to test partitioning and print analytics
├── requirements.txt       ← Python dependencies
│
├── sql/
│   ├── create_star_schema.sql  ← Handwritten DDL scripts
│   ├── schema_report.sql       ← Auto-generated DDL from PySpark schema
│   └── analytics_queries.sql   ← Optimized analytical SQL queries
│
└── docs/
    ├── architecture.md    ← Explanation of ETL flow and Partitioning
    └── star_schema.md     ← Star schema diagram and model logic



# How to Run the Pipeline

### Prerequisites
Make sure you have Python and Java installed.
```bash
pip install -r requirements.txt
```

Windows Users:
1. Java 21+: Spark may throw a `SecurityManager` error on newer Java versions. This project automatically injects `-Djava.security.manager=allow` to fix it.
2. Hadoop Winutils: Spark on Windows requires `winutils.exe` and `hadoop.dll`.
   - Create the folder `C:\hadoop\bin\`
   - Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.6 (or similar) from [cdarlint/winutils](https://github.com/cdarlint/winutils) and place them in that folder.
   - The project `config.py` will automatically find them.

# 1. Get the Data
Download a few months of NYC Yellow Taxi data (e.g., Jan-Mar 2025) from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place the `.parquet` files inside the `data/raw/` directory.

# 2. Run the ETL Pipeline
Run the main PySpark job. It will automatically read the raw data, apply transformations and quality checks, and partition the output.
```bash
python etl_pipeline.py
```

# 3. Generate Analytics and Prove Optimization
To simulate querying the Data Warehouse and prove that the partitioning strategy works, you can run the report generator:
```bash
python generate_report.py
```
This will output the Data Quality Report, the Partition Layout, and execute analytics queries (like Average Fare and Top Pickup Zones) against the clean data.
