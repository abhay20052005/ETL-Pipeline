## INTRO
In this project i have created a data pipeline which works as a real world pipeline it takes various data in form of parquent files , it cleans it and divides the file into two folders one which is used to examine or analysis and one in rejected section and it converts it into a star schema ready for platforms. The code written follows modularity , testable and is fast.

### TLDR
* **Ingest** raw Parquet files from `data/raw/`
* **Transform** the data by formatting timestamps, calculating trip duration, creating date and time fields, and converting payment codes into readable payment types
* **Validate** the data by checking for invalid records. Trips with negative duration, fare amount, or distance are moved to `data/rejected/` along with a `rejection_reason` explaining why they were rejected
* **Load** the cleaned data into a Star Schema consisting of `dim_date`, `dim_payment`, `dim_zone`, and `fact_trips` tables inside `data/warehouse/`, partitioned by `pickup_year` and `pickup_month` for faster querying
* **Optional**: create Snowflake table scripts automatically and generate a basic analytics report to summarize key business insights from the trip data


---

## Repo layout
```
project-root/
│
├─ data/                     
│   ├─ raw/                  
│   ├─ warehouse/            
│   └─ rejected/             
│
├─ transformations.py        
├─ quality_checks.py         
├─ warehouse_loader.py       
├─ ddl_generator.py          
├─ config.py                 
├─ etl_run.py                
├─ etl_pipeline.py           
├─ generate_report.py        
│
├─ tests/                    
│   └─ test_pipeline.py
│
├─ sql/                      
│   └─ schema_report.sql
│
├─ docs/                     
│   └─ architecture.md
│
├─ requirements.txt          
└─ README.md                 
```

## How to get it running
1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Windows‑only prep**
* Install **Java 21 or a newer version**, as Spark requires Java to run. If you encounter a `SecurityManager` error, the project is already configured to handle it using the `-Djava.security.manager=allow` setting.
* Download **`winutils.exe`** and **`hadoop.dll`** for Hadoop 3.3.x.
* Place both files inside the `C:\hadoop\bin\` folder.
* Once they are in the correct location, `config.py` will automatically detect and use them, so no additional configuration is needed.



3. **Run the test suite** 
   ```bash
   pytest tests/ -v
   ```
   You should see 4 passed.

4. **Drop some raw data**
   Take a few months data of NYC Yellow‑Taxi data from the
   [TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and unzip the `.parquet` files into `data/raw/`.

5. **Run the pipeline**
   ```bash
   python etl_run.py
   ```
   You’ll see some logs and data quality summary is generated and star schema is also generated in `data/warehouse/`.

6. **Generate report from it**
   ```bash
   python generate_report.py
   ```
    This prints a quick analytics reports and hows on terminal (average fare, top zones, etc.) and also writes the Snowflake DDL to `sql/schema_report.sql`.

## Why the partitioning matters
We store `fact_trips` *by year & month*.
When a tool asks *“average fare for March 2025”* Spark only opens that single folder – that’s **partition pruning** and saves all input output

## Data Quality Checks

To ensure that only valid trip records are loaded into the warehouse, the pipeline applies three simple validation rules:

trip_duration > 0

fare_amount >= 0

trip_distance >= 0

If a record fails any of these checks, it is considered invalid.

For every invalid record, a rejection_reason is added and the record is stored in data/rejected/ for future review and validation
