# ETL PIPELINE

This work of mine includes solution of a given large parquent files and how to create or convert that data in such a way in order to make analysis easy. The goal of this project was to build fast ETL pipeline that handles such data well with forming a Star Schema in Warehouse.

## Objectives used in making 

### Pipeline Construction

A PySpark ETL pipeline (`etl_pipeline.py`) is used to read raw Parquet files from the `data/raw/` folder. Spark processes large volumes of trip data efficiently, cleans and transforms the data, and prepares it for loading into analytical tables. During this process, important fields such as trip duration, payment type, and date information are generated to support reporting and analysis.

### Data Modeling Using a Star Schema

The processed data is organized using a **Star Schema**, which is optimized for analytical queries and dashboard reporting.

* **`Fact_Trips`**: The main table containing trip-related metrics such as fare amount, trip distance, passenger count, and trip duration.
* **Dimension Tables**:

* `Dim_Date` – stores date-related information.
* `Dim_Geography` – stores pickup and dropoff location details.
* `Dim_Payment` – stores payment method information.

   This design makes it easier and faster to analyze trip data from different business perspectives.

* Refer to `docs/star_schema.md` for the complete schema diagram and table relationships.

### Storage Optimization

To improve query performance, the `Fact_Trips` data is partitioned by **pickup year** and **pickup month**.

* **Example:** `data/warehouse/fact_trips/pickup_year=2025/pickup_month=1/`

* **Result:** If a user wants to analyze trips from a specific month, Spark reads only the relevant partition instead of scanning the entire dataset. This technique, known as **partition pruning**, significantly reduces processing time and compute costs, especially when working with many years of historical data.

### Data Quality Checks

To maintain data accuracy and reliability, the pipeline automatically identifies and separates invalid trip records.

* Records are rejected if:

  * `trip_duration <= 0`
  * `fare_amount < 0`
  * `trip_distance < 0`

* Valid records are stored in `data/warehouse/`.

* Invalid records are moved to `data/rejected/`.

Each rejected record includes a `rejection_reason` column explaining why it failed validation, making it easy to review and audit data quality issues later.

---

## Repository Structure

```
big-apple-scalability-challenge/
│
├── data/
│   ├── raw/               
│   ├── warehouse/        
│   └── rejected/          
│
├── config.py              
├── etl_pipeline.py        
├── generate_report.py     
├── requirements.txt       
│
├── sql/
│   ├── create_star_schema.sql  
│   ├── schema_report.sql       
│   └── analytics_queries.sql   
│
└── docs/
    ├── architecture.md    
    └── star_schema.md     
```


## How to Run the Pipeline

### Prerequisites
Make sure you have Python and Java installed.
```bash
pip install -r requirements.txt
```

### Windows Requirements

**Java 21 or Higher**

Spark requires Java to run. On some newer Java versions, you may encounter a `SecurityManager` error. This project automatically handles that issue by applying the `-Djava.security.manager=allow` setting, so no additional configuration is needed.

**Hadoop Winutils**

Spark on Windows requires two Hadoop support files:

* `winutils.exe`
* `hadoop.dll`

#### Setup Steps

* Create the folder:

```text
C:\hadoop\bin\
```

* Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x.
* Copy both files into the `C:\hadoop\bin\` folder.
* The project's `config.py` file will automatically detect and use these files, so no further setup is required.

### Run the ETL Pipeline

Run the main PySpark ETL job using the following command:

```bash
python etl_pipeline.py
```

The pipeline will automatically:

* Read the raw Parquet files from the `data/raw/` folder
* Apply all required data transformations
* Calculate fields such as trip duration and date attributes
* Perform data quality checks to identify invalid records
* Move rejected records to `data/rejected/`
* Load the cleaned data into the warehouse tables
* Partition the output by `pickup_year` and `pickup_month` to improve query performance and reduce processing time for future analytics queries


### Generate Analytics and Validate Performance

To analyze the processed data and verify that the partitioning strategy is working correctly, run the report generation script:

```bash
python generate_report.py
```

This script will:

* Generate a **Data Quality Report** showing the number of valid and rejected records.
* Display the **Partition Layout** used in the data warehouse.
* Run sample analytical queries on the cleaned data.
* Produce useful business insights such as:

  * Average Fare Amount
  * Total Number of Trips
  * Top Pickup Zones
  * Other trip-related statistics

By running these queries, you can also demonstrate how partitioning improves performance by allowing Spark to read only the required partitions instead of scanning the entire dataset. This results in faster query execution and lower processing costs.
