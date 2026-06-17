import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def build_dim_date(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("pickup_date").alias("date_key"),
        F.col("pickup_year").alias("year"),
        F.col("pickup_month").alias("month"),
        F.dayofmonth(F.col("tpep_pickup_datetime")).alias("day"),
        F.quarter(F.col("tpep_pickup_datetime")).alias("quarter")
    ).distinct()

def build_dim_payment(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("payment_type").alias("payment_key"),
        F.col("payment_type_desc").alias("payment_type")
    ).distinct()

def load_dim_zone(spark: SparkSession, lookup_path: str = "data/raw/taxi_zone_lookup.csv") -> DataFrame:
    try:
        dim_zone = spark.read.csv(lookup_path, header=True, inferSchema=True)
        return dim_zone.withColumnRenamed("LocationID", "zone_key")
    except Exception as e:
        print(f"[WARNING] Could not read taxi_zone_lookup.csv from {lookup_path}: {e}")
        return None

def build_fact_trips(df: DataFrame) -> DataFrame:
    return df.select(
        F.monotonically_increasing_id().alias("trip_id"),
        F.col("pickup_date").alias("pickup_date_key"),
        F.col("PULocationID").alias("pickup_zone_key"),
        F.col("DOLocationID").alias("dropoff_zone_key"),
        F.col("payment_type").alias("payment_key"),
        "fare_amount",
        "trip_distance",
        "trip_duration",
        "passenger_count",
        "pickup_year",
        "pickup_month"
    )

def write_local_warehouse(
    dim_date: DataFrame,
    dim_payment: DataFrame,
    dim_zone: DataFrame,
    fact_trips: DataFrame,
    rejected_df: DataFrame,
    warehouse_path: str,
    rejected_path: str
) -> None:
    print("[INFO] Writing Star Schema to local warehouse (Partitioning Fact table)...")
    dim_date.write.mode("overwrite").parquet(os.path.join(warehouse_path, "dim_date"))
    dim_payment.write.mode("overwrite").parquet(os.path.join(warehouse_path, "dim_payment"))
    if dim_zone is not None:
        dim_zone.write.mode("overwrite").parquet(os.path.join(warehouse_path, "dim_zone"))
        
    fact_trips.write \
        .partitionBy("pickup_year", "pickup_month") \
        .mode("overwrite") \
        .parquet(os.path.join(warehouse_path, "fact_trips"))
        
    print("[INFO] Writing rejected records to quarantine zone...")
    rejected_df.write.mode("overwrite").parquet(rejected_path)
    
    print("[INFO] Local warehouse written successfully.")

def write_snowflake_warehouse(
    dim_date: DataFrame,
    dim_payment: DataFrame,
    fact_trips: DataFrame,
    dim_zone: DataFrame = None
) -> None:
    sf_url = os.getenv("SNOWFLAKE_ACCOUNT")
    if sf_url and not sf_url.endswith(".snowflakecomputing.com"):
        sf_url_full = f"{sf_url}.snowflakecomputing.com"
    else:
        sf_url_full = sf_url

    sf_user = os.getenv("SNOWFLAKE_USER")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")
    sf_database = os.getenv("SNOWFLAKE_DATABASE")
    sf_schema = os.getenv("SNOWFLAKE_SCHEMA")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    placeholders = ["your_snowflake_account", "your_username", "your_password"]
    has_real_creds = (
        sf_url and sf_user and sf_password
        and sf_url not in placeholders
        and sf_user not in placeholders
        and sf_password not in placeholders
    )

    if has_real_creds:
        print("[INFO] Snowflake credentials detected — writing to Snowflake...")
        snowflake_options = {
            "sfURL": sf_url_full,
            "sfUser": sf_user,
            "sfPassword": sf_password,
            "sfDatabase": sf_database,
            "sfSchema": sf_schema,
            "sfWarehouse": sf_warehouse,
            "JDBC_QUERY_RESULT_FORMAT": "JSON",
            "dbtable": ""
        }

        def write_df(df: DataFrame, table_name: str):
            opts = snowflake_options.copy()
            opts["dbtable"] = table_name
            df.write.format("snowflake") \
                .options(**opts) \
                .mode("overwrite") \
                .save()

        write_df(dim_date, "dim_date")
        write_df(dim_payment, "dim_payment")
        write_df(fact_trips, "fact_trips")
        if dim_zone is not None:
            write_df(dim_zone, "dim_zone")
        print("[INFO] Snowflake write completed.")
    else:
        print("[INFO] Snowflake credentials not fully configured — skipping Snowflake write.")

