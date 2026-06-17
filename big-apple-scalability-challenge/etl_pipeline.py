import sys
import os
import config
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, year, month, lit, when, dayofmonth, quarter, monotonically_increasing_id

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";C:/hadoop/bin"
_winutils = os.path.join("C:/hadoop", "bin", "winutils.exe")
if sys.platform == "win32" and not os.path.isfile(_winutils):
    raise RuntimeError(
        f"winutils.exe not found at {_winutils}. "
        "Download it from https://github.com/cdarlint/winutils and place it in C:/hadoop/bin/"
    )

def main():
    print("Starting the NYC Taxi ETL Pipeline...")
    snowflake_jars = ",".join([
        os.path.join(config.PROJECT_ROOT, "jars", "snowflake-jdbc-3.13.17.jar"),
        os.path.join(config.PROJECT_ROOT, "jars", "spark-snowflake_2.13-3.1.9.jar")
    ])
    
    builder = SparkSession.builder \
        .appName("NYC_Taxi_Scalability_Challenge") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.jars", snowflake_jars)
        
    spark = builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    input_path = config.RAW_DATA_PATH
    print(f"Reading data from {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print("Error reading data. Make sure your parquet files are in data/raw/")
        sys.exit(1)

    total_records = df.count()
    print(f"Total raw records found: {total_records:,}")
    print("Applying transformations and creating Star Schema columns...")
    df_transformed = df.withColumn(
        "trip_duration", 
        unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))
    )
    df_transformed = df_transformed \
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
    df_transformed = df_transformed.withColumn(
        "payment_type_desc",
        when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .otherwise("Unknown")
    )
    print("Running Data Quality Checks...")
    is_valid_condition = (
        (col("trip_duration") > 0) & 
        (col("fare_amount") >= 0) & 
        (col("trip_distance") >= 0)
    )
    valid_df = df_transformed.filter(is_valid_condition)
    rejected_df = df_transformed.filter(~is_valid_condition).withColumn(
        "rejection_reason", lit("Failed quality checks: duration<=0 OR fare<0 OR distance<0")
    )

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()

    print("\n=============================================")
    print("DATA QUALITY REPORT")
    print("=============================================")
    print(f"Total Records:    {total_records:,}")
    print(f"Valid Records:    {valid_count:,} ({valid_count/total_records*100:.2f}%)")
    print(f"Rejected Records: {rejected_count:,} ({rejected_count/total_records*100:.2f}%)")
    print("=============================================\n")
    print("Building Star Schema Dimension and Fact tables...")
    dim_date = valid_df.select(
        col("pickup_date").alias("date_key"),
        col("pickup_year").alias("year"),
        col("pickup_month").alias("month"),
        dayofmonth(col("tpep_pickup_datetime")).alias("day"),
        quarter(col("tpep_pickup_datetime")).alias("quarter")
    ).distinct()

    dim_payment = valid_df.select(
        col("payment_type").alias("payment_key"),
        col("payment_type_desc").alias("payment_type")
    ).distinct()
    zone_lookup_path = os.path.join("data", "raw", "taxi_zone_lookup.csv")
    try:
        dim_zone = spark.read.csv(zone_lookup_path, header=True, inferSchema=True)
        dim_zone = dim_zone.withColumnRenamed("LocationID", "zone_key")
    except Exception as e:
        print(f"Could not read taxi_zone_lookup.csv: {e}")
        dim_zone = None
    fact_trips = valid_df.select(
        monotonically_increasing_id().alias("trip_id"),
        col("pickup_date").alias("pickup_date_key"),
        col("PULocationID").alias("pickup_zone_key"),
        col("DOLocationID").alias("dropoff_zone_key"),
        col("payment_type").alias("payment_key"),
        "fare_amount",
        "trip_distance",
        "trip_duration",
        "passenger_count",
        "pickup_year",
        "pickup_month"
    )

    print("\nPreviewing Star Schema Data:")
    print("--- Dim_Date ---")
    dim_date.show(5)
    print("--- Dim_Payment ---")
    dim_payment.show(5)
    if dim_zone is not None:
        print("--- Dim_Zone ---")
        dim_zone.show(5)
    print("--- Fact_Trips ---")
    fact_trips.show(5)
    warehouse_path = config.WAREHOUSE_PATH
    rejected_path = config.REJECTED_PATH

    print("Writing Star Schema to local warehouse (Partitioning Fact table)...")
    dim_date.write.mode("overwrite").parquet(warehouse_path + "dim_date")
    dim_payment.write.mode("overwrite").parquet(warehouse_path + "dim_payment")
    if dim_zone is not None:
        dim_zone.write.mode("overwrite").parquet(warehouse_path + "dim_zone")
        
    fact_trips.write \
        .partitionBy("pickup_year", "pickup_month") \
        .mode("overwrite") \
        .parquet(warehouse_path + "fact_trips")

    print("Writing rejected records to quarantine zone...")
    rejected_df.write.mode("overwrite").parquet(rejected_path)

    print("Local warehouse written successfully.")
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
        print("Snowflake credentials detected — writing to Snowflake...")
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

        def write_df(df, table_name: str):
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
        print("Snowflake write completed.")
    else:
        print("Snowflake credentials not fully configured — skipping Snowflake write.")

    print("\nPipeline completed successfully!")
    print("Your Star Schema is ready in data/warehouse/ (Dim_Date, Dim_Payment, Dim_Zone, Fact_Trips).")
    def generate(df, table_name):
        type_map = {
            "long": "NUMBER",
            "integer": "NUMBER",
            "string": "VARCHAR(256)",
            "double": "FLOAT",
            "float": "FLOAT",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
            "boolean": "BOOLEAN"
        }
        columns = []
        for field in df.schema.fields:
            spark_type = field.dataType.typeName()
            snowflake_type = type_map.get(spark_type, "VARCHAR(256)")
            columns.append(f'"{field.name}" {snowflake_type}')
        cols_sql = ",\n    ".join(columns)
        return f'CREATE TABLE IF NOT EXISTS {table_name} (\n    {cols_sql}\n);'

    ddl_statements = [
        generate(dim_date, "dim_date"),
        generate(dim_payment, "dim_payment"),
        generate(fact_trips, "fact_trips")
    ]
    if dim_zone is not None:
        ddl_statements.append(generate(dim_zone, "dim_zone"))
    ddl_path = config.DDL_OUTPUT_PATH
    with open(ddl_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(ddl_statements))
    print("Star‑schema DDL written to:", ddl_path)
    spark.stop()

if __name__ == "__main__":
    main()
