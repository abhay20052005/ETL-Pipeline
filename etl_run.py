import sys
import os

# Environment configurations for Spark Security Manager, Hadoop, and Python on Windows
os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow --add-opens=java.base/java.nio=ALL-UNNAMED"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow --add-opens=java.base/java.nio=ALL-UNNAMED"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";C:/hadoop/bin"
# Ensure PySpark uses the current Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Force UTF-8 output so emojis/special chars don't crash on Windows console
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Ensure we can import project modules (no need for src folder)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import config
from transformations import compute_trip_duration, add_time_dimensions, map_payment_types
from quality_checks import split_valid_invalid_records, print_quality_report
from warehouse_loader import (
    build_dim_date,
    build_dim_payment,
    load_dim_zone,
    build_fact_trips,
    write_local_warehouse,
    write_snowflake_warehouse
)
from ddl_generator import generate_ddl_statement, write_ddl_report

# Fail fast if winutils.exe is missing on Windows
_winutils = os.path.join("C:/hadoop", "bin", "winutils.exe")
if sys.platform == "win32" and not os.path.isfile(_winutils):
    raise RuntimeError(
        f"winutils.exe not found at {_winutils}. "
        "Download it from https://github.com/cdarlint/winutils and place it in C:/hadoop/bin/"
    )

def main():
    print("[INFO] Starting the modular NYC Taxi ETL Pipeline...")

    # Initialize Spark Session with Snowflake connector (local JARs)
    snowflake_jars = ",".join([
        os.path.join(config.PROJECT_ROOT, "jars", "snowflake-jdbc-3.13.17.jar"),
        os.path.join(config.PROJECT_ROOT, "jars", "spark-snowflake_2.13-3.1.9.jar")
    ])

    builder = SparkSession.builder \
        .appName("NYC_Taxi_Scalability_Challenge_Modular") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow --add-opens=java.base/java.nio=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow --add-opens=java.base/java.nio=ALL-UNNAMED") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.jars", snowflake_jars)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. READ (Ingestion)
    input_path = config.RAW_DATA_PATH
    print(f"[INFO] Reading data from {input_path}")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[ERROR] Error reading data from {input_path}. Make sure your parquet files are in data/raw/")
        print(f"Details: {e}")
        spark.stop()
        sys.exit(1)

    total_records = df.count()
    print(f"[INFO] Total raw records found: {total_records:,}")

    # 2. TRANSFORM
    print("[INFO] Applying transformations...")
    df = compute_trip_duration(df)
    df = add_time_dimensions(df)
    df = map_payment_types(df)

    # 3. VALIDATE
    print("[INFO] Running Data Quality Checks...")
    valid_df, rejected_df = split_valid_invalid_records(df)

    # Force count evaluation to log validation results
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    print_quality_report(total_records, valid_count, rejected_count)

    # 4. BUILD STAR SCHEMA
    print("[INFO] Building Star Schema Dimension and Fact tables...")
    dim_date = build_dim_date(valid_df)
    dim_payment = build_dim_payment(valid_df)
    dim_zone = load_dim_zone(spark)
    fact_trips = build_fact_trips(valid_df)

    # 5. PREVIEW SCHEMA DATA
    print("\n[INFO] Previewing Star Schema Data:")
    print("--- Dim_Date ---")
    dim_date.show(5)
    print("--- Dim_Payment ---")
    dim_payment.show(5)
    if dim_zone is not None:
        print("--- Dim_Zone ---")
        dim_zone.show(5)
    print("--- Fact_Trips ---")
    fact_trips.show(5)

    # 6. WRITE DATA
    # Write to local parquet warehouse and rejected data path
    write_local_warehouse(
        dim_date=dim_date,
        dim_payment=dim_payment,
        dim_zone=dim_zone,
        fact_trips=fact_trips,
        rejected_df=rejected_df,
        warehouse_path=config.WAREHOUSE_PATH,
        rejected_path=config.REJECTED_PATH
    )

    # Write to remote Snowflake warehouse (optional)
    write_snowflake_warehouse(
        dim_date=dim_date,
        dim_payment=dim_payment,
        fact_trips=fact_trips,
        dim_zone=dim_zone
    )

    # 7. GENERATE DDL REPORT
    print("[INFO] Generating Snowflake DDL schema report...")
    ddl_statements = [
        generate_ddl_statement(dim_date, "dim_date"),
        generate_ddl_statement(dim_payment, "dim_payment"),
        generate_ddl_statement(fact_trips, "fact_trips")
    ]
    if dim_zone is not None:
        ddl_statements.append(generate_ddl_statement(dim_zone, "dim_zone"))

    write_ddl_report(ddl_statements, config.DDL_OUTPUT_PATH)

    spark.stop()
    print("\n[SUCCESS] Modular pipeline execution completed successfully!")

if __name__ == "__main__":
    main()
