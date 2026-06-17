import sys
import os
os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow --add-opens=java.base/java.nio=ALL-UNNAMED"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow --add-opens=java.base/java.nio=ALL-UNNAMED"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";C:/hadoop/bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import config
from transformations import trip_duration, time, payment
from quality_checks import valid_invalid, quality
from warehouse_loader import (
    build_dim_date,
    build_dim_payment,
    load_dim_zone,
    build_fact_trips,
    write_local_warehouse,
    write_snowflake_warehouse
)
from ddl_generator import generateddl, writeddl
_winutils = os.path.join("C:/hadoop", "bin", "winutils.exe")
if sys.platform == "win32" and not os.path.isfile(_winutils):
    raise RuntimeError(
        f"winutils.exe not found at {_winutils}. "
        "Download it from https://github.com/cdarlint/winutils and place it in C:/hadoop/bin/"
    )

def main():
    print("Starting the modular NYC Taxi ETL Pipeline...")
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
    input_path = config.RAW_DATA_PATH
    print(f"Reading data from {input_path}")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading data from {input_path}. Make sure your parquet files are in data/raw/")
        print(f"Details: {e}")
        spark.stop()
        sys.exit(1)

    total_records = df.count()
    print(f"Total raw records found: {total_records:,}")
    print("Applying transformations...")
    df = trip_duration(df)
    df = time(df)
    df = payment(df)
    print("Running Data Quality Checks...")
    valid_df, rejected_df = valid_invalid(df)

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    quality(total_records, valid_count, rejected_count)

    print("Building Star Schema Dimension and Fact tables...")
    dim_date = build_dim_date(valid_df)
    dim_payment = build_dim_payment(valid_df)
    dim_zone = load_dim_zone(spark)
    fact_trips = build_fact_trips(valid_df)

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

    write_local_warehouse(
        dim_date=dim_date,
        dim_payment=dim_payment,
        dim_zone=dim_zone,
        fact_trips=fact_trips,
        rejected_df=rejected_df,
        warehouse_path=config.WAREHOUSE_PATH,
        rejected_path=config.REJECTED_PATH
    )

    write_snowflake_warehouse(
        dim_date=dim_date,
        dim_payment=dim_payment,
        fact_trips=fact_trips,
        dim_zone=dim_zone
    )

    print("Generating Snowflake DDL schema report...")
    ddl_statements = [
        generateddl(dim_date, "dim_date"),
        generateddl(dim_payment, "dim_payment"),
        generateddl(fact_trips, "fact_trips")
    ]
    if dim_zone is not None:
        ddl_statements.append(generateddl(dim_zone, "dim_zone"))

    writeddl(ddl_statements, config.DDL_OUTPUT_PATH)

    spark.stop()
    print("\nModular pipeline execution completed successfully!")

if __name__ == "__main__":
    main()
