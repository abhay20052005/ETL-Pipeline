import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

def main():
    print("[INFO] Generating Analytics Report from Data Warehouse...")
    
    # We are initializing our spark session 
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Warehouse_Report") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Read Star Schema tables
    print(f"[INFO] Reading Star Schema from {config.WAREHOUSE_PATH}")
    try:
        fact_trips = spark.read.parquet(config.WAREHOUSE_PATH + "fact_trips")
        dim_payment = spark.read.parquet(config.WAREHOUSE_PATH + "dim_payment")
        try:
            dim_zone = spark.read.parquet(config.WAREHOUSE_PATH + "dim_zone")
        except Exception:
            dim_zone = None
    except Exception as e:
        print("[ERROR] Warehouse not found. Did you run etl_pipeline.py first?")
        return

    total_trips = fact_trips.count()
    
    print("\n=============================================")
    print("WAREHOUSE STAR SCHEMA REPORT")
    print("=============================================")
    print(f"Total Trips in Fact Table: {total_trips:,}\n")

    print("--- Fact Table Schema ---")
    fact_trips.printSchema()

    # Average Fare & Trip Distance
    print("--- Averages ---")
    avg_stats = fact_trips.select(
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_distance")
    ).collect()[0]
    
    print(f"Average Fare:     ${avg_stats['avg_fare']:.2f}")
    print(f"Average Distance: {avg_stats['avg_distance']:.2f} miles\n")

    # Top Pickup Zones
    print("--- Top Pickup Zones ---")
    if dim_zone is not None:
        top_zones = fact_trips.join(dim_zone, fact_trips.pickup_zone_key == dim_zone.zone_key) \
            .groupBy("Zone").count() \
            .orderBy(col("count").desc()).limit(5)
        for row in top_zones.collect():
            print(f"Zone {row['Zone']:>25}: {row['count']:,} trips")
    else:
        top_zones = fact_trips.groupBy("pickup_zone_key").count() \
            .orderBy(col("count").desc()).limit(5)
        for row in top_zones.collect():
            print(f"Zone {row['pickup_zone_key']:>3}: {row['count']:,} trips")
    
    print("\n--- Analytics: Payment Method Breakdown ---")
    # Join with Dim_Payment
    payment_stats = fact_trips.join(dim_payment, "payment_key") \
        .groupBy("payment_type").count() \
        .orderBy(col("count").desc())
    
    for row in payment_stats.collect():
        print(f"{row['payment_type']:<15}: {row['count']:,} trips")
        
    print("=============================================\n")
    spark.stop()

if __name__ == "__main__":
    main()
