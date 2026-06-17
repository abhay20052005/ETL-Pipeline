from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def trip_duration(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "trip_duration", 
        F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    )

def time(df: DataFrame) -> DataFrame:
    return df \
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime"))) \
        .withColumn("pickup_year", F.year(F.col("tpep_pickup_datetime"))) \
        .withColumn("pickup_month", F.month(F.col("tpep_pickup_datetime")))

def payment(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit card")
        .when(F.col("payment_type") == 2, "Cash")
        .when(F.col("payment_type") == 3, "No charge")
        .when(F.col("payment_type") == 4, "Dispute")
        .otherwise("Unknown")
    )




