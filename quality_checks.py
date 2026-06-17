from typing import Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    is_valid_condition = (
        (F.col("trip_duration") > 0) & 
        (F.col("fare_amount") >= 0) & 
        (F.col("trip_distance") >= 0)
    )
    
    valid_df = df.filter(is_valid_condition)
    rejected_df = df.filter(~is_valid_condition).withColumn(
        "rejection_reason", F.lit("Failed quality checks: duration<=0 OR fare<0 OR distance<0")
    )
    
    return valid_df, rejected_df

def quality(total_records: int, valid_count: int, rejected_count: int) -> None:
    pct_valid = (valid_count / total_records * 100) if total_records > 0 else 0.0
    pct_rejected = (rejected_count / total_records * 100) if total_records > 0 else 0.0
    
    print("\n=============================================")
    print("DATA QUALITY REPORT")
    print("=============================================")
    print(f"Total Records:    {total_records:,}")
    print(f"Valid Records:    {valid_count:,} ({pct_valid:.2f}%)")
    print(f"Rejected Records: {rejected_count:,} ({pct_rejected:.2f}%)")
    print("=============================================\n")
