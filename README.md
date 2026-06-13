# ETL-Pipeline
This repo delivers a scalable PySpark ETL for NYC TLC Parquet trips: it cleans invalid records (negative fares, non-positive durations/distances), writes clean data to a star-schema (Fact_Trips partitioned by pickup_year / pickup_month and Date/Geography/Payment dims), quarantines rejections with reasons, and includes DDL, docs, reports.
