CREATE TABLE IF NOT EXISTS dim_date (
    "date_key" DATE,
    "year" NUMBER,
    "month" NUMBER,
    "day" NUMBER,
    "quarter" NUMBER
);

CREATE TABLE IF NOT EXISTS dim_payment (
    "payment_key" NUMBER,
    "payment_type" VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS fact_trips (
    "trip_id" NUMBER,
    "pickup_date_key" DATE,
    "pickup_zone_key" NUMBER,
    "dropoff_zone_key" NUMBER,
    "payment_key" NUMBER,
    "fare_amount" FLOAT,
    "trip_distance" FLOAT,
    "trip_duration" NUMBER,
    "passenger_count" NUMBER,
    "pickup_year" NUMBER,
    "pickup_month" NUMBER
);

CREATE TABLE IF NOT EXISTS dim_zone (
    "zone_key" NUMBER,
    "Borough" VARCHAR(256),
    "Zone" VARCHAR(256),
    "service_zone" VARCHAR(256)
);