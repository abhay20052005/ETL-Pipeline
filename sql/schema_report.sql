CREATE TABLE IF NOT EXISTS dim_date (
    "date_key" DATE,
    "year" INT,
    "month" INT,
    "day" INT,
    "quarter" INT
);

CREATE TABLE IF NOT EXISTS dim_payment (
    "payment_key" BIGINT,
    "payment_type" VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS fact_trips (
    "trip_id" BIGINT,
    "pickup_date_key" DATE,
    "pickup_zone_key" INT,
    "dropoff_zone_key" INT,
    "payment_key" BIGINT,
    "fare_amount" DOUBLE PRECISION,
    "trip_distance" DOUBLE PRECISION,
    "trip_duration" BIGINT,
    "passenger_count" BIGINT,
    "pickup_year" INT,
    "pickup_month" INT
);

CREATE TABLE IF NOT EXISTS dim_zone (
    "zone_key" INT,
    "Borough" VARCHAR(256),
    "Zone" VARCHAR(256),
    "service_zone" VARCHAR(256)
);