-- =============================================================================
-- Big Apple Scalability Challenge — Star Schema DDL
-- =============================================================================
-- File        : sql/create_star_schema.sql
-- Description : Creates all Star Schema tables for the NYC Taxi Data Warehouse.
--               Compatible with Snowflake, BigQuery (with minor syntax tweaks),
--               Redshift, and PostgreSQL.
--
-- Tables created
-- --------------
--   Dim_Date        — Date dimension (one row per calendar day)
--   Dim_Geography   — NYC TLC Taxi Zone dimension
--   Dim_Payment     — Payment method dimension
--   Fact_Trips      — Central fact table (one row per taxi trip)
--
-- Usage
-- -----
--   Snowflake : Run directly in a Snowflake worksheet
--   BigQuery  : Adjust data types (INT64, FLOAT64, STRING) as needed
--   Redshift  : Replace VARCHAR(MAX) with VARCHAR(255) or appropriate size
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Database / Schema setup  (Snowflake / Redshift)
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS nyc_taxi_dw;

-- Snowflake / Redshift
CREATE SCHEMA IF NOT EXISTS nyc_taxi_dw.star_schema;

-- Switch context
USE DATABASE nyc_taxi_dw;
USE SCHEMA star_schema;


-- ===========================================================================
-- DIMENSION TABLES
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Dim_Date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key    INTEGER         NOT NULL,   -- YYYYMMDD surrogate key e.g. 20220615
    date        DATE            NOT NULL,   -- full date value
    day         SMALLINT        NOT NULL,   -- 1–31
    month       SMALLINT        NOT NULL,   -- 1–12
    quarter     SMALLINT        NOT NULL,   -- 1–4
    year        SMALLINT        NOT NULL,   -- 4-digit year

    -- Derived convenience columns
    month_name  VARCHAR(10)     NOT NULL DEFAULT '',
    day_of_week SMALLINT        NOT NULL DEFAULT 0,   -- 1=Sun … 7=Sat
    is_weekend  BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

COMMENT ON TABLE  Dim_Date           IS 'Calendar date dimension — one row per day.';
COMMENT ON COLUMN Dim_Date.date_key  IS 'YYYYMMDD integer surrogate key.';
COMMENT ON COLUMN Dim_Date.quarter   IS 'Fiscal quarter (1–4).';
COMMENT ON COLUMN Dim_Date.is_weekend IS 'TRUE if Saturday or Sunday.';


-- ---------------------------------------------------------------------------
-- Dim_Geography  (NYC TLC Taxi Zones)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Dim_Geography (
    zone_key    INTEGER         NOT NULL,   -- LocationID from TLC zone lookup
    zone_name   VARCHAR(100)    NOT NULL,   -- e.g. 'Midtown Center'
    borough     VARCHAR(50)     NOT NULL,   -- e.g. 'Manhattan', 'Queens'
    service_zone VARCHAR(50)    NULL,       -- e.g. 'Yellow Zone', 'Boro Zone'

    CONSTRAINT pk_dim_geography PRIMARY KEY (zone_key)
);

COMMENT ON TABLE  Dim_Geography          IS 'NYC TLC taxi zone geography dimension.';
COMMENT ON COLUMN Dim_Geography.zone_key IS 'TLC LocationID (1–265).';
COMMENT ON COLUMN Dim_Geography.borough  IS 'NYC borough name or EWR/Unknown.';


-- ---------------------------------------------------------------------------
-- Dim_Payment
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Dim_Payment (
    payment_key     INTEGER         NOT NULL,   -- 1–6
    payment_type    VARCHAR(30)     NOT NULL,   -- e.g. 'Credit card', 'Cash'
    is_electronic   BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_dim_payment PRIMARY KEY (payment_key)
);

COMMENT ON TABLE  Dim_Payment               IS 'Payment method dimension.';
COMMENT ON COLUMN Dim_Payment.payment_key   IS 'TLC payment type code (1–6).';
COMMENT ON COLUMN Dim_Payment.is_electronic IS 'TRUE for credit card / electronic payments.';


-- ===========================================================================
-- FACT TABLE
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Fact_Trips
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Fact_Trips (
    trip_id             BIGINT          NOT NULL,   -- monotonically increasing surrogate
    pickup_date_key     INTEGER         NOT NULL,   -- FK → Dim_Date.date_key
    pickup_zone_key     INTEGER         NOT NULL,   -- FK → Dim_Geography.zone_key
    dropoff_zone_key    INTEGER         NOT NULL,   -- FK → Dim_Geography.zone_key
    payment_key         INTEGER         NOT NULL,   -- FK → Dim_Payment.payment_key

    -- Measures
    fare_amount         DECIMAL(10, 2)  NULL,
    trip_distance       DECIMAL(10, 2)  NULL,       -- miles
    trip_duration       DECIMAL(12, 2)  NULL,       -- seconds
    passenger_count     SMALLINT        NULL,
    total_amount        DECIMAL(10, 2)  NULL,
    tip_amount          DECIMAL(10, 2)  NULL,
    toll_amount         DECIMAL(10, 2)  NULL,
    congestion_surcharge DECIMAL(8, 2)  NULL,

    CONSTRAINT pk_fact_trips    PRIMARY KEY (trip_id),
    CONSTRAINT fk_date          FOREIGN KEY (pickup_date_key)  REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_pickup_zone   FOREIGN KEY (pickup_zone_key)  REFERENCES Dim_Geography(zone_key),
    CONSTRAINT fk_dropoff_zone  FOREIGN KEY (dropoff_zone_key) REFERENCES Dim_Geography(zone_key),
    CONSTRAINT fk_payment       FOREIGN KEY (payment_key)      REFERENCES Dim_Payment(payment_key)
);

-- Distribution / sort keys for Redshift performance
-- DISTSTYLE KEY DISTKEY (pickup_date_key)
-- SORTKEY (pickup_date_key, pickup_zone_key);

COMMENT ON TABLE  Fact_Trips                  IS 'Central fact table — one row per taxi trip.';
COMMENT ON COLUMN Fact_Trips.trip_id          IS 'Surrogate key generated by monotonically_increasing_id().';
COMMENT ON COLUMN Fact_Trips.trip_duration    IS 'Duration in seconds (dropoff - pickup).';
COMMENT ON COLUMN Fact_Trips.pickup_date_key  IS 'YYYYMMDD date key linking to Dim_Date.';


-- ===========================================================================
-- INDEXES (PostgreSQL / Redshift syntax)
-- ===========================================================================

-- Fact_Trips: common filter patterns
CREATE INDEX IF NOT EXISTS idx_fact_trips_date
    ON Fact_Trips(pickup_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_zone
    ON Fact_Trips(pickup_zone_key);

CREATE INDEX IF NOT EXISTS idx_fact_trips_dropoff_zone
    ON Fact_Trips(dropoff_zone_key);

CREATE INDEX IF NOT EXISTS idx_fact_trips_payment
    ON Fact_Trips(payment_key);

-- Dim_Date: often queried by year/month
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
    ON Dim_Date(year, month);


-- ===========================================================================
-- SEED DATA — Dim_Payment
-- ===========================================================================

INSERT INTO Dim_Payment (payment_key, payment_type, is_electronic)
VALUES
    (1, 'Credit card',  TRUE),
    (2, 'Cash',         FALSE),
    (3, 'No charge',    FALSE),
    (4, 'Dispute',      FALSE),
    (5, 'Unknown',      FALSE),
    (6, 'Voided trip',  FALSE)
ON CONFLICT (payment_key) DO NOTHING;


-- ===========================================================================
-- VIEWS — convenience analytics views
-- ===========================================================================

-- Monthly trip summary
CREATE OR REPLACE VIEW vw_monthly_summary AS
SELECT
    d.year,
    d.month,
    COUNT(*)                          AS total_trips,
    ROUND(AVG(f.fare_amount), 2)      AS avg_fare,
    ROUND(SUM(f.fare_amount), 2)      AS total_revenue,
    ROUND(AVG(f.trip_distance), 2)    AS avg_distance,
    ROUND(AVG(f.trip_duration / 60), 2) AS avg_duration_minutes
FROM Fact_Trips f
JOIN Dim_Date   d ON f.pickup_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Zone revenue summary
CREATE OR REPLACE VIEW vw_zone_revenue AS
SELECT
    g.zone_name,
    g.borough,
    COUNT(*)                          AS total_trips,
    ROUND(SUM(f.fare_amount), 2)      AS total_revenue,
    ROUND(AVG(f.fare_amount), 2)      AS avg_fare
FROM Fact_Trips   f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
GROUP BY g.zone_name, g.borough
ORDER BY total_revenue DESC;
