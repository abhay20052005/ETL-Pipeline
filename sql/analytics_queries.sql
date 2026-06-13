-- Average Fare — Last Month
SELECT
    d.year,
    d.month,
    COUNT(*)                          AS total_trips,
    ROUND(AVG(f.fare_amount),  2)     AS avg_fare_amount,
    ROUND(AVG(f.total_amount), 2)     AS avg_total_amount,
    ROUND(AVG(f.tip_amount),   2)     AS avg_tip_amount
FROM Fact_Trips f
JOIN Dim_Date   d ON f.pickup_date_key = d.date_key
WHERE
    -- last month (compatible with most SQL dialects)
    d.year  = EXTRACT(YEAR  FROM CURRENT_DATE - INTERVAL '1 month')
    AND d.month = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
GROUP BY d.year, d.month;



-- Top 10 Pickup Zones by Trip Count (all time)

SELECT
    g.zone_name,
    g.borough,
    COUNT(*)                          AS total_trips,
    ROUND(SUM(f.fare_amount),  2)     AS total_revenue,
    ROUND(AVG(f.fare_amount),  2)     AS avg_fare,
    ROUND(AVG(f.trip_distance), 2)    AS avg_distance_miles
FROM Fact_Trips    f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
GROUP BY g.zone_name, g.borough
ORDER BY total_trips DESC
LIMIT 10;


-- Revenue by Borough

SELECT
    g.borough,
    COUNT(*)                          AS total_trips,
    ROUND(SUM(f.fare_amount),  2)     AS total_fare_revenue,
    ROUND(SUM(f.total_amount), 2)     AS total_gross_revenue,
    ROUND(AVG(f.fare_amount),  2)     AS avg_fare,
    ROUND(AVG(f.trip_distance), 2)    AS avg_distance_miles,
    ROUND(SUM(f.fare_amount) * 100.0
          / SUM(SUM(f.fare_amount)) OVER (), 2) AS revenue_pct
FROM Fact_Trips    f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
GROUP BY g.borough
ORDER BY total_fare_revenue DESC;


-- Monthly Trip Trends 

WITH monthly AS (
    SELECT
        d.year,
        d.month,
        COUNT(*)                        AS total_trips,
        ROUND(SUM(f.fare_amount), 2)    AS total_revenue
    FROM Fact_Trips f
    JOIN Dim_Date   d ON f.pickup_date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    m.year,
    m.month,
    m.total_trips,
    m.total_revenue,
    -- Month-over-month change
    m.total_trips - LAG(m.total_trips)    OVER (ORDER BY m.year, m.month) AS mom_trip_delta,
    m.total_revenue - LAG(m.total_revenue) OVER (ORDER BY m.year, m.month) AS mom_revenue_delta,
    -- Year-over-year
    m.total_trips - LAG(m.total_trips)    OVER (PARTITION BY m.month ORDER BY m.year) AS yoy_trip_delta,
    m.total_revenue - LAG(m.total_revenue) OVER (PARTITION BY m.month ORDER BY m.year) AS yoy_revenue_delta
FROM monthly m
ORDER BY m.year, m.month;


-- Average Trip Duration by Pickup Zone (Top 20 busiest zones)

SELECT
    g.zone_name,
    g.borough,
    COUNT(*)                                    AS total_trips,
    ROUND(AVG(f.trip_duration / 60.0), 2)       AS avg_duration_minutes,
    ROUND(PERCENTILE_CONT(0.5)
          WITHIN GROUP (ORDER BY f.trip_duration / 60.0), 2)
                                                AS median_duration_minutes,
    ROUND(AVG(f.trip_distance), 2)              AS avg_distance_miles,
    ROUND(AVG(f.fare_amount),   2)              AS avg_fare
FROM Fact_Trips    f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
WHERE f.trip_duration > 0          -- exclude bad records (should already be clean)
GROUP BY g.zone_name, g.borough
ORDER BY total_trips DESC
LIMIT 20;


-- BONUS — Peak Hour Analysis (trips per hour of day)

SELECT
    EXTRACT(HOUR FROM d.date)         AS pickup_hour,
    COUNT(*)                          AS total_trips,
    ROUND(AVG(f.fare_amount), 2)      AS avg_fare,
    ROUND(AVG(f.trip_duration / 60.0), 2) AS avg_duration_minutes
FROM Fact_Trips f
JOIN Dim_Date   d ON f.pickup_date_key = d.date_key
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- BONUS — Payment Method Breakdown

SELECT
    p.payment_type,
    COUNT(*)                          AS total_trips,
    ROUND(SUM(f.fare_amount),  2)     AS total_revenue,
    ROUND(AVG(f.fare_amount),  2)     AS avg_fare,
    ROUND(AVG(f.tip_amount),   2)     AS avg_tip,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 2) AS pct_of_trips
FROM Fact_Trips  f
JOIN Dim_Payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_type
ORDER BY total_trips DESC;


-- BONUS — Top Pickup-to-Dropoff Corridors

SELECT
    pu.zone_name    AS pickup_zone,
    pu.borough      AS pickup_borough,
    do.zone_name    AS dropoff_zone,
    do.borough      AS dropoff_borough,
    COUNT(*)        AS total_trips,
    ROUND(AVG(f.fare_amount), 2)   AS avg_fare,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles
FROM Fact_Trips    f
JOIN Dim_Geography pu ON f.pickup_zone_key  = pu.zone_key
JOIN Dim_Geography do ON f.dropoff_zone_key = do.zone_key
GROUP BY pu.zone_name, pu.borough, do.zone_name, do.borough
ORDER BY total_trips DESC
LIMIT 20;


-- BONUS — Revenue Ranking by Zone with Window Functions

SELECT
    g.borough,
    g.zone_name,
    COUNT(*)                                              AS total_trips,
    ROUND(SUM(f.fare_amount), 2)                          AS total_revenue,
    RANK()        OVER (ORDER BY SUM(f.fare_amount) DESC) AS global_revenue_rank,
    DENSE_RANK()  OVER (PARTITION BY g.borough
                        ORDER BY SUM(f.fare_amount) DESC) AS borough_revenue_rank
FROM Fact_Trips    f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
GROUP BY g.borough, g.zone_name
ORDER BY global_revenue_rank;
