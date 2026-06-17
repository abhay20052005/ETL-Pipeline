'''Average Fare'''
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
    d.year  = EXTRACT(YEAR  FROM CURRENT_DATE - INTERVAL '1 month')
    AND d.month = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
GROUP BY d.year, d.month;



'''Top 10 Pickup Zones on basis of trip count'''

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


'''Revenue'''

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
'''Average trip '''
SELECT
    g.zone_name,
    g.borough,
    count(*) as total_trips,
    round(avg(f.trip_duration / 60.0), 2) as avg_duration_minutes,
    round(
        percentile_cont(0.5) within group (order by f.trip_duration / 60.0), 
        2
    ) as median_duration_minutes,
    round(avg(f.trip_distance), 2) as avg_distance_miles,
    round(avg(f.fare_amount), 2) as avg_fare
FROM Fact_Trips f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
WHERE f.trip_duration > 0
GROUP  BY g.zone_name, g.borough
ORDER BY total_trips DESC
LIMIT 20;

'''Payment Method'''

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