# Star Schema Design 

The data model follows the **Star Schema** methodology: a **Fact table** at the center connected to **Dimension tables** surrounding it. This design is optimised (OLAP) over (OLTP).

## Star Schema Diagram

```
                         ┌─────────────────────────┐
                         │       Dim_Date          │
                         ├─────────────────────────┤
                         │ PK  date_key  INTEGER   │
                         │     date      DATE      │
                         │     day       SMALLINT  │
                         │     month     SMALLINT  │
                         │     quarter   SMALLINT  │
                         │     year      SMALLINT  │
                         │     month_name VARCHAR  │
                         │     day_of_week SMALLINT│
                         │     is_weekend BOOLEAN  │
                         └────────────┬────────────┘
                                      │
                                      │ FK pickup_date_key
                                      │
┌──────────────────────┐              │              ┌──────────────────────┐
│    Dim_Geography     │              │              │    Dim_Geography     │
│  (Pickup Zones)      │              │              │  (Dropoff Zones)     │
├──────────────────────┤              │              ├──────────────────────┤
│ PK  zone_key  INT    ├──────────────┼──────────────┤ PK  zone_key  INT    │
│     zone_name VARCHAR│              │              │     zone_name VARCHAR│
│     borough   VARCHAR│              │              │     borough   VARCHAR│
│     service_zone     │              │              │     service_zone     │
└──────────────────────┘              │              └──────────────────────┘
        FK pickup_zone_key            │                   FK dropoff_zone_key
                                      ▼
                         ┌─────────────────────────────────┐
                         │           Fact_Trips            │  ← Central Fact Table
                         ├─────────────────────────────────┤
                         │ PK  trip_id           BIGINT    │
                         │ FK  pickup_date_key   INTEGER   │
                         │ FK  pickup_zone_key   INTEGER   │
                         │ FK  dropoff_zone_key  INTEGER   │
                         │ FK  payment_key       INTEGER   │
                         │ ── MEASURES ───────────────── ─ │
                         │     fare_amount       DECIMAL   │
                         │     trip_distance     DECIMAL   │
                         │     trip_duration     DECIMAL   │
                         │     passenger_count   SMALLINT  │
                         │     total_amount      DECIMAL   │
                         │     tip_amount        DECIMAL   │
                         │     toll_amount       DECIMAL   │
                         │     congestion_surch  DECIMAL   │
                         └────────────┬────────────────────┘
                                      │
                                      │ FK payment_key
                                      │
                         ┌────────────▼────────────┐
                         │       Dim_Payment       │
                         ├─────────────────────────┤
                         │ PK  payment_key INTEGER │
                         │     payment_type VARCHAR│
                         │     is_electronic BOOL  │
                         └─────────────────────────┘
```


## Table Descriptions

### Fact_Trips (Grain: one row per taxi trip)

| Column | Type | Description |
|--------|------|-------------|
| `trip_id` | BIGINT | Surrogate primary key |
| `pickup_date_key` | INTEGER | FK → Dim_Date |
| `pickup_zone_key` | INTEGER | FK → Dim_Geography |
| `dropoff_zone_key` | INTEGER | FK → Dim_Geography |
| `payment_key` | INTEGER | FK → Dim_Payment |
| `fare_amount` | DECIMAL(10,2) | Base fare in USD |
| `trip_distance` | DECIMAL(10,2) | Distance in miles |
| `trip_duration` | DECIMAL(12,2) | Duration in seconds |
| `passenger_count` | SMALLINT | Number of passengers |
| `total_amount` | DECIMAL(10,2) | Total charged |
| `tip_amount` | DECIMAL(10,2) | Tip paid |


### Dim_Date

Slowly Changing Dimension

| Column | Type | Example |
|--------|------|---------|
| `date_key` | INTEGER | `20220615` |
| `date` | DATE | `2022-06-15` |
| `day` | SMALLINT | `15` |
| `month` | SMALLINT | `6` |
| `quarter` | SMALLINT | `2` |
| `year` | SMALLINT | `2022` |
| `month_name` | VARCHAR | `June` |
| `day_of_week` | SMALLINT | `4` (Wednesday) |
| `is_weekend` | BOOLEAN | `FALSE` |


### Dim_Geography

| Column | Type | Example |
|--------|------|---------|
| `zone_key` | INTEGER | `161` |
| `zone_name` | VARCHAR | `Midtown Center` |
| `borough` | VARCHAR | `Manhattan` |
| `service_zone` | VARCHAR | `Yellow Zone` |

**Used twice** in Fact_Trips:
- `pickup_zone_key` — where the trip started
- `dropoff_zone_key` — where the trip ended

### Dim_Payment

| Column | Type | Example |
|--------|------|---------|
| `payment_key` | INTEGER | `1` |
| `payment_type` | VARCHAR | `Credit card` |
| `is_electronic` | BOOLEAN | `TRUE` |


## Design Decisions

### Why Star Schema

| Criterion | Star Schema | Snowflake Schema | 3NF |
|-----------|------------|-----------------|-----|
| Query complexity | **Simple JOINs** | Complex JOINs | Most complex |
| Query speed | **Fastest** | Moderate | Slowest for analytics |
| Storage | Slight redundancy | Less redundancy | Normalised |
| BI tool compatibility | **Excellent** | Good | Poor |

### Why is a Star Schema used?

A **Star Schema** is a popular design for analytics because it makes queries faster and easier to understand. Instead of joining many complex tables, analysts can work with one central fact table connected to a few dimension tables. Although it uses a little more storage space, the cost is negligible compared to the improvement in reporting and dashboard performance.

### Why is `Dim_Geography` used twice in `Fact_Trips`?

`Dim_Geography` is used twice because a trip has **two different locations**: where it starts and where it ends. Rather than creating two separate geography tables with the same information, we reuse the same table for both purposes.

This approach is known as a **role-playing dimension**. The table plays one role as the **pickup location** and another role as the **dropoff location**.

This allows us to answer business questions such as:

* Which areas generate the highest number of pickups?
* Which areas receive the most dropoffs?
* What are the most common routes between locations?
* How many trips start in one zone and end in another?

By reusing the same geography table, we avoid duplicating data, keep the model cleaner, and make it easier to maintain.

```sql
-- Which pickup zones send the most trips to JFK?
WHERE dropoff_zone_key = 132  -- JFK Airport
```

### Slowly Changing Dimensions

- **Dim_Date**: Type 0 — Never changes (historical dates are immutable)
- **Dim_Geography**: Type 1 — Overwrite if zone names change (rare)
- **Dim_Payment**: Type 0 — TLC payment codes are stable


## Sample Analytics Queries

```sql
'''Average fare last month'''
SELECT AVG(f.fare_amount)
FROM Fact_Trips f
JOIN Dim_Date d ON f.pickup_date_key = d.date_key
WHERE d.year = 2022 AND d.month = 5;

'''Top 5 pickup zones by revenue'''
SELECT g.zone_name, SUM(f.fare_amount) AS revenue
FROM Fact_Trips f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
GROUP BY g.zone_name
ORDER BY revenue DESC LIMIT 5;

'''Revenue'''
SELECT g.borough, SUM(f.fare_amount) AS total_revenue
FROM Fact_Trips f
JOIN Dim_Geography g ON f.pickup_zone_key = g.zone_key
GROUP BY g.borough ORDER BY total_revenue DESC;
```
