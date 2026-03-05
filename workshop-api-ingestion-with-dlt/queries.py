import duckdb

DUCKDB_PATH = "taxi_pipeline.duckdb"

con = duckdb.connect(DUCKDB_PATH)

# print("Tables:", con.execute("show all tables").fetchall())
# print("Columns:", con.execute("describe taxi_data.yellow_taxi_trips").fetchall())

# Q1) date range
q1 = """
select
  min(trip_pickup_date_time) as min_pickup,
  max(trip_pickup_date_time) as max_pickup
from taxi_data.yellow_taxi_trips
"""
print("Q1:", con.execute(q1).fetchall())

# Inspect payment_type values (do this once)
print(
    "payment_type counts:",
    con.execute(
        """
        select payment_type, count(*) as cnt
        from taxi_data.yellow_taxi_trips
        group by 1
        order by cnt desc
        """
    ).fetchall(),
)

# Q2) proportion credit card (pick ONE of the below after you see values)
q2 = """
SELECT
  ROUND(
    100.0 * SUM(CASE WHEN LOWER(payment_type) = 'credit' THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS credit_card_percentage
FROM taxi_data.yellow_taxi_trips;
"""
print("Q2:", con.execute(q2).fetchall())


# Q3) total tips
q3 = """
select sum(tip_amt) as total_tips
from taxi_data.yellow_taxi_trips
"""
print("Q3:", con.execute(q3).fetchall())

