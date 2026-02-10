-- Create a dataset in BigQuery
CREATE SCHEMA IF NOT EXISTS nytaxi_data;

-- Create an external table that logically combines all matching files referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE nytaxi_data.yellow_trips_ext24
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bqsql-exercises-2026de/yellow_tripdata_2024-*.parquet']
);

-- Check yellow_trips_ext24 data
SELECT *
FROM nytaxi_data.yellow_trips_ext24
LIMIT 10;

-- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records
CREATE OR REPLACE TABLE nytaxi_data.yellow_trips24
AS
SELECT *
FROM nytaxi_data.yellow_trips_ext24;

-- Total number of records for the 2024 Yellow Taxi Data
SELECT COUNT(*) AS total_records
FROM nytaxi_data.yellow_trips24;

-- Distinct number of PULocationIDs for the entire dataset on the external table
SELECT COUNT(DISTINCT PULocationID) AS column_size_external
FROM nytaxi_data.yellow_trips_ext24;

-- Distinct number of PULocationIDs for the entire dataset on the materialized table
SELECT COUNT(DISTINCT PULocationID) AS column_size_materialized
FROM nytaxi_data.yellow_trips24;

-- PULocationIDs on the materialized table
SELECT PULocationID
FROM nytaxi_data.yellow_trips24;

-- PULocationIDs and DOLocationID together on the materialized table
SELECT PULocationID, DOLocationID
FROM nytaxi_data.yellow_trips24;

-- Total number of trips with zero fare
SELECT COUNT(*) AS zero_fare_records
FROM nytaxi_data.yellow_trips24
WHERE fare_amount=0;

-- Create a partitioned and clustered table
CREATE OR REPLACE TABLE nytaxi_data.yellow_trips24_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM nytaxi_data.yellow_trips24;

-- The distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) queried from the original materialized table
SELECT DISTINCT(VendorID)
FROM nytaxi_data.yellow_trips24
WHERE tpep_dropoff_datetime >= '2024-03-01'
AND tpep_dropoff_datetime <= '2024-03-15';

-- The distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) queried from the partitioned, clustered table
SELECT DISTINCT(VendorID)
FROM nytaxi_data.yellow_trips24_partitioned_clustered
WHERE tpep_dropoff_datetime >= '2024-03-01'
AND tpep_dropoff_datetime <= '2024-03-15';

-- Understanding table scans in BigQuery
SELECT COUNT(*) AS total_records
FROM nytaxi_data.yellow_trips24;
