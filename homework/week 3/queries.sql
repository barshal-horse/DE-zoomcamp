-- Create External Table for 2024 Yellow Taxi Data
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-taxi-data/yellow_tripdata_2024-*.parquet']
);

-- Create Materialized Table from External Table
CREATE OR REPLACE TABLE `dezoomcamp.yellow_taxi_materialized` AS
SELECT * 
FROM `dezoomcamp.yellow_taxi_external`;

-- Create Optimized Table (Partition + Cluster)
CREATE OR REPLACE TABLE `dezoomcamp.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `dezoomcamp.yellow_taxi_materialized`;

-- Q1: Count total records in 2024 dataset
SELECT COUNT(*) 
FROM `dezoomcamp.yellow_taxi_materialized`;

-- Q4: Count records where fare_amount = 0
SELECT COUNT(*)
FROM `dezoomcamp.yellow_taxi_materialized`
WHERE fare_amount = 0;

-- Q6: Distinct VendorIDs between March 1–15, 2024 (materialized table)
SELECT DISTINCT VendorID
FROM `dezoomcamp.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Q6: Same query on partitioned table
SELECT DISTINCT VendorID
FROM `dezoomcamp.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Q9: Count(*) bytes estimate test
SELECT COUNT(*)
FROM `dezoomcamp.yellow_taxi_materialized`;
