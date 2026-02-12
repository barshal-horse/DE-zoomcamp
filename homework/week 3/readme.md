# DE Zoomcamp 2026 – Homework 3 (Data Warehousing)

This repo contains my work for Homework 3 using the 2024 NYC Yellow Taxi dataset in BigQuery.

I created an external table from the parquet files in GCS, then loaded it into a BigQuery table for querying.  
After that, I built an optimized version partitioned by dropoff date and clustered by VendorID since most queries filter by time and sort/group by vendor.

Queries included:
- total record count for 2024 data
- rows where fare_amount = 0
- distinct VendorIDs for a March 2024 date range
- comparison of performance between regular and partitioned tables

Tools used:
- BigQuery
- Google Cloud Storage
- SQL
