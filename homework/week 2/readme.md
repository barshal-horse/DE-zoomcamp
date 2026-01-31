
# Module 2 Homework – Workflow Orchestration (Kestra)

This repository contains my solution for **Module 2 Homework** of the Data Engineering Zoomcamp 2026.

## Tools
- Kestra
- Docker & Docker Compose
- PostgreSQL

## Dataset
NYC TLC Yellow and Green Taxi trip data  
Source: https://github.com/DataTalksClub/nyc-tlc-data

## Work Done
- Used the scheduled Kestra flow `05_postgres_taxi_scheduled`
- Ran backfill to process **2021 data (January–July)**
- Executed backfill for both **yellow** and **green** taxi data
- Data was loaded into PostgreSQL via Docker Compose

## Files
- `flows/05_postgres_taxi_scheduled.yaml`
- `docker-compose.yml`
