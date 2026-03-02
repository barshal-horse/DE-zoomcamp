# Module 5 Homework: Data Platforms with Bruin

## Question 1. Bruin Pipeline Structure

**Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

A Bruin project requires:
- `.bruin.yml` at the project root for environment and connection configuration
- A `pipeline/` directory containing `pipeline.yml` (pipeline definition) and `assets/` (asset definitions)

---

## Question 2. Materialization Strategies

**Answer:** `time_interval` - incremental based on a time column

The `time_interval` strategy processes data for a specific time period by deleting existing data in the given interval and inserting new data. This is ideal for processing NYC taxi data organized by month based on `pickup_datetime`.

---

## Question 3. Pipeline Variables

**Answer:** `bruin run --var 'taxi_types=["yellow"]'`

Since `taxi_types` is defined as an array type variable, the override must also be a valid JSON array. Using `--var 'taxi_types=["yellow"]'` correctly passes the single-element array.

---

## Question 4. Running with Dependencies

**Answer:** `bruin run ingestion/trips.py --downstream`

The `--downstream` flag tells Bruin to run the specified asset along with all assets that depend on it (downstream dependencies).

---

## Question 5. Quality Checks

**Answer:** `name: not_null`

The `not_null` quality check ensures that a column never contains NULL values. This is the appropriate check for ensuring `pickup_datetime` always has a value.

---

## Question 6. Lineage and Dependencies

**Answer:** `bruin lineage`

The `bruin lineage` command displays the dependency graph of assets, showing upstream and downstream relationships in the pipeline.

---

## Question 7. First-Time Run

**Answer:** `--full-refresh`

The `--full-refresh` flag drops and recreates tables from scratch, ensuring a clean state. This is essential for first-time runs on a new DuckDB database.
