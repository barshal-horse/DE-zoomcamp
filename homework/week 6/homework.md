# Module 6 Homework: Batch Processing with Spark

## Question 1. Install Spark and PySpark

**Answer:** `4.1.1`

```python
spark.version
# Output: '4.1.1'
```

---

## Question 2. Yellow November 2025

Read the November 2025 Yellow data, repartitioned to 4 partitions, and saved as parquet.

**Answer:** **25MB**

Each partition file was ~24.4 MB, so the closest answer is **25MB**.

---

## Question 3. Count records

How many taxi trips started on November 15, 2025?

**Answer:** **162,604**

```python
df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()
# Output: 162604
```

---

## Question 4. Longest trip

What is the length of the longest trip in hours?

**Answer:** **90.6**

```python
df.withColumn(
    "trip_duration_hours",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
).agg({"trip_duration_hours": "max"}).collect()
# Output: 90.6 hours
```

---

## Question 5. User Interface

Spark's UI dashboard runs on which port?

**Answer:** **4040**

---

## Question 6. Least frequent pickup location zone

**Answer:** **Governor's Island/Ellis Island/Liberty Island**

```
+---------------------------------------------+----------+
|Zone                                         |trip_count|
+---------------------------------------------+----------+
|Governor's Island/Ellis Island/Liberty Island|1         |
|Eltingville/Annadale/Prince's Bay            |1         |
|Arden Heights                                |1         |
|Port Richmond                                |3         |
|Rikers Island                                |4         |
+---------------------------------------------+----------+
```

Governor's Island/Ellis Island/Liberty Island had only 1 pickup trip, making it the least frequent.
