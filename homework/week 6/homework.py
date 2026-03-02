"""
Module 6 Homework: Batch Processing with Spark
DE Zoomcamp 2026
"""
import os
import sys

# Set JAVA_HOME and HADOOP_HOME
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, count, min as spark_min
import glob

# ============================================================
# Question 1: Spark Version
# ============================================================
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("HW6") \
    .getOrCreate()

print("=" * 60)
print(f"Q1 - Spark Version: {spark.version}")
print("=" * 60)

# ============================================================
# Question 2: Read Yellow Nov 2025, repartition to 4, save
# ============================================================
data_dir = os.path.dirname(os.path.abspath(__file__))
parquet_path = os.path.join(data_dir, "yellow_tripdata_2025-11.parquet")
output_path = os.path.join(data_dir, "yellow_nov_2025_repartitioned")

df = spark.read.parquet(parquet_path)

# Repartition to 4 and save
df.repartition(4).write.mode("overwrite").parquet(output_path)

# Check file sizes
parquet_files = glob.glob(os.path.join(output_path, "*.parquet"))
sizes_mb = [os.path.getsize(f) / (1024 * 1024) for f in parquet_files]
avg_size = sum(sizes_mb) / len(sizes_mb) if sizes_mb else 0

print(f"\nQ2 - Repartitioned parquet files:")
for f, s in zip(parquet_files, sizes_mb):
    print(f"  {os.path.basename(f)}: {s:.1f} MB")
print(f"  Average size: {avg_size:.1f} MB")
print("=" * 60)

# ============================================================
# Question 3: Count trips on Nov 15
# ============================================================
df_with_date = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
count_nov_15 = df_with_date.filter(col("pickup_date") == "2025-11-15").count()

print(f"\nQ3 - Trips on Nov 15, 2025: {count_nov_15}")
print("=" * 60)

# ============================================================
# Question 4: Longest trip in hours
# ============================================================
df_with_duration = df.withColumn(
    "trip_duration_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600.0
)
longest_trip = df_with_duration.agg({"trip_duration_hours": "max"}).collect()[0][0]

print(f"\nQ4 - Longest trip duration: {longest_trip:.1f} hours")
print("=" * 60)

# ============================================================
# Question 5: Spark UI Port (knowledge question)
# ============================================================
print(f"\nQ5 - Spark UI Port: 4040")
print("=" * 60)

# ============================================================
# Question 6: Least frequent pickup location zone
# ============================================================
zones_path = os.path.join(data_dir, "taxi_zone_lookup.csv")
zones_df = spark.read.option("header", "true").csv(zones_path)
zones_df.createOrReplaceTempView("zones")

df.createOrReplaceTempView("trips")

result = spark.sql("""
    SELECT z.Zone, COUNT(*) as trip_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
    LIMIT 5
""")

print(f"\nQ6 - Least frequent pickup location zones:")
result.show(truncate=False)
print("=" * 60)

spark.stop()
