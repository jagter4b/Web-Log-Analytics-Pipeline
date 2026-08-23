"""
verify_data.py  —  Read-only integrity check for all pipeline layers.

Reads Bronze, Silver, and all 4 Gold tables from HDFS and prints
row counts and key metrics. Does not write anything.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
    .appName("verify")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

print("=== DATA INTEGRITY CHECK ===")

bronze = spark.read.parquet("hdfs://hdfs-namenode:9000/data/bronze/access_logs")
b_count = bronze.count()
print(f"[bronze] rows={b_count:,}  (expected 10,365,077)")

silver = spark.read.parquet("hdfs://hdfs-namenode:9000/data/silver/access_logs")
s_count = silver.count()
bot_count   = silver.filter(F.col("is_bot")).count()
error_count = silver.filter(F.col("is_error")).count()
print(f"[silver] rows={s_count:,}  (expected 10,365,077)")
print(f"[silver] bots={bot_count:,} ({100*bot_count/s_count:.1f}%)  (expected ~10.9%)")
print(f"[silver] errors={error_count:,} ({100*error_count/s_count:.1f}%)  (expected ~1.7%)")

for table in ["traffic_by_hour", "top_pages", "error_rates", "bot_vs_human"]:
    df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/data/gold/{table}")
    print(f"[gold/{table}] rows={df.count():,}")

spark.stop()
print("=== CHECK COMPLETE ===")
