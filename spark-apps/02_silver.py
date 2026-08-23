"""
02_silver.py  —  Silver layer: clean, enrich, classify.

Reads:  hdfs://hdfs-namenode:9000/data/bronze/access_logs/   (Parquet)
        hdfs://hdfs-namenode:9000/raw/client-hostname/client_hostname.csv
Writes: hdfs://hdfs-namenode:9000/data/silver/access_logs/   (Snappy Parquet, partitioned by log_date)

Transformations:
  - Parse ts_raw → TimestampType (format: dd/MMM/yyyy:HH:mm:ss Z)
  - Extract hour integer from timestamp
  - Left-join client_hostname.csv as a broadcast (12.8 MB — well within threshold)
    to add reverse-DNS hostname; rows where hostname == IP had a failed lookup
    and are set to NULL, then coalesced back to IP so the column is never null.
  - Flag bots via user-agent keyword match (BOT_RE — derived from real UA analysis)
  - Flag errors (status >= 400)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_BRONZE  = "hdfs://hdfs-namenode:9000/data/bronze/access_logs"
HDFS_DIM     = "hdfs://hdfs-namenode:9000/raw/client-hostname/client_hostname.csv"
HDFS_OUTPUT  = "hdfs://hdfs-namenode:9000/data/silver/access_logs"

# Bot user-agent keywords derived from a top-30 UA frequency analysis
# on a 200k-line sample (see notebooks/01_data_exploration.ipynb).
BOT_RE = r"(?i)(bot|spider|crawl|ahref|python-requests|bingpreview|slurp|semrush|dataprovider|zgrab)"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("02_silver")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.broadcastTimeout", "300")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(HDFS_BRONZE)
    print(f"[silver] Bronze rows    : {bronze.count():,}")

    # 258,445 rows, 12.8 MB — broadcast to avoid a shuffle join
    dim = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(HDFS_DIM)
        .select(
            F.col("client").alias("ip"),
            F.when(F.col("hostname") == F.col("client"), None)
             .otherwise(F.col("hostname"))
             .alias("hostname"),
        )
    )

    silver = (
        bronze
        .withColumn("ts", F.to_timestamp("ts_raw", "dd/MMM/yyyy:HH:mm:ss Z"))
        .withColumn("hour", F.hour("ts"))
        .join(F.broadcast(dim), on="ip", how="left")
        .withColumn("hostname", F.coalesce(F.col("hostname"), F.col("ip")))
        .withColumn("is_bot", F.col("user_agent").rlike(BOT_RE))
        .withColumn("is_error", F.col("status") >= 400)
        .drop("ts_raw")
        .select(
            "ip", "hostname", "ts", "log_date", "hour",
            "method", "path", "protocol", "status", "bytes",
            "referrer", "user_agent", "xff",
            "is_bot", "is_error",
        )
    )

    silver.write.mode("overwrite").partitionBy("log_date").parquet(HDFS_OUTPUT)

    written = spark.read.parquet(HDFS_OUTPUT)
    total       = written.count()
    bot_count   = written.filter(F.col("is_bot")).count()
    error_count = written.filter(F.col("is_error")).count()
    print(f"[silver] Rows written   : {total:,}")
    print(f"[silver] Bot requests   : {bot_count:,}  ({100*bot_count/total:.1f}%)")
    print(f"[silver] Error requests : {error_count:,}  ({100*error_count/total:.1f}%)")

    spark.stop()
    print("[silver] Done.")


if __name__ == "__main__":
    main()
