"""
01_bronze.py  —  Bronze layer: parse raw Nginx access log → Parquet.

Reads:  hdfs://hdfs-namenode:9000/raw/access-logs/access.log  (3.3 GB text)
Writes: hdfs://hdfs-namenode:9000/data/bronze/access_logs/    (Snappy Parquet, partitioned by log_date)

Transformations (Bronze = minimal, raw-faithful):
  - Apply Nginx combined-log regex
  - Cast status → IntegerType, bytes → LongType  (bytes='-' → NULL)
  - Extract log_date (DateType) as partition key
  - Drop lines where the regex did not match (binary payloads — see REGEX comment)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

HDFS_INPUT  = "hdfs://hdfs-namenode:9000/raw/access-logs/access.log"
HDFS_OUTPUT = "hdfs://hdfs-namenode:9000/data/bronze/access_logs"

# Nginx combined log format + optional X-Forwarded-For 10th field.
# Validated on a 200k-line sample: 199,998/200,000 matched (99.999%).
# The 2 failures are raw TCP/BitTorrent binary payloads that arrived on
# the HTTP port and cannot be parsed as HTTP — they are correctly dropped.
REGEX = (
    r"^(\S+) \S+ \S+ \[([^\]]+)\] "
    r'"(\S+) (\S+) ([^"]+)" (\d{3}) (\S+) '
    r'"([^"]*)" "([^"]*)"(?: "([^"]*)")?'
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("01_bronze")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.text(HDFS_INPUT)
    raw_count = raw.count()
    print(f"[bronze] Raw lines read : {raw_count:,}")

    bronze = (
        raw
        .select(
            F.regexp_extract("value", REGEX, 1).alias("ip"),
            F.regexp_extract("value", REGEX, 2).alias("ts_raw"),
            F.regexp_extract("value", REGEX, 3).alias("method"),
            F.regexp_extract("value", REGEX, 4).alias("path"),
            F.regexp_extract("value", REGEX, 5).alias("protocol"),
            F.regexp_extract("value", REGEX, 6).cast(IntegerType()).alias("status"),
            F.when(
                F.regexp_extract("value", REGEX, 7) == "-", None
            ).otherwise(
                F.regexp_extract("value", REGEX, 7).cast(LongType())
            ).alias("bytes"),
            F.regexp_extract("value", REGEX, 8).alias("referrer"),
            F.regexp_extract("value", REGEX, 9).alias("user_agent"),
            F.regexp_extract("value", REGEX, 10).alias("xff"),
        )
        .filter(F.col("ip") != "")
        .withColumn(
            "log_date",
            F.to_date(
                F.regexp_extract("ts_raw", r"^(\d{2}/\w+/\d{4})", 1),
                "dd/MMM/yyyy",
            ),
        )
    )

    bronze.write.mode("overwrite").partitionBy("log_date").parquet(HDFS_OUTPUT)

    written = spark.read.parquet(HDFS_OUTPUT)
    written_count = written.count()
    print(f"[bronze] Rows written   : {written_count:,}")
    print(f"[bronze] Dropped        : {raw_count - written_count:,} (binary/malformed lines)")
    print("[bronze] Partitions:")
    written.select("log_date").distinct().orderBy("log_date").show()

    spark.stop()
    print("[bronze] Done.")


if __name__ == "__main__":
    main()
