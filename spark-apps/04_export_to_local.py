"""
04_export_to_local.py  —  Export Gold Parquet tables to the local filesystem.

Reads:  hdfs://hdfs-namenode:9000/data/gold/*
Writes: /opt/spark-apps/output/<table>.parquet  (bind-mounted → host ./output/)

coalesce(1) is appropriate here because all Gold tables are small (5–228 rows).
It produces a single Parquet file per table, which simplifies downstream
consumption (e.g. Power BI Desktop reading from ./output/).
"""

from pyspark.sql import SparkSession

HDFS_GOLD    = "hdfs://hdfs-namenode:9000/data/gold"
LOCAL_OUTPUT = "/opt/spark-apps/output"

GOLD_TABLES = [
    "traffic_by_hour",
    "top_pages",
    "error_rates",
    "bot_vs_human",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("04_export_to_local")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    for table in GOLD_TABLES:
        hdfs_path  = f"{HDFS_GOLD}/{table}"
        local_path = f"file://{LOCAL_OUTPUT}/{table}.parquet"

        df = spark.read.parquet(hdfs_path)
        rows = df.count()
        df.coalesce(1).write.mode("overwrite").parquet(local_path)
        print(f"[export] {table:20s}: {rows:,} rows → {local_path}")

    spark.stop()
    print("[export] All Gold tables exported to local output/. Done.")


if __name__ == "__main__":
    main()
