"""
03_gold.py  —  Gold layer: business-level aggregation tables.

Reads:  hdfs://hdfs-namenode:9000/data/silver/access_logs/   (Parquet)
Writes four Gold tables (flat Parquet, no partitioning — all are small outputs):
  hdfs://hdfs-namenode:9000/data/gold/traffic_by_hour/   (228 rows)
  hdfs://hdfs-namenode:9000/data/gold/top_pages/         (50 rows)
  hdfs://hdfs-namenode:9000/data/gold/error_rates/       (114 rows)
  hdfs://hdfs-namenode:9000/data/gold/bot_vs_human/      (5 rows)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_SILVER = "hdfs://hdfs-namenode:9000/data/silver/access_logs"
HDFS_GOLD   = "hdfs://hdfs-namenode:9000/data/gold"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("03_gold")
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

    # Cache Silver — reused across 4 aggregations to avoid re-reading HDFS each time.
    silver = spark.read.parquet(HDFS_SILVER)
    silver.cache()
    print(f"[gold] Silver rows cached: {silver.count():,}")

    # ── traffic_by_hour ───────────────────────────────────────────────────────
    # 5 days × 24 hours × 2 (bot/human) = 240 theoretical max; 228 actual.
    traffic_by_hour = (
        silver
        .groupBy("log_date", "hour", "is_bot")
        .agg(
            F.count("*").alias("requests"),
            F.sum("bytes").alias("total_bytes"),
            F.round(F.avg("bytes"), 2).alias("avg_bytes"),
            F.countDistinct("ip").alias("unique_ips"),
        )
        .orderBy("log_date", "hour", "is_bot")
    )
    traffic_by_hour.write.mode("overwrite").parquet(f"{HDFS_GOLD}/traffic_by_hour")
    print(f"[gold] traffic_by_hour : {spark.read.parquet(f'{HDFS_GOLD}/traffic_by_hour').count():,} rows")

    # ── top_pages ─────────────────────────────────────────────────────────────
    # Human traffic only. Query strings stripped so /page?ref=x and /page are
    # counted together rather than as separate endpoints.
    top_pages = (
        silver
        .filter(~F.col("is_bot"))
        .withColumn("clean_path", F.regexp_replace("path", r"\?.*$", ""))
        .groupBy("clean_path")
        .agg(
            F.count("*").alias("requests"),
            F.countDistinct("ip").alias("unique_ips"),
            F.round(F.avg("bytes"), 2).alias("avg_bytes"),
        )
        .orderBy(F.desc("requests"))
        .limit(50)
    )
    top_pages.write.mode("overwrite").parquet(f"{HDFS_GOLD}/top_pages")
    print(f"[gold] top_pages        : {spark.read.parquet(f'{HDFS_GOLD}/top_pages').count():,} rows")

    # ── error_rates ───────────────────────────────────────────────────────────
    # 5 days × 24 hours = 120 max; 114 actual (6 hours have zero traffic).
    error_rates = (
        silver
        .groupBy("log_date", "hour")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum(F.col("is_error").cast("long")).alias("error_count"),
            F.round(
                100.0 * F.sum(F.col("is_error").cast("long")) / F.count("*"), 2
            ).alias("error_rate_pct"),
        )
        .orderBy("log_date", "hour")
    )
    error_rates.write.mode("overwrite").parquet(f"{HDFS_GOLD}/error_rates")
    print(f"[gold] error_rates      : {spark.read.parquet(f'{HDFS_GOLD}/error_rates').count():,} rows")

    # ── bot_vs_human ──────────────────────────────────────────────────────────
    bot_vs_human = (
        silver
        .groupBy("log_date")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum(F.col("is_bot").cast("long")).alias("bot_requests"),
            F.sum((~F.col("is_bot")).cast("long")).alias("human_requests"),
            F.round(
                100.0 * F.sum(F.col("is_bot").cast("long")) / F.count("*"), 2
            ).alias("bot_pct"),
            F.countDistinct(
                F.when(~F.col("is_bot"), F.col("ip"))
            ).alias("unique_human_ips"),
        )
        .orderBy("log_date")
    )
    bot_vs_human.write.mode("overwrite").parquet(f"{HDFS_GOLD}/bot_vs_human")
    print(f"[gold] bot_vs_human     : {spark.read.parquet(f'{HDFS_GOLD}/bot_vs_human').count():,} rows")

    silver.unpersist()
    spark.stop()
    print("[gold] Done.")


if __name__ == "__main__":
    main()
