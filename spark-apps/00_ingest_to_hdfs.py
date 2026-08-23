"""
00_ingest_to_hdfs.py  —  Phase 0: upload raw files into HDFS raw zone.

Uses subprocess + hdfs CLI instead of Spark because a file copy
needs no distributed computation. Kept as a .py so Airflow can
invoke it uniformly alongside the other stages.
"""

import subprocess
import sys

HDFS_RAW_LOGS = "hdfs://hdfs-namenode:9000/raw/access-logs"
HDFS_RAW_DIM  = "hdfs://hdfs-namenode:9000/raw/client-hostname"
LOCAL_LOG     = "/opt/spark-apps/access.log"
LOCAL_DIM     = "/opt/spark-apps/client_hostname.csv"


def run(cmd: list[str]) -> None:
    """Run a shell command, raise on non-zero exit."""
    print(f"RUN: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        print(result.stderr.strip(), file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def main() -> None:
    # Create HDFS directories (idempotent) and ensure world-writable so
    # any container user (root or airflow) can write without ACL conflicts.
    run(["hdfs", "dfs", "-mkdir", "-p", HDFS_RAW_LOGS, HDFS_RAW_DIM])
    run(["hdfs", "dfs", "-mkdir", "-p",
         "hdfs://hdfs-namenode:9000/data/bronze",
         "hdfs://hdfs-namenode:9000/data/silver",
         "hdfs://hdfs-namenode:9000/data/gold"])
    run(["hdfs", "dfs", "-chmod", "-R", "777",
         "hdfs://hdfs-namenode:9000/data"])

    print("Uploading access.log → HDFS (several minutes for 3.3 GB)…")
    run(["hdfs", "dfs", "-put", "-f", LOCAL_LOG, f"{HDFS_RAW_LOGS}/access.log"])

    print("Uploading client_hostname.csv → HDFS…")
    run(["hdfs", "dfs", "-put", "-f", LOCAL_DIM, f"{HDFS_RAW_DIM}/client_hostname.csv"])

    run(["hdfs", "dfs", "-ls", "-h", HDFS_RAW_LOGS])
    run(["hdfs", "dfs", "-ls", "-h", HDFS_RAW_DIM])
    print("Ingestion complete.")


if __name__ == "__main__":
    main()
