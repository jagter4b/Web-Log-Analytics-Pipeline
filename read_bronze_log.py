#!/usr/bin/env python3
"""Parse Airflow JSONL log files and print all event messages for bronze_job."""
import json, os, sys

base = "/opt/airflow/logs/dag_id=log_processing_pipeline"
log_files = []
for run_dir in sorted(os.listdir(base)):
    task_dir = os.path.join(base, run_dir, "task_id=bronze_job")
    if os.path.isdir(task_dir):
        for f in sorted(os.listdir(task_dir)):
            if f.endswith(".log"):
                log_files.append(os.path.join(task_dir, f))

if not log_files:
    print("No bronze_job log files found")
    sys.exit(1)

for path in log_files:
    print(f"\n{'='*70}")
    print(f"FILE: {path}")
    print('='*70)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                level = obj.get("level", "info")
                event = str(obj.get("event", ""))
                # Show: errors, commands, submit-related, spark-related
                keywords = ["error","fail","exception","command","submit","spark","Running","Output","return code","exit"]
                if level in ("error","warning") or any(k.lower() in event.lower() for k in keywords):
                    print(f"[{level.upper():7s}] {event[:300]}")
                    # If there's error_detail, print the exc_value
                    if "error_detail" in obj:
                        for ed in obj["error_detail"]:
                            print(f"           EXC: {ed.get('exc_type')}: {ed.get('exc_value','')[:200]}")
                            for frame in ed.get("frames", [])[-5:]:
                                print(f"           at {frame.get('filename')}:{frame.get('lineno')} in {frame.get('name')}")
            except json.JSONDecodeError:
                if any(k in line.lower() for k in ["error","fail","exception","spark","command"]):
                    print(f"[RAW    ] {line[:300]}")
