#!/usr/bin/env python3
"""Parse Airflow JSONL log for the latest export_to_local task attempt."""
import json, os, sys

base = "/opt/airflow/logs/dag_id=log_processing_pipeline"
log_files = []
for run_dir in sorted(os.listdir(base)):
    task_dir = os.path.join(base, run_dir, "task_id=export_to_local")
    if os.path.isdir(task_dir):
        for f in sorted(os.listdir(task_dir)):
            if f.endswith(".log"):
                log_files.append(os.path.join(task_dir, f))

if not log_files:
    print("No export_to_local log files found")
    sys.exit(1)

# Read only the latest one
path = log_files[-1]
print(f"Reading: {path}\n")
with open(path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            level = obj.get("level", "info")
            event = str(obj.get("event", ""))
            keywords = ["error","fail","exception","command","submit","spark","Running","Output","return code","exit","Permission","Access","denied"]
            if level in ("error","warning") or any(k.lower() in event.lower() for k in keywords):
                print(f"[{level.upper():7s}] {event[:400]}")
                if "error_detail" in obj:
                    for ed in obj["error_detail"]:
                        print(f"           EXC: {ed.get('exc_type')}: {ed.get('exc_value','')[:300]}")
                        for frame in ed.get("frames", [])[-5:]:
                            print(f"           at {frame.get('filename')}:{frame.get('lineno')} in {frame.get('name')}")
        except json.JSONDecodeError:
            if any(k in line.lower() for k in ["error","fail","exception","permission","access"]):
                print(f"[RAW    ] {line[:400]}")
