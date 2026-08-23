#!/usr/bin/env python3
"""Check the latest DAG run and task instance status."""
import urllib.request, urllib.parse, json

USERNAME = "admin"; PASSWORD = "TuebqvvgEd8XFPHA"; BASE_URL = "http://localhost:8080"

req = urllib.request.Request(BASE_URL+'/auth/token',
    data=json.dumps({"username":USERNAME,"password":PASSWORD}).encode(),
    headers={"Content-Type":"application/json"}, method="POST")
with urllib.request.urlopen(req) as r:
    token = json.loads(r.read())["access_token"]
auth = {"Authorization": "Bearer "+token}

req2 = urllib.request.Request(
    BASE_URL+"/api/v2/dags/log_processing_pipeline/dagRuns?limit=2&order_by=-run_after",
    headers=auth)
with urllib.request.urlopen(req2) as r:
    runs = json.loads(r.read()).get("dag_runs",[])

print(f"Latest {len(runs)} run(s):")
for run in runs:
    print(f"  {run.get('dag_run_id')}  state={run.get('state')}  start={run.get('start_date')}  end={run.get('end_date')}")

latest = runs[0] if runs else None
if latest:
    dag_run_id = latest.get("dag_run_id")
    enc = urllib.parse.quote(str(dag_run_id), safe="")
    req3 = urllib.request.Request(
        BASE_URL+f"/api/v2/dags/log_processing_pipeline/dagRuns/{enc}/taskInstances",
        headers=auth)
    try:
        with urllib.request.urlopen(req3) as r:
            tis = json.loads(r.read()).get("task_instances",[])
        print(f"\nTask instances ({len(tis)}) for: {dag_run_id}")
        for ti in tis:
            t = str(ti.get("task_id","?"))
            s = str(ti.get("state") or "pending")
            start = str(ti.get("start_date") or "-")
            end   = str(ti.get("end_date") or "-")
            print(f"  {t:<35s}  {s:<12s}  start={start}  end={end}")
    except urllib.error.HTTPError as e:
        print(f"Error fetching tasks: {e.code} {e.read().decode()}")
