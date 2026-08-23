#!/usr/bin/env python3
"""Fail all currently-running DAG runs to clear stale state."""
import urllib.request, urllib.parse, json, sys

USERNAME = "admin"; PASSWORD = "TuebqvvgEd8XFPHA"; BASE_URL = "http://localhost:8080"

# Get token
req = urllib.request.Request(BASE_URL+'/auth/token',
    data=json.dumps({"username":USERNAME,"password":PASSWORD}).encode(),
    headers={"Content-Type":"application/json"}, method="POST")
with urllib.request.urlopen(req) as r:
    token = json.loads(r.read())["access_token"]
auth = {"Authorization": "Bearer "+token, "Content-Type": "application/json"}

# Get all running/queued runs
req2 = urllib.request.Request(
    BASE_URL+"/api/v2/dags/log_processing_pipeline/dagRuns?limit=20&order_by=-run_after",
    headers={"Authorization": "Bearer "+token})
with urllib.request.urlopen(req2) as r:
    runs = json.loads(r.read()).get("dag_runs", [])

stale = [r for r in runs if r.get("state") in ("running", "queued")]
print(f"Found {len(stale)} stale run(s) to fail:")

for run in stale:
    dag_run_id = run.get("dag_run_id")
    enc = urllib.parse.quote(str(dag_run_id), safe="")
    patch_url = BASE_URL+f"/api/v2/dags/log_processing_pipeline/dagRuns/{enc}"
    patch_data = json.dumps({"state": "failed"}).encode()
    patch_req = urllib.request.Request(patch_url, data=patch_data, headers=auth, method="PATCH")
    try:
        with urllib.request.urlopen(patch_req) as r:
            result = json.loads(r.read())
            print(f"  FAILED: {dag_run_id}  new_state={result.get('state')}")
    except urllib.error.HTTPError as e:
        print(f"  ERROR failing {dag_run_id}: {e.code} {e.read().decode()}")

print("Done. All stale runs cleared.")
