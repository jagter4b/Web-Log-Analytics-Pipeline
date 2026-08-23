#!/usr/bin/env python3
"""Force export_to_local to run immediately by setting its state to scheduled."""
import urllib.request, urllib.parse, json

USERNAME = "admin"; PASSWORD = "TuebqvvgEd8XFPHA"; BASE_URL = "http://localhost:8080"
DAG_RUN_ID = "manual__2026-08-22T17:19:43.036521+00:00"
TASK_ID = "export_to_local"

req = urllib.request.Request(BASE_URL+'/auth/token',
    data=json.dumps({"username":USERNAME,"password":PASSWORD}).encode(),
    headers={"Content-Type":"application/json"}, method="POST")
with urllib.request.urlopen(req) as r:
    token = json.loads(r.read())["access_token"]
auth_h = {"Authorization":"Bearer "+token, "Content-Type":"application/json"}

enc_run = urllib.parse.quote(DAG_RUN_ID, safe="")
enc_task = urllib.parse.quote(TASK_ID, safe="")

# PATCH the task instance state to scheduled so scheduler picks it up immediately
url = f"{BASE_URL}/api/v2/dags/log_processing_pipeline/dagRuns/{enc_run}/taskInstances/{enc_task}"
patch_data = json.dumps({"state": "scheduled", "dry_run": False}).encode()
req2 = urllib.request.Request(url, data=patch_data, headers=auth_h, method="PATCH")
try:
    with urllib.request.urlopen(req2) as r:
        result = json.loads(r.read())
        print(f"Task {TASK_ID} patched to: {result.get('state')}")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"HTTP {e.code}: {body}")
    # If PATCH on task instance fails, try clearing the task
    print("Trying clear instead...")
    clear_url = f"{BASE_URL}/api/v2/dags/log_processing_pipeline/dagRuns/{enc_run}/taskInstances/clear"
    clear_data = json.dumps({
        "dry_run": False,
        "task_ids": [TASK_ID],
        "reset_dag_runs": False
    }).encode()
    req3 = urllib.request.Request(clear_url, data=clear_data, headers=auth_h, method="POST")
    try:
        with urllib.request.urlopen(req3) as r:
            print(f"Clear result: {r.read().decode()[:200]}")
    except urllib.error.HTTPError as e2:
        print(f"Clear also failed: {e2.code} {e2.read().decode()[:200]}")
