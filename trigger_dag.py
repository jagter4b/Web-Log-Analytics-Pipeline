#!/usr/bin/env python3
"""
trigger_dag.py — Trigger the log_processing_pipeline DAG via Airflow 3 REST API.

Authenticates with SimpleAuthManager (JWT), then POSTs a dagRun.
Run this inside the airflow-apiserver container:
  docker cp trigger_dag.py airflow-apiserver:/tmp/trigger_dag.py
  docker exec airflow-apiserver python3 /tmp/trigger_dag.py

Update PASSWORD from: docker logs airflow-apiserver 2>&1 | grep "Password for user"
(SimpleAuthManager regenerates credentials on every airflow-init run.)
"""
import urllib.request
import urllib.parse
import json
import base64
import sys
from datetime import datetime, timezone

USERNAME = "admin"
PASSWORD = "TuebqvvgEd8XFPHA"
BASE_URL = "http://localhost:8080"

# Obtain a JWT token
login_req = urllib.request.Request(
    f"{BASE_URL}/auth/token",
    data=json.dumps({"username": USERNAME, "password": PASSWORD}).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST"
)

try:
    with urllib.request.urlopen(login_req) as resp:
        token_data = json.loads(resp.read().decode())
        token = token_data.get("access_token") or token_data.get("token")
        print(f"Authenticated. Token: {token[:20]}...")
except urllib.error.HTTPError as e:
    print(f"Login failed HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
    token = None

# Trigger the DAG run
trigger_url = f"{BASE_URL}/api/v2/dags/log_processing_pipeline/dagRuns"
payload = json.dumps({
    "logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "conf": {"sample_mode": True},
}).encode("utf-8")

headers = {"Content-Type": "application/json"}
if token:
    headers["Authorization"] = f"Bearer {token}"
else:
    creds = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    headers["Authorization"] = f"Basic {creds}"

trigger_req = urllib.request.Request(
    trigger_url, data=payload, headers=headers, method="POST"
)

try:
    with urllib.request.urlopen(trigger_req) as resp:
        data = json.loads(resp.read().decode())
        print("DAG triggered successfully!")
        print(f"  run_id    : {data.get('run_id')}")
        print(f"  state     : {data.get('state')}")
        print(f"  run_after : {data.get('run_after')}")
        print(f"  dag_id    : {data.get('dag_id')}")
except urllib.error.HTTPError as e:
    print(f"Trigger failed HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
    sys.exit(1)
