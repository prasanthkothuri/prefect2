import requests
import os

# get environment variables
env = os.environ['env']

# payload to create the work pool
payload = {
    "name": f"{env}-agent-pool",
    "description": f"Default work pool for {env} environment",
    "type": "prefect-agent",
    "base_job_template": {},
    "is_paused": False,
    "concurrency_limit": 0
}

# create the work pool
r = requests.post('http://localhost:4200/api/work_pools', json=payload)
if r.status_code == 201:
    print(f"Work pool created, details: {r.text}")
else:
    print(f"Unable to create work pool, error: {r.text}")
