# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Online Feature Store
# MAGIC
# MAGIC ```
# MAGIC Step 1: Register auto_features_active as a Feature Table
# MAGIC Step 2: Create Online Table  (backing store for millisecond lookups)
# MAGIC Step 3: Wait for Online Table → ONLINE_CONTINUOUS_UPDATE
# MAGIC Step 4: Create Feature Spec   (defines lookup: key=auto_policy_id, 11 features)
# MAGIC Step 5: Create Feature Serving Endpoint  (entity_name = Feature Spec)
# MAGIC Step 6: Query — pass auto_policy_id → get all 11 features in ~10ms
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time, requests
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup

dbutils.widgets.text("catalog",       "ws_prd_analytics")
dbutils.widgets.text("schema",        "default")
dbutils.widgets.text("endpoint_name", "auto-insurance-feature-serving")

CATALOG       = dbutils.widgets.get("catalog")
SCHEMA        = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

ACTIVE_TABLE      = f"{CATALOG}.{SCHEMA}.auto_features_active"
ONLINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.auto_features_active_online"
FEATURE_SPEC_NAME = f"{CATALOG}.{SCHEMA}.auto_policy_feature_spec"

HOST    = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

fe = FeatureEngineeringClient()

FEATURE_COLUMNS = [
    "no_of_vehicles", "newest_vehicle_age", "oldest_vehicle_age", "avg_vehicle_age",
    "no_of_drivers",  "avg_driver_age",     "youngest_driver_age","oldest_driver_age",
    "login_count_24h","login_count_7d",     "last_login_time",
]

print(f"Active table  : {ACTIVE_TABLE}")
print(f"Online table  : {ONLINE_TABLE_NAME}")
print(f"Feature spec  : {FEATURE_SPEC_NAME}")
print(f"Endpoint      : {ENDPOINT_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Register Active Table as a Feature Table

# COMMAND ----------

try:
    fe.create_table(
        name=ACTIVE_TABLE,
        primary_keys=["auto_policy_id"],
        description="Active feature store — one row per auto policy, 11 features",
    )
    print(f"Feature table registered: {ACTIVE_TABLE}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Feature table already registered: {ACTIVE_TABLE}")
    else:
        print(f"Note: {e}")
        print("Continuing — table may already be registered.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Create Online Table
# MAGIC
# MAGIC Required backing store for millisecond lookups.
# MAGIC Syncs continuously from `auto_features_active` via CDF.

# COMMAND ----------

# Check if online table already exists
ot_resp = requests.get(
    f"https://{HOST}/api/2.0/online-tables/{ONLINE_TABLE_NAME}",
    headers=HEADERS
)

if ot_resp.status_code == 200:
    ot_state = ot_resp.json().get("status", {}).get("detailed_state", "unknown")
    print(f"Online table already exists — state: {ot_state}")
else:
    print(f"Creating online table: {ONLINE_TABLE_NAME} ...")
    body = {
        "name": ONLINE_TABLE_NAME,
        "spec": {
            "source_table_full_name": ACTIVE_TABLE,
            "primary_key_columns": ["auto_policy_id"],
            "run_continuously": {}
        }
    }
    cr = requests.post(
        f"https://{HOST}/api/2.0/online-tables",
        headers=HEADERS, json=body
    )
    if cr.status_code in (200, 201):
        print(f"Online table created: {ONLINE_TABLE_NAME}")
    elif cr.status_code == 409 or "already exists" in cr.text.lower():
        print(f"Online table already exists: {ONLINE_TABLE_NAME}")
    else:
        print(f"REST API {cr.status_code}: {cr.text[:500]}")
        print()
        print("=== ACTION REQUIRED: Create Online Table via UI ===")
        print("1. Go to Catalog Explorer (left sidebar)")
        print(f"2. Navigate to: {CATALOG} → {SCHEMA} → auto_features_active")
        print("3. Click the ⋮ menu → 'Create online table'")
        print("4. Name: auto_features_active_online")
        print("5. Primary key: auto_policy_id")
        print("6. Sync mode: Continuous")
        print("7. Click Create, then re-run Step 3 to wait for it to be ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Wait for Online Table to be READY
# MAGIC
# MAGIC Run this cell after Step 2. Polls every 20s for up to 15 minutes.
# MAGIC Target state: `ONLINE_CONTINUOUS_UPDATE`

# COMMAND ----------

print(f"Polling online table status: {ONLINE_TABLE_NAME}")
READY_STATES = {"ONLINE_CONTINUOUS_UPDATE", "ACTIVE", "ONLINE", "ACTIVE_CONTINUOUS"}

for i in range(45):
    poll = requests.get(
        f"https://{HOST}/api/2.0/online-tables/{ONLINE_TABLE_NAME}",
        headers=HEADERS
    )
    if poll.status_code != 200:
        print(f"  [{i*20}s] Not found yet (status {poll.status_code})")
        time.sleep(20)
        continue

    status = poll.json().get("status", {})
    state  = status.get("detailed_state", "unknown")
    msg    = status.get("message", "")
    print(f"  [{i*20}s] state={state}  {msg[:80] if msg else ''}")

    if state in READY_STATES:
        print(f"\nOnline table is READY ({state}). Proceed to Step 4.")
        break
    if "FAILED" in state.upper():
        print(f"\nOnline table FAILED: {msg}")
        print("Check Catalog Explorer → auto_features_active for error details.")
        break
    time.sleep(20)
else:
    print("Timed out. Check Databricks UI → Data → online table for current status.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Create Feature Spec

# COMMAND ----------

try:
    fe.get_feature_spec(name=FEATURE_SPEC_NAME)
    print(f"Feature spec already exists: {FEATURE_SPEC_NAME}")
except Exception:
    try:
        fe.create_feature_spec(
            name=FEATURE_SPEC_NAME,
            features=[
                FeatureLookup(
                    table_name=ACTIVE_TABLE,
                    lookup_key="auto_policy_id",
                    feature_names=FEATURE_COLUMNS,
                )
            ],
        )
        print(f"Feature spec created: {FEATURE_SPEC_NAME}")
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"Feature spec already exists: {FEATURE_SPEC_NAME}")
        else:
            raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Create Feature Serving Endpoint
# MAGIC
# MAGIC If the endpoint exists but is in UPDATE_FAILED state, delete it and recreate.

# COMMAND ----------

ep_resp = requests.get(
    f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=HEADERS
)

if ep_resp.status_code == 200:
    ep_data   = ep_resp.json()
    ep_ready  = ep_data.get("state", {}).get("ready", "unknown")
    ep_update = ep_data.get("state", {}).get("config_update", "")
    print(f"Endpoint exists: ready={ep_ready}  config_update={ep_update}")

    if ep_update == "UPDATE_FAILED":
        print("Endpoint is in UPDATE_FAILED state — deleting and recreating...")
        del_resp = requests.delete(
            f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
            headers=HEADERS
        )
        if del_resp.status_code in (200, 204):
            print("Deleted. Recreating...")
            time.sleep(5)
            ep_resp = requests.get(
                f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
                headers=HEADERS
            )
        else:
            print(f"Delete failed {del_resp.status_code}: {del_resp.text[:200]}")

if ep_resp.status_code != 200:
    print(f"Creating endpoint: {ENDPOINT_NAME} ...")
    body = {
        "name": ENDPOINT_NAME,
        "config": {
            "served_entities": [
                {
                    "name": "auto-policy-features",
                    "entity_name": FEATURE_SPEC_NAME,
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True,
                }
            ]
        }
    }
    cr = requests.post(
        f"https://{HOST}/api/2.0/serving-endpoints",
        headers=HEADERS, json=body
    )
    if cr.status_code not in (200, 201):
        raise RuntimeError(f"Create failed {cr.status_code}: {cr.text[:400]}")

    print("Endpoint created. Waiting for READY (up to 20 min)...")
    for i in range(60):
        poll = requests.get(
            f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
            headers=HEADERS
        )
        ep     = poll.json().get("state", {})
        ready  = ep.get("ready", "unknown")
        update = ep.get("config_update", "")
        print(f"  [{i*20}s] ready={ready}  config_update={update}")
        if ready == "READY":
            print("Endpoint is READY.")
            break
        if update == "UPDATE_FAILED":
            print("FAILED again. Check Databricks UI > Serving > Logs for details.")
            break
        time.sleep(20)
else:
    ep_ready = ep_resp.json().get("state", {}).get("ready", "unknown")
    if ep_ready == "READY":
        print("Endpoint already READY.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Query: pass `auto_policy_id`, get all 11 features

# COMMAND ----------

# Check endpoint is READY before querying
ep_check = requests.get(
    f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=HEADERS
)

if ep_check.status_code != 200:
    raise RuntimeError(
        f"Endpoint '{ENDPOINT_NAME}' does not exist.\n"
        "  → Run Step 5 first to create the endpoint."
    )

ep_state  = ep_check.json().get("state", {})
ready     = ep_state.get("ready", "unknown")
update    = ep_state.get("config_update", "")
print(f"Endpoint state  : ready={ready}  config_update={update}")

if ready != "READY":
    raise RuntimeError(
        f"Endpoint not ready (ready={ready}  config_update={update}).\n"
        "  → Wait a few minutes then re-run this cell.\n"
        "  → If config_update=UPDATE_FAILED, re-run Step 5."
    )

print("Endpoint is READY — querying now...\n")


def query_features(auto_policy_id: str):
    url     = f"https://{HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations"
    payload = {"dataframe_records": [{"auto_policy_id": auto_policy_id}]}

    start    = time.time()
    response = requests.post(url, headers=HEADERS, json=payload, timeout=10)
    latency  = round((time.time() - start) * 1000, 1)

    if response.status_code != 200:
        raise RuntimeError(f"{response.status_code}: {response.text}")

    features = response.json()["outputs"][0]

    print(f"Policy ID  : {auto_policy_id}")
    print(f"Latency    : {latency} ms")
    print("-" * 45)
    for col in FEATURE_COLUMNS:
        print(f"  {col:<35} = {features.get(col)}")
    return features


result = query_features("POL001")
print(f"\nlogin_count_7d for POL001 = {result.get('login_count_7d')}")
