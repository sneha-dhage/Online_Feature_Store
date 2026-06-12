# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Online Feature Store
# MAGIC
# MAGIC ```
# MAGIC Step 1: Register auto_features_active as a Feature Table
# MAGIC Step 2: Create Feature Spec  (defines lookup: key=auto_policy_id, 11 features)
# MAGIC Step 3: Create Feature Serving Endpoint  (entity_name = Feature Spec)
# MAGIC Step 4: Query — pass auto_policy_id → get all 11 features in ~10ms
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
# MAGIC ## Step 2 — Create Feature Spec
# MAGIC
# MAGIC Defines the lookup: key = `auto_policy_id`, return all 11 feature columns.

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
# MAGIC ## Step 3 — Create Feature Serving Endpoint
# MAGIC
# MAGIC `entity_name` points to the Feature Spec.

# COMMAND ----------

resp = requests.get(
    f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=HEADERS
)

if resp.status_code == 200:
    state = resp.json().get("state", {}).get("ready", "unknown")
    print(f"Endpoint already exists: {ENDPOINT_NAME}  state: {state}")
else:
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
            print("FAILED. Check Databricks UI > Serving for error details.")
            break
        time.sleep(20)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Query: pass `auto_policy_id`, get all 11 features

# COMMAND ----------

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
    print("─" * 45)
    for col in FEATURE_COLUMNS:
        print(f"  {col:<35} = {features.get(col)}")
    return features


result = query_features("POL001")
print(f"\nlogin_count_7d for POL001 = {result.get('login_count_7d')}")
