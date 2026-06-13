# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Online Feature Store (Lakebase)
# MAGIC
# MAGIC ```
# MAGIC Step 1: Create Lakebase Online Store
# MAGIC Step 2: Publish auto_features_active → Lakebase Online Store
# MAGIC Step 3: Create Feature Spec  (key=auto_policy_id, 11 features)
# MAGIC Step 4: Create Feature Serving Endpoint  (entity_name = Feature Spec)
# MAGIC Step 5: Query — pass auto_policy_id → get all 11 features in ~10ms
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.13.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time, requests
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup

dbutils.widgets.text("catalog",        "ws_prd_analytics")
dbutils.widgets.text("schema",         "default")
dbutils.widgets.text("endpoint_name",  "auto-insurance-feature-serving")
dbutils.widgets.text("online_store",   "auto-insurance-online-store")

CATALOG        = dbutils.widgets.get("catalog")
SCHEMA         = dbutils.widgets.get("schema")
ENDPOINT_NAME  = dbutils.widgets.get("endpoint_name")
ONLINE_STORE   = dbutils.widgets.get("online_store")

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

print(f"Active table      : {ACTIVE_TABLE}")
print(f"Online table name : {ONLINE_TABLE_NAME}")
print(f"Feature spec      : {FEATURE_SPEC_NAME}")
print(f"Online store      : {ONLINE_STORE}")
print(f"Endpoint          : {ENDPOINT_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Create Lakebase Online Store
# MAGIC
# MAGIC Provisions the Lakebase instance that backs fast feature lookups.
# MAGIC Skip if already exists.

# COMMAND ----------

try:
    existing = fe.get_online_store(name=ONLINE_STORE)
    print(f"Online store already exists: {ONLINE_STORE}  state={existing.state}")
except Exception:
    print(f"Creating online store: {ONLINE_STORE} ...")
    try:
        store = fe.create_online_store(
            name=ONLINE_STORE,
            capacity="CU_1"
        )
        print(f"Online store created: {store.name}  state={store.state}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Online store already exists: {ONLINE_STORE}")
        else:
            raise

# Wait for store to be ACTIVE
print("Waiting for online store to be ACTIVE...")
for i in range(30):
    try:
        store = fe.get_online_store(name=ONLINE_STORE)
        print(f"  [{i*10}s] state={store.state}")
        if store.state in ("ACTIVE", "RUNNING", "ONLINE"):
            print("Online store is ACTIVE.")
            break
    except Exception as e:
        print(f"  [{i*10}s] checking... ({e})")
    time.sleep(10)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Publish Feature Table to Online Store
# MAGIC
# MAGIC Syncs `auto_features_active` into the Lakebase online store.
# MAGIC Sets up continuous sync so the online store stays fresh.

# COMMAND ----------

online_store = fe.get_online_store(name=ONLINE_STORE)
print(f"Using online store: {online_store.name}  state={online_store.state}")

try:
    fe.publish_table(
        online_store=online_store,
        source_table_name=ACTIVE_TABLE,
        online_table_name=ONLINE_TABLE_NAME,
    )
    print(f"Table published: {ONLINE_TABLE_NAME}")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"Online table already published: {ONLINE_TABLE_NAME}")
    else:
        print(f"Publish error: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Create Feature Spec
# MAGIC
# MAGIC Registers the lookup definition: key=auto_policy_id, return 11 feature columns.

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
# MAGIC ## Step 4 — Create Feature Serving Endpoint
# MAGIC
# MAGIC Deletes the old failed endpoint if present, then creates fresh.

# COMMAND ----------

ep_resp = requests.get(
    f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=HEADERS
)

if ep_resp.status_code == 200:
    ep_state  = ep_resp.json().get("state", {})
    ep_ready  = ep_state.get("ready", "unknown")
    ep_update = ep_state.get("config_update", "")
    print(f"Endpoint exists: ready={ep_ready}  config_update={ep_update}")

    if ep_update == "UPDATE_FAILED":
        print("Deleting failed endpoint and recreating...")
        del_resp = requests.delete(
            f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
            headers=HEADERS
        )
        if del_resp.status_code in (200, 202, 204):
            print("Deleted. Waiting 10s...")
            time.sleep(10)
            ep_resp = requests.get(
                f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
                headers=HEADERS
            )
        else:
            print(f"Delete failed: {del_resp.status_code} {del_resp.text[:200]}")
    elif ep_ready == "READY":
        print("Endpoint already READY — skip to Step 5.")

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
            print("\nEndpoint is READY.")
            break
        if update == "UPDATE_FAILED":
            print("\nFAILED. Check Databricks UI > Serving > Logs.")
            break
        time.sleep(20)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Query: pass `auto_policy_id`, get all 11 features

# COMMAND ----------

# Verify endpoint is READY
ep_check = requests.get(
    f"https://{HOST}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=HEADERS
)
if ep_check.status_code != 200:
    raise RuntimeError(f"Endpoint '{ENDPOINT_NAME}' not found. Run Step 4 first.")

ep_state = ep_check.json().get("state", {})
ready    = ep_state.get("ready", "unknown")
update   = ep_state.get("config_update", "")
print(f"Endpoint state: ready={ready}  config_update={update}")

if ready != "READY":
    raise RuntimeError(
        f"Endpoint not ready (ready={ready}).\n"
        "  → Wait a few minutes and re-run this cell."
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
