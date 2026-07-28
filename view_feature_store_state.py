# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store Viewer — Active / Pending / History (demo utility — not a pipeline step)
# MAGIC
# MAGIC Clean, presentation-friendly view of the three Feature Store tables for one policy —
# MAGIC run this before and after publishing a test transaction to show an audience exactly
# MAGIC what changed and where.
# MAGIC
# MAGIC - **ACTIVE**   — the live feature values right now
# MAGIC - **PENDING**  — future-dated GROUP_B changes waiting for their `EFFECTIVE_DATE`
# MAGIC - **HISTORY**  — the full SCD2 timeline for this policy, oldest → newest

# COMMAND ----------

dbutils.widgets.text("catalog",   "ws_prd_analytics")
dbutils.widgets.text("schema",    "default")
dbutils.widgets.text("policy_id", "503203043")   # blank = show all policies

CATALOG   = dbutils.widgets.get("catalog")
SCHEMA    = dbutils.widgets.get("schema")
POLICY_ID = dbutils.widgets.get("policy_id").strip()

ACTIVE  = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_ACTIVE"
PENDING = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_PENDING"
HISTORY = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_HISTORY"

_filter = f"WHERE AUTO_POLICY_ID = '{POLICY_ID}'" if POLICY_ID else ""
print(f"Showing: {'policy ' + POLICY_ID if POLICY_ID else 'ALL policies'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. ACTIVE — live feature values right now

# COMMAND ----------

print("=" * 70)
print("  AUTO_FEATURES_ACTIVE — current state")
print("=" * 70)
display(spark.sql(f"SELECT * FROM {ACTIVE} {_filter} ORDER BY AUTO_POLICY_ID"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. PENDING — future-dated changes waiting for their effective date

# COMMAND ----------

print("=" * 70)
print("  AUTO_FEATURES_PENDING — waiting to be promoted")
print("=" * 70)

pending_df = spark.sql(f"SELECT * FROM {PENDING} {_filter} ORDER BY EFFECTIVE_DATE")
pending_count = pending_df.count()

if pending_count == 0:
    print("  (nothing pending — every known change is already active)")
else:
    print(f"  {pending_count} row(s) waiting")
    display(pending_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. HISTORY — the full timeline, oldest → newest
# MAGIC
# MAGIC `IS_CURRENT = true` marks the version that matches ACTIVE right now.
# MAGIC Every row above it is a superseded snapshot — this is the "how did we get here" view.

# COMMAND ----------

print("=" * 70)
print("  AUTO_FEATURES_HISTORY — full SCD2 timeline")
print("=" * 70)

history_df = spark.sql(f"""
    SELECT * FROM {HISTORY}
    {_filter}
    ORDER BY AUTO_POLICY_ID, VALID_TRANSACTION_FROM
""")
print(f"  {history_df.count()} snapshot(s) total")
display(history_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Online Store — same feature values, retrieved by id in milliseconds
# MAGIC
# MAGIC This is the "Delta table vs. serving layer" moment of the demo: everything above
# MAGIC comes from Delta tables (seconds-scale queries); this comes from the Lakebase-backed
# MAGIC Feature Serving Endpoint set up in `08_online_feature_store.py` — pass a policy id,
# MAGIC get every feature back over REST, no Spark job involved.

# COMMAND ----------

import time, requests

dbutils.widgets.text("endpoint_name", "auto-policy-feature-serving")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

HOST    = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

GROUP_A_COLUMN_NAMES = [
    "TXN_CNT_1D", "TXN_CNT_1W", "TXN_CNT_1M",
    "OUTSTANDING_TXN_IND", "TENURE_YRS",
]
GROUP_B_COLUMN_NAMES = [
    "VEH_CNT", "AVG_VEH_AGE", "VEH_ADDED_LATEST_TXN",
    "DRVR_CNT", "MAX_DRVR_AGE", "MIN_DRVR_AGE", "AVG_DRVR_AGE", "DRVRS_ADDED_LATEST_TXN",
    "PREM_CHANGE_AMT",
    "BI_COVERAGE_ADDED", "BI_COVERAGE_REMOVED",
    "PD_COVERAGE_ADDED", "PD_COVERAGE_REMOVED",
    "MD_COVERAGE_ADDED", "MD_COVERAGE_REMOVED",
    "COLL_COVERAGE_ADDED", "COLL_COVERAGE_REMOVED",
    "COMP_COVERAGE_ADDED", "COMP_COVERAGE_REMOVED",
    "PIP_COVERAGE_ADDED", "PIP_COVERAGE_REMOVED",
    "COMP_DED_CHANGE_IND", "GNDR_CHANGE_IND", "MRTL_CHANGE_IND",
]
FEATURE_COLUMNS = GROUP_A_COLUMN_NAMES + GROUP_B_COLUMN_NAMES


def get_features_from_online_store(auto_policy_id: str):
    url     = f"https://{HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations"
    payload = {"dataframe_records": [{"AUTO_POLICY_ID": auto_policy_id}]}

    start    = time.time()
    response = requests.post(url, headers=HEADERS, json=payload, timeout=120)
    latency  = round((time.time() - start) * 1000, 1)

    if response.status_code != 200:
        print(f"  Online store query failed ({response.status_code}) — is the endpoint READY? "
              f"Run 08_online_feature_store.py first if not.")
        print(f"  {response.text[:300]}")
        return None, latency

    features = response.json()["outputs"][0]
    print(f"  AUTO_POLICY_ID : {auto_policy_id}")
    print(f"  Latency        : {latency} ms")
    print("  " + "-" * 45)
    for col in FEATURE_COLUMNS:
        print(f"    {col:<30} = {features.get(col)}")
    return features, latency


print("=" * 70)
print("  ONLINE STORE — retrieved via Feature Serving Endpoint")
print("=" * 70)

online_features, online_latency = None, None
if POLICY_ID:
    online_features, online_latency = get_features_from_online_store(POLICY_ID)
else:
    print("  (set policy_id widget to a specific policy to query the online store)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quick summary line — good for narrating a demo

# COMMAND ----------

if POLICY_ID:
    row = spark.sql(f"SELECT * FROM {ACTIVE} WHERE AUTO_POLICY_ID = '{POLICY_ID}'").collect()
    if row:
        r = row[0]
        latency_note = f"{online_latency} ms" if online_latency is not None else "n/a"
        print(f"Policy {POLICY_ID}: VEH_CNT={r['VEH_CNT']}, DRVR_CNT={r['DRVR_CNT']}, "
              f"TXN_CNT_1M={r['TXN_CNT_1M']}, {pending_count} change(s) pending, "
              f"{history_df.count()} historical snapshot(s), "
              f"online store retrieval={latency_note}.")
    else:
        print(f"Policy {POLICY_ID} not found in ACTIVE.")
