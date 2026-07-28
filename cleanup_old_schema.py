# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup: Old Schema (one-off utility — not a pipeline step)
# MAGIC
# MAGIC Drops the old-design tables and empties the two volumes in `ws_prd_analytics.default`
# MAGIC so this schema can be reused for the new GROUP_A/GROUP_B pipeline (`01_setup_tables.py`
# MAGIC onward), since new catalog/schema creation isn't available in this workspace.
# MAGIC
# MAGIC Leaves the `auto_policy_feature_spec` function untouched, as requested.
# MAGIC Run this once, then re-run `01_setup_tables.py` with `catalog=ws_prd_analytics`,
# MAGIC `schema=default` to create the new tables fresh in the same place.

# COMMAND ----------

CATALOG = "ws_prd_analytics"
SCHEMA  = "default"

print(f"Cleaning: {CATALOG}.{SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Drop old tables

# COMMAND ----------

OLD_TABLES = [
    "auto_features_active",
    "auto_features_dead_letter",
    "auto_features_history",
    "auto_features_pending",
    "auto_features_raw_events_log",
    "auto_policy_login_events",
    "feature_group_registry",
    "policy_drivers",
    "policy_history",
    "policy_vehicles",
]

for tbl in OLD_TABLES:
    full_name = f"{CATALOG}.{SCHEMA}.{tbl}"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    print(f"  Dropped: {full_name}")

print("\nAll old tables dropped (or already absent).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Empty the volumes (contents only — volumes themselves are kept)

# COMMAND ----------

VOLUMES = ["landing", "streaming"]

for vol in VOLUMES:
    vol_path = f"/Volumes/{CATALOG}/{SCHEMA}/{vol}"
    try:
        files = dbutils.fs.ls(vol_path)
    except Exception as e:
        print(f"  Skipping {vol_path} — {e}")
        continue

    for f in files:
        dbutils.fs.rm(f.path, recurse=True)
        print(f"  Deleted: {f.path}")

    print(f"  Emptied: {vol_path}")

print("\nVolumes emptied. Volume objects themselves were NOT dropped.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify — should be empty / gone

# COMMAND ----------

print("=== Remaining tables in schema ===")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

print("=== auto_policy_feature_spec — left untouched ===")
display(spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.{SCHEMA} LIKE 'auto_policy_feature_spec'"))

for vol in VOLUMES:
    vol_path = f"/Volumes/{CATALOG}/{SCHEMA}/{vol}"
    try:
        remaining = dbutils.fs.ls(vol_path)
        print(f"{vol_path}: {len(remaining)} item(s) remaining")
    except Exception as e:
        print(f"{vol_path}: {e}")
