# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Reset All (Tables + Checkpoints)
# MAGIC
# MAGIC Wipes the pipeline so it can be re-run from scratch with `01` → `02` → `03` → `04` → `05`.
# MAGIC
# MAGIC | What | Action |
# MAGIC |---|---|
# MAGIC | 7 pipeline tables | `DROP TABLE` (data is deleted) |
# MAGIC | `checkpoints` volume | every sub-folder deleted (`autoloader_landing`, `sp_compute`, `routing`, ...) |
# MAGIC | `incoming_json` volume | every file deleted by default — set `clear_incoming_json = false` to keep the JSON files |
# MAGIC
# MAGIC **Before running:** cancel any running streaming cell in `03` / `04` / `05` (e.g. the stuck
# MAGIC `05_routing_pipeline` query). A stream still running against a deleted checkpoint will fail or
# MAGIC recreate the folder.
# MAGIC
# MAGIC Nothing happens unless `confirm = YES`.

# COMMAND ----------

dbutils.widgets.text("catalog", "ws_prd_analytics")
dbutils.widgets.text("schema",  "featurestore_test")
dbutils.widgets.dropdown("clear_incoming_json", "true", ["true", "false"])
dbutils.widgets.text("confirm", "")   # type YES to actually delete

CATALOG             = dbutils.widgets.get("catalog")
SCHEMA              = dbutils.widgets.get("schema")
CLEAR_INCOMING_JSON = dbutils.widgets.get("clear_incoming_json") == "true"
CONFIRM             = dbutils.widgets.get("confirm").strip() == "YES"

CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
INCOMING_VOLUME   = f"/Volumes/{CATALOG}/{SCHEMA}/incoming_json"

TABLES = [
    "RAW_POLICY_EVENTS",
    "RAW_EVENTS_DLQ",
    "COMPUTED_FEATURE_EVENTS",
    "AUTO_FEATURES_ACTIVE",
    "AUTO_FEATURES_PENDING",
    "AUTO_FEATURES_HISTORY",
    "FEATURE_GROUP_REGISTRY",
]

print(f"Target              : {CATALOG}.{SCHEMA}")
print(f"Checkpoints         : {CHECKPOINT_VOLUME}")
print(f"Clear incoming_json : {CLEAR_INCOMING_JSON}")
print(f"Confirm             : {CONFIRM}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preview — what will be deleted

# COMMAND ----------

def _ls(path):
    try:
        return dbutils.fs.ls(path)
    except Exception:
        return []


existing_tables = {r["tableName"].upper() for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()}

print("=== Tables ===")
for t in TABLES:
    if t in existing_tables:
        cnt = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
        print(f"  DROP   {t:30s} ({cnt} rows)")
    else:
        print(f"  skip   {t:30s} (not found)")

print("\n=== Checkpoint folders ===")
checkpoint_dirs = _ls(CHECKPOINT_VOLUME)
for f in checkpoint_dirs:
    print(f"  DELETE {f.path}")
if not checkpoint_dirs:
    print("  (none)")

print("\n=== incoming_json files ===")
incoming_files = _ls(INCOMING_VOLUME)
for f in incoming_files:
    print(f"  {'DELETE' if CLEAR_INCOMING_JSON else 'keep  '} {f.name}")
if not incoming_files:
    print("  (none)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Delete

# COMMAND ----------

if not CONFIRM:
    raise Exception("Preview only — nothing deleted. Set the `confirm` widget to YES and re-run to delete.")

# Stop any stream still running in THIS session (streams in other notebooks must be cancelled manually)
for q in spark.streams.active:
    print(f"Stopping active stream: {q.name or q.id}")
    q.stop()

for t in TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")
    print(f"Dropped table     : {t}")

for f in checkpoint_dirs:
    dbutils.fs.rm(f.path, True)
    print(f"Deleted checkpoint: {f.path}")

if CLEAR_INCOMING_JSON:
    for f in incoming_files:
        dbutils.fs.rm(f.path, True)
        print(f"Deleted file      : {f.path}")

print("\nReset complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

remaining_tables = {r["tableName"].upper() for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()}
print("Pipeline tables left :", [t for t in TABLES if t in remaining_tables] or "none")
print("Checkpoint folders   :", [f.name for f in _ls(CHECKPOINT_VOLUME)] or "none")
print("incoming_json files  :", [f.name for f in _ls(INCOMING_VOLUME)] or "none")

print("\n>>> Next: run 01_setup_tables → 02_seed_data → 03 → 04 → 05 <<<")
