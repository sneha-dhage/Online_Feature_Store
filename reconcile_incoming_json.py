# Databricks notebook source
# MAGIC %md
# MAGIC # Reconcile incoming_json Folder Against RAW_POLICY_EVENTS
# MAGIC
# MAGIC `03_autoloader_landing_zone.py`'s incremental Auto Loader can silently miss files —
# MAGIC observed cases: files with a modification timestamp lower than its internal watermark
# MAGIC (backdated files), and more recently, files that should be newer than anything already
# MAGIC landed still not getting picked up. Rather than chase Auto Loader's internal cursor state,
# MAGIC this directly compares every `.json` file actually sitting in the volume against what's
# MAGIC already in `RAW_POLICY_EVENTS` (by `TRANS_ID`, read from each file's own content) and lands
# MAGIC anything missing — using the exact same validation logic `03` uses.
# MAGIC
# MAGIC Safe to re-run anytime — already-landed files are skipped via the same `TRANS_ID` check `03`
# MAGIC uses. Does not touch `03`'s checkpoint or any other file.

# COMMAND ----------

import json

dbutils.widgets.text("catalog",         "ws_prd_analytics")
dbutils.widgets.text("schema",          "default")
dbutils.widgets.text("incoming_volume", "/Volumes/ws_prd_analytics/default/incoming_json")

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
INCOMING_VOLUME = dbutils.widgets.get("incoming_volume")

RAW_EVENTS = f"{CATALOG}.{SCHEMA}.RAW_POLICY_EVENTS"
DLQ        = f"{CATALOG}.{SCHEMA}.RAW_EVENTS_DLQ"

print(f"Scanning : {INCOMING_VOLUME}")
print(f"Landing  : {RAW_EVENTS}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Same helpers `03_autoloader_landing_zone.py` uses

# COMMAND ----------

def _clean_ts(ts):
    return ts.strip().replace("T", " ").split(".")[0]


def _sql_escape(value):
    return str(value).replace("'", "''")

# COMMAND ----------
# MAGIC %md
# MAGIC ## List every .json file, land whatever's missing from RAW_POLICY_EVENTS

# COMMAND ----------

existing_ids = {
    r["TRANS_ID"] for r in spark.sql(f"SELECT DISTINCT TRANS_ID FROM {RAW_EVENTS}").collect()
}
print(f"Already landed (RAW_POLICY_EVENTS): {len(existing_ids)}")

files = [f for f in dbutils.fs.ls(INCOMING_VOLUME) if f.name.lower().endswith(".json")]
print(f"JSON files found in folder: {len(files)}")

landed_count  = 0
skipped_count = 0
dlq_count     = 0

for f in files:
    try:
        raw_payload = dbutils.fs.head(f.path, 1_000_000)
        event = json.loads(raw_payload)

        trans_id   = str(event.get("auto_plcy_trans_sk", "")).strip()
        policy_id  = str(event.get("plcy_id_sk", "")).strip()
        cntrct_num = str(event.get("plcy_cntrct_num", "") or "").strip()
        hh_num     = str(event.get("src_hh_num", "") or "").strip()
        eff_dt     = str(event.get("eff_dt", "")).strip()
        trans_tmsp = str(event.get("src_trans_tmsp", "")).strip()

        missing = [fld for fld, v in {
            "auto_plcy_trans_sk": trans_id, "plcy_id_sk": policy_id,
            "eff_dt": eff_dt, "src_trans_tmsp": trans_tmsp,
        }.items() if not v]

        if missing:
            raise ValueError(f"Missing JSON header fields: {missing}")
        if not isinstance(event.get("1_vehicle_snapshot"), list) or not event["1_vehicle_snapshot"]:
            raise ValueError("1_vehicle_snapshot is missing or empty")

        if trans_id in existing_ids:
            skipped_count += 1
            continue

        spark.sql(f"""
            INSERT INTO {RAW_EVENTS}
            (TRANS_ID, AUTO_POLICY_ID, PLCY_CNTRCT_NUM, SRC_HH_NUM, EFF_DT, SRC_TRANS_TMSP, RAW_PAYLOAD, INGESTED_AT)
            VALUES (
                '{_sql_escape(trans_id)}', '{_sql_escape(policy_id)}', '{_sql_escape(cntrct_num)}', '{_sql_escape(hh_num)}',
                TIMESTAMP '{_clean_ts(eff_dt)}', TIMESTAMP '{_clean_ts(trans_tmsp)}',
                '{_sql_escape(raw_payload)}', current_timestamp()
            )
        """)
        existing_ids.add(trans_id)
        landed_count += 1
        print(f"  [LANDED] {f.name} -> TRANS_ID={trans_id} policy={policy_id}")

    except Exception as e:
        dlq_count += 1
        spark.sql(f"""
            INSERT INTO {DLQ} (RAW_PAYLOAD, ERROR_MESSAGE, FAILED_AT, SOURCE_NAME)
            VALUES ('{_sql_escape(f.path)}', '{_sql_escape(str(e))}', current_timestamp(), 'reconcile_incoming_json')
        """)
        print(f"  [DLQ] {f.name}: {e}")

print(f"\nDone. Landed: {landed_count}  Already had: {skipped_count}  Routed to DLQ: {dlq_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.table(RAW_EVENTS).orderBy(spark.table(RAW_EVENTS).INGESTED_AT.desc()).limit(20))

print("\n>>> Next: run 04_sp_compute_features, then 05 (or the fast lane) <<<")
