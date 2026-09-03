# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Routing Pipeline (Fast Lane — skips the existing backlog)
# MAGIC
# MAGIC Identical routing logic to `05_routing_pipeline.py`, with ONE difference: this stream starts
# MAGIC at `startingVersion = "latest"` instead of `"0"`, using its OWN checkpoint
# MAGIC (`.../checkpoints/routing_fast_lane`, separate from `05`'s `.../checkpoints/routing`).
# MAGIC
# MAGIC ## Why this exists
# MAGIC `05` replays the ENTIRE change history of `COMPUTED_FEATURE_EVENTS` from version 0 forward
# MAGIC the first time it runs (or whenever its checkpoint is behind) — for a large backlog (e.g. the
# MAGIC ~11K-row bulk backfill), each event costs ~4 sequential SQL statements, which can take hours.
# MAGIC This fast-lane version skips all of that and ONLY processes transactions that land in
# MAGIC `COMPUTED_FEATURE_EVENTS` from the moment THIS notebook starts, going forward.
# MAGIC
# MAGIC ## IMPORTANT TRADE-OFF — read before running
# MAGIC Everything currently sitting in `COMPUTED_FEATURE_EVENTS` (the entire existing backlog,
# MAGIC including whatever `04` already computed) will NEVER be routed into `ACTIVE`/`HISTORY` by
# MAGIC this notebook — it is permanently skipped from this stream's point of view. To eventually
# MAGIC route that backlog, run the ORIGINAL `05_routing_pipeline.py` separately (its own checkpoint
# MAGIC is untouched by this file, so it still works whenever you have time to let it run in full).
# MAGIC
# MAGIC **Caution:** any single transaction that ends up processed by BOTH this fast lane AND a later
# MAGIC full run of `05` will be routed twice, creating a duplicate `HISTORY` snapshot row for it.
# MAGIC Check for and clean up duplicate `EVENT_ID`s in `HISTORY` if/when you later run `05` to catch
# MAGIC up the backlog.
# MAGIC
# MAGIC Standalone — does not modify `05_routing_pipeline.py`.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("catalog",         "ws_prd_analytics")
dbutils.widgets.text("schema",          "default")
dbutils.widgets.text("checkpoint_path", "/Volumes/ws_prd_analytics/default/checkpoints/routing_fast_lane")
dbutils.widgets.text("sim_date",        "")   # YYYY-MM-DD HH:MM:SS or blank for real time

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
_sim            = dbutils.widgets.get("sim_date").strip()

if _sim:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            EFFECTIVE_TS = datetime.strptime(_sim, fmt).strftime("%Y-%m-%d %H:%M:%S")
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse sim_date '{_sim}'. Use YYYY-MM-DD HH:MM:SS")
    print(f"Routing date : {EFFECTIVE_TS}  ← sim_date override")
else:
    EFFECTIVE_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Routing date : {EFFECTIVE_TS}  ← real current_timestamp()")

COMPUTED = f"{CATALOG}.{SCHEMA}.COMPUTED_FEATURE_EVENTS"
ACTIVE   = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_ACTIVE"
PENDING  = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_PENDING"
HISTORY  = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_HISTORY"
REGISTRY = f"{CATALOG}.{SCHEMA}.FEATURE_GROUP_REGISTRY"

print(f"Checkpoint (fast lane, separate from 05): {CHECKPOINT_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers — identical logic to `05_routing_pipeline.py`

# COMMAND ----------

def _sql_value(v):
    """Safe SQL literal for a Python value pulled off a computed-feature Row."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _merge_group_to_active(policy, trans_id, event, feature_group, owned_columns):
    """MERGE one feature group's columns into ACTIVE, inserting the row if it's new."""
    updated_at_col = f"{feature_group}_UPDATED_AT"

    select_cols = ", ".join(f"{_sql_value(event[c])} AS {c}" for c in owned_columns)
    set_clause  = ", ".join(f"t.{c} = s.{c}" for c in owned_columns)
    insert_cols = ", ".join(owned_columns)
    insert_vals = ", ".join(f"s.{c}" for c in owned_columns)

    spark.sql(f"""
        MERGE INTO {ACTIVE} AS t
        USING (SELECT
            '{policy}'   AS AUTO_POLICY_ID,
            '{trans_id}' AS TRANS_ID,
            {_sql_value(event["PLCY_CNTRCT_NUM"])} AS PLCY_CNTRCT_NUM,
            {_sql_value(event["SRC_HH_NUM"])}      AS SRC_HH_NUM,
            {select_cols}
        ) AS s
        ON t.AUTO_POLICY_ID = s.AUTO_POLICY_ID
        WHEN MATCHED THEN UPDATE SET
            {set_clause},
            t.PLCY_CNTRCT_NUM        = s.PLCY_CNTRCT_NUM,
            t.SRC_HH_NUM             = s.SRC_HH_NUM,
            t.{updated_at_col}       = current_timestamp(),
            t.TRANS_ID               = s.TRANS_ID,
            t.FEATURE_UPDATED_AT     = current_timestamp(),
            t.LAST_SOURCE_EVENT_ID   = s.TRANS_ID
        WHEN NOT MATCHED THEN INSERT (
            AUTO_POLICY_ID, TRANS_ID, PLCY_CNTRCT_NUM, SRC_HH_NUM, {insert_cols}, {updated_at_col},
            FEATURE_UPDATED_AT, LAST_SOURCE_EVENT_ID
        ) VALUES (
            s.AUTO_POLICY_ID, s.TRANS_ID, s.PLCY_CNTRCT_NUM, s.SRC_HH_NUM, {insert_vals}, current_timestamp(),
            current_timestamp(), s.TRANS_ID
        )
    """)


def _merge_group_to_pending(policy, trans_id, event, feature_group, owned_columns, group_eff_date):
    select_cols = ", ".join(f"{_sql_value(event[c])} AS {c}" for c in owned_columns)
    set_clause  = ", ".join(f"t.{c} = s.{c}" for c in owned_columns)
    insert_cols = ", ".join(owned_columns)
    insert_vals = ", ".join(f"s.{c}" for c in owned_columns)

    spark.sql(f"""
        MERGE INTO {PENDING} AS t
        USING (SELECT
            '{policy}'     AS AUTO_POLICY_ID,
            '{trans_id}'   AS TRANS_ID,
            {select_cols},
            TIMESTAMP '{group_eff_date}' AS EFFECTIVE_DATE
        ) AS s
        ON  t.AUTO_POLICY_ID  = s.AUTO_POLICY_ID
        AND t.SOURCE_EVENT_ID = s.TRANS_ID
        WHEN MATCHED THEN UPDATE SET
            {set_clause},
            t.EVENT_RECEIVED_AT = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            AUTO_POLICY_ID, TRANS_ID,
            {insert_cols},
            EFFECTIVE_DATE, CHANGED_FEATURE_GROUP,
            SOURCE_EVENT_ID, EVENT_RECEIVED_AT
        ) VALUES (
            s.AUTO_POLICY_ID, s.TRANS_ID,
            {insert_vals},
            s.EFFECTIVE_DATE, '{feature_group}',
            s.TRANS_ID, current_timestamp()
        )
    """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Route a Single Event

# COMMAND ----------

def _route_single_event(event, registry):
    """
    Route one computed event through the feature store.

    Closes the existing IS_CURRENT history row ONCE using the earliest
    effective date across all immediate feature groups — preventing the
    negative-validity-window bug when SRC_TRANS_TMSP == EFF_DT.
    """
    policy   = event["AUTO_POLICY_ID"]
    trans_id = event["TRANS_ID"]

    print(f"\n  [{trans_id}] Policy={policy} | src_trans_tmsp={event['SRC_TRANS_TMSP']} | eff_dt={event['EFF_DT']}")

    immediate_groups = []
    pending_groups   = []

    for reg in registry:
        feature_group  = reg["FEATURE_GROUP"]
        routing_type   = reg["ROUTING_TYPE"]
        eff_date_col   = reg["EFFECTIVE_DATE_COL"]
        owned_columns  = reg["OWNED_COLUMNS"]
        group_eff_date = str(event[eff_date_col])

        if routing_type == "immediate" or group_eff_date <= EFFECTIVE_TS:
            immediate_groups.append((feature_group, group_eff_date, owned_columns))
            tag = "always-immediate" if routing_type == "immediate" else f"eff {group_eff_date} <= now"
            print(f"    [{feature_group}] → IMMEDIATE ({tag})")
        else:
            pending_groups.append((feature_group, group_eff_date, owned_columns))
            print(f"    [{feature_group}] → PENDING   (eff {group_eff_date} > {EFFECTIVE_TS})")

    # ── IMMEDIATE GROUPS ──────────────────────────────────────────────
    if immediate_groups:
        earliest_eff   = min(d for _, d, _ in immediate_groups)
        changed_groups = ",".join(g for g, _, _ in immediate_groups)

        spark.sql(f"""
            UPDATE {HISTORY}
            SET    IS_CURRENT           = false,
                   VALID_TRANSACTION_TO = TIMESTAMP '{earliest_eff}' - INTERVAL 1 MILLISECOND
            WHERE  AUTO_POLICY_ID = '{policy}'
              AND  IS_CURRENT     = true
        """)
        print(f"    Closed old IS_CURRENT → VALID_TO = {earliest_eff} - 1ms")

        for feature_group, _, owned_columns in immediate_groups:
            _merge_group_to_active(policy, trans_id, event, feature_group, owned_columns)
            print(f"    [{feature_group}] merged {len(owned_columns)} column(s) → ACTIVE")

        spark.sql(f"""
            INSERT INTO {HISTORY}
            SELECT
                a.*,
                current_timestamp()        AS SNAPSHOT_AT,
                'immediate_merge'          AS REASON,
                '{trans_id}'                AS EVENT_ID,
                '{changed_groups}'         AS CHANGED_FEATURE_GROUP,
                TIMESTAMP '{earliest_eff}' AS VALID_TRANSACTION_FROM,
                NULL                       AS VALID_TRANSACTION_TO,
                true                       AS IS_CURRENT
            FROM {ACTIVE} a
            WHERE a.AUTO_POLICY_ID = '{policy}'
        """)
        print(f"    Snapshotted → HISTORY: VALID_FROM={earliest_eff} | groups={changed_groups}")

    # ── PENDING GROUPS ────────────────────────────────────────────────
    for feature_group, group_eff_date, owned_columns in pending_groups:
        _merge_group_to_pending(policy, trans_id, event, feature_group, owned_columns, group_eff_date)
        print(f"    [{feature_group}] → PENDING ({len(owned_columns)} column(s), EFFECTIVE_DATE={group_eff_date})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## foreachBatch — Registry-Driven Routing

# COMMAND ----------

def process_micro_batch(df, batch_id):
    import traceback
    try:
        df    = df.filter(F.col("_change_type") == "insert")
        count = df.count()
        print(f"\n[batch {batch_id}] {count} computed event(s)")
        if count == 0:
            return

        registry = spark.table(REGISTRY).collect()

        events = df.orderBy("SRC_TRANS_TMSP", "TRANS_ID").collect()

        for event in events:
            _route_single_event(event, registry)

        print(f"\n[batch {batch_id}] done.")

    except Exception as e:
        print(f"[ERROR] batch {batch_id}: {e}")
        traceback.print_exc()
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start Streaming — `startingVersion = "latest"` (skips the entire existing backlog)

# COMMAND ----------

query = (
    spark.readStream
         .format("delta")
         .option("readChangeFeed", "true")
         .option("startingVersion", "latest")
         .table(COMPUTED)
         .writeStream
         .foreachBatch(process_micro_batch)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(availableNow=True)
         .start()
)

query.awaitTermination()
print("\nFast-lane routing pipeline finished — only processed transactions landed after this run started.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=== AUTO_FEATURES_ACTIVE ===")
display(spark.table(ACTIVE).orderBy("AUTO_POLICY_ID"))

print("=== AUTO_FEATURES_PENDING (future-dated) ===")
display(spark.table(PENDING).orderBy("EFFECTIVE_DATE"))

print("=== AUTO_FEATURES_HISTORY (all snapshots) ===")
display(spark.table(HISTORY).orderBy("AUTO_POLICY_ID", "VALID_TRANSACTION_FROM"))
