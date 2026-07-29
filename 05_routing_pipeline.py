# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Routing Pipeline
# MAGIC
# MAGIC Reads COMPUTED_FEATURE_EVENTS via CDF stream.
# MAGIC For each new computed row, reads FEATURE_GROUP_REGISTRY and
# MAGIC routes each feature group independently based on its routing_type
# MAGIC and its own effective date. Column lists per group come from
# MAGIC `REGISTRY.OWNED_COLUMNS` — no column names are hardcoded here.
# MAGIC
# MAGIC ## Routing Logic
# MAGIC
# MAGIC | routing_type | effective_date_col | Decision |
# MAGIC |---|---|---|
# MAGIC | immediate | SRC_TRANS_TMSP | Always MERGE to ACTIVE now (GROUP_A) |
# MAGIC | effective_date | EFF_DT | <= now → MERGE to ACTIVE, > now → INSERT to PENDING (GROUP_B) |
# MAGIC
# MAGIC ## Flow per event
# MAGIC ```
# MAGIC For each event (in SRC_TRANS_TMSP order):
# MAGIC   1. Categorise all feature groups → immediate vs pending
# MAGIC   2. If any immediate:
# MAGIC        a. Close old IS_CURRENT history row ONCE (earliest effective date - 1ms)
# MAGIC        b. MERGE each immediate group's OWNED_COLUMNS to ACTIVE
# MAGIC        c. Snapshot ACTIVE → HISTORY ONCE  (single IS_CURRENT=true)
# MAGIC   3. For each pending group → MERGE its OWNED_COLUMNS to PENDING
# MAGIC ```
# MAGIC
# MAGIC ## Why event-by-event (not one SQL per feature group across the batch)
# MAGIC Processing events one at a time and closing history ONCE per event prevents the
# MAGIC negative-validity-window bug that occurs when two feature groups both route
# MAGIC immediate for the same event (e.g. new business where SRC_TRANS_TMSP == EFF_DT).

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("catalog",         "main")
dbutils.widgets.text("schema",          "final_database")
dbutils.widgets.text("checkpoint_path", "/Volumes/main/final_database/checkpoints/routing")
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers

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
        # Earliest effective date across ALL immediate groups this event touches.
        # This is the point in time where the existing IS_CURRENT row ends.
        earliest_eff   = min(d for _, d, _ in immediate_groups)
        changed_groups = ",".join(g for g, _, _ in immediate_groups)

        # Step 1: Close old IS_CURRENT ONCE
        spark.sql(f"""
            UPDATE {HISTORY}
            SET    IS_CURRENT           = false,
                   VALID_TRANSACTION_TO = TIMESTAMP '{earliest_eff}' - INTERVAL 1 MILLISECOND
            WHERE  AUTO_POLICY_ID = '{policy}'
              AND  IS_CURRENT     = true
        """)
        print(f"    Closed old IS_CURRENT → VALID_TO = {earliest_eff} - 1ms")

        # Step 2: MERGE each immediate group's OWNED_COLUMNS into ACTIVE
        for feature_group, _, owned_columns in immediate_groups:
            _merge_group_to_active(policy, trans_id, event, feature_group, owned_columns)
            print(f"    [{feature_group}] merged {len(owned_columns)} column(s) → ACTIVE")

        # Step 3: Snapshot ACTIVE → HISTORY ONCE (one IS_CURRENT=true row per policy)
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

        # Process events in SRC_TRANS_TMSP order — ensures correct SCD2 chain
        # when multiple events for the same policy land in the same micro-batch
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
# MAGIC ## Start Streaming from COMPUTED_FEATURE_EVENTS via CDF

# COMMAND ----------

query = (
    spark.readStream
         .format("delta")
         .option("readChangeFeed", "true")
         .option("startingVersion", "0")
         .table(COMPUTED)
         .writeStream
         .foreachBatch(process_micro_batch)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(availableNow=True)
         .start()
)

query.awaitTermination()
print("\nRouting pipeline finished.")

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
