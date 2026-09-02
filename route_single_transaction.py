# Databricks notebook source
# MAGIC %md
# MAGIC # Route a Single Transaction (bypasses `05`'s CDF backlog)
# MAGIC
# MAGIC For testing one specific `TRANS_ID` right now, without waiting for `05_routing_pipeline.py`
# MAGIC to catch up on everything else sitting in `COMPUTED_FEATURE_EVENTS`.
# MAGIC
# MAGIC `05` decides what's "new" via its own CDF checkpoint (`startingVersion = "0"`) — once it
# MAGIC runs, it replays every insert event in the table's history from version 0 forward, no matter
# MAGIC what the table currently contains. Deleting rows doesn't skip them. This notebook sidesteps
# MAGIC that entirely: a plain batch read for one `TRANS_ID`, routed with the exact same logic `05`
# MAGIC uses — no streaming, no checkpoint, so it never interferes with `05`'s own backlog.
# MAGIC
# MAGIC Standalone — does not modify `05_routing_pipeline.py`. The other pending records stay
# MAGIC untouched in `COMPUTED_FEATURE_EVENTS`, ready for `05` to process normally whenever you want.
# MAGIC
# MAGIC **Idempotency note:** unlike `05` (protected by its checkpoint), running this notebook twice
# MAGIC for the same `TRANS_ID` WOULD duplicate its `HISTORY` snapshot row — so this checks first and
# MAGIC refuses to re-route a `TRANS_ID` that's already been routed, unless you pass `force=true`.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("catalog",  "ws_prd_analytics")
dbutils.widgets.text("schema",   "default")
dbutils.widgets.text("trans_id", "")
dbutils.widgets.text("sim_date", "")   # YYYY-MM-DD HH:MM:SS or blank for real time
dbutils.widgets.dropdown("force", "false", ["false", "true"])

CATALOG  = dbutils.widgets.get("catalog")
SCHEMA   = dbutils.widgets.get("schema")
TRANS_ID = dbutils.widgets.get("trans_id").strip()
FORCE    = dbutils.widgets.get("force") == "true"
_sim     = dbutils.widgets.get("sim_date").strip()

if not TRANS_ID:
    raise ValueError("Set the 'trans_id' widget to the exact TRANS_ID you want to route.")

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

print(f"Target TRANS_ID: {TRANS_ID}")
print(f"Force re-route : {FORCE}")

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
# MAGIC ## Idempotency guard — refuse to double-route the same TRANS_ID

# COMMAND ----------

already_routed = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {HISTORY} WHERE EVENT_ID = '{TRANS_ID}'
""").first()["cnt"] > 0

if already_routed and not FORCE:
    raise ValueError(
        f"TRANS_ID '{TRANS_ID}' already has a HISTORY row (EVENT_ID match) — refusing to "
        f"re-route and duplicate it. Set the 'force' widget to 'true' if you really want to "
        f"re-run this (will create a duplicate HISTORY snapshot)."
    )
elif already_routed and FORCE:
    print(f"⚠️  TRANS_ID '{TRANS_ID}' already routed — proceeding anyway because force=true.")
else:
    print(f"'{TRANS_ID}' has not been routed yet — safe to proceed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch the one target row from COMPUTED_FEATURE_EVENTS (plain batch read, no CDF)

# COMMAND ----------

rows = spark.table(COMPUTED).filter(F.col("TRANS_ID") == TRANS_ID).collect()

if len(rows) == 0:
    raise ValueError(f"No row found in {COMPUTED} with TRANS_ID = '{TRANS_ID}'.")
if len(rows) > 1:
    raise ValueError(f"Found {len(rows)} rows with TRANS_ID = '{TRANS_ID}' — expected exactly 1.")

event = rows[0]
print(f"Found event for policy {event['AUTO_POLICY_ID']}, TRANS_ID={TRANS_ID}")

registry = spark.table(REGISTRY).collect()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Route it

# COMMAND ----------

_route_single_event(event, registry)
print("\nDone — single transaction routed. COMPUTED_FEATURE_EVENTS and 05's checkpoint were untouched.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

policy_id = event["AUTO_POLICY_ID"]

print("=== AUTO_FEATURES_ACTIVE (this policy) ===")
display(spark.table(ACTIVE).filter(F.col("AUTO_POLICY_ID") == policy_id))

print("=== AUTO_FEATURES_PENDING (this policy) ===")
display(spark.table(PENDING).filter(F.col("AUTO_POLICY_ID") == policy_id))

print("=== AUTO_FEATURES_HISTORY (this policy, current) ===")
display(spark.table(HISTORY).filter(
    (F.col("AUTO_POLICY_ID") == policy_id) & (F.col("IS_CURRENT") == True)
))
