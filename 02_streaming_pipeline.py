# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Streaming Pipeline (Registry-Driven Event Router)
# MAGIC
# MAGIC ## How the registry drives routing at runtime
# MAGIC
# MAGIC At the start of every micro-batch, the pipeline reads `feature_group_registry`
# MAGIC and builds two mappings:
# MAGIC
# MAGIC ```
# MAGIC event_type → [feature groups it triggers]
# MAGIC   "vehicle_event"  → ["vehicle"]
# MAGIC   "driver_event"   → ["driver"]
# MAGIC   "multiple_event" → ["vehicle", "driver"]
# MAGIC   "login_event"    → ["login"]
# MAGIC
# MAGIC feature group → columns it owns
# MAGIC   "vehicle" → [no_of_vehicles, newest_vehicle_age, ...]
# MAGIC   "driver"  → [no_of_drivers, avg_driver_age, ...]
# MAGIC   "login"   → [login_count_24h, login_count_7d, last_login_time]
# MAGIC ```
# MAGIC
# MAGIC **To add a new feature group** (e.g. "telematics"):
# MAGIC   - INSERT one row into `feature_group_registry`
# MAGIC   - No code change needed here
# MAGIC
# MAGIC **Routing rules (from registry)**
# MAGIC
# MAGIC | update_mechanism | What streaming pipeline does |
# MAGIC |---|---|
# MAGIC | `streaming_immediate_or_pending` | immediate → MERGE active; future-dated → pending |
# MAGIC | `hourly_batch` | append to raw log only, scheduled job handles the rest |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

dbutils.widgets.text("catalog",         "ws_prd_analytics")
dbutils.widgets.text("schema",          "default")
dbutils.widgets.text("source_path",     "/Volumes/ws_prd_analytics/default/landing/events")
dbutils.widgets.text("checkpoint_path", "/tmp/auto_insurance_features/streaming_checkpoint")

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
SOURCE_PATH     = dbutils.widgets.get("source_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")

ACTIVE       = f"{CATALOG}.{SCHEMA}.auto_features_active"
PENDING      = f"{CATALOG}.{SCHEMA}.auto_features_pending"
HISTORY      = f"{CATALOG}.{SCHEMA}.auto_features_history"
LOGIN_EVENTS = f"{CATALOG}.{SCHEMA}.auto_policy_login_events"
REGISTRY     = f"{CATALOG}.{SCHEMA}.feature_group_registry"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Event schema

# COMMAND ----------

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id",            T.StringType(),    False),
    T.StructField("event_type",          T.StringType(),    False),
    T.StructField("auto_policy_id",      T.StringType(),    False),
    T.StructField("policyholder_id",     T.StringType(),    True),
    T.StructField("effective_date",      T.DateType(),      True),
    T.StructField("no_of_vehicles",      T.IntegerType(),   True),
    T.StructField("newest_vehicle_age",  T.IntegerType(),   True),
    T.StructField("oldest_vehicle_age",  T.IntegerType(),   True),
    T.StructField("avg_vehicle_age",     T.DoubleType(),    True),
    T.StructField("no_of_drivers",       T.IntegerType(),   True),
    T.StructField("avg_driver_age",      T.DoubleType(),    True),
    T.StructField("youngest_driver_age", T.IntegerType(),   True),
    T.StructField("oldest_driver_age",   T.IntegerType(),   True),
    T.StructField("login_timestamp",     T.TimestampType(), True),
    T.StructField("event_source",        T.StringType(),    True),
    T.StructField("event_timestamp",     T.TimestampType(), True),
    T.StructField("event_received_at",   T.TimestampType(), True),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Registry loader — reads at runtime every micro-batch

# COMMAND ----------

def load_registry():
    """
    Reads feature_group_registry and returns two dicts:

    event_to_groups  : { "vehicle_event": ["vehicle"],
                         "multiple_event": ["vehicle", "driver"], ... }

    group_to_columns : { "vehicle": ["no_of_vehicles", ...],
                         "driver":  ["no_of_drivers", ...], ... }

    group_to_mechanism: { "vehicle": "streaming_immediate_or_pending",
                          "login":   "hourly_batch", ... }
    """
    rows = spark.table(REGISTRY).collect()

    event_to_groups   = {}
    group_to_columns  = {}
    group_to_mechanism = {}

    for row in rows:
        group     = row["feature_group"]
        mechanism = row["update_mechanism"]
        columns   = row["owned_columns"]
        events    = row["event_types"]

        group_to_columns[group]   = columns
        group_to_mechanism[group] = mechanism

        for event_type in events:
            event_to_groups.setdefault(event_type, []).append(group)

    return event_to_groups, group_to_columns, group_to_mechanism


# Quick sanity check at startup
_evt, _grp, _mech = load_registry()
print("Registry loaded at startup:")
for et, groups in _evt.items():
    print(f"  {et:25s} → {groups}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## History helpers

# COMMAND ----------

def _close_history(policy_ids_view: str):
    spark.sql(f"""
        MERGE INTO {HISTORY} AS h
        USING {policy_ids_view} AS p
        ON h.auto_policy_id = p.auto_policy_id AND h.is_current = true
        WHEN MATCHED THEN UPDATE SET
            h.is_current = false,
            h.valid_to   = current_timestamp()
    """)


def _snapshot_active_to_history(policy_ids_view: str, event_id_col: str,
                                 group: str, reason: str):
    spark.sql(f"""
        INSERT INTO {HISTORY}
        SELECT
            a.auto_policy_id, a.policyholder_id,
            a.no_of_vehicles, a.newest_vehicle_age, a.oldest_vehicle_age, a.avg_vehicle_age,
            a.no_of_drivers,  a.avg_driver_age, a.youngest_driver_age, a.oldest_driver_age,
            a.login_count_24h, a.login_count_7d, a.last_login_time,
            a.feature_updated_at, a.vehicle_features_updated_at,
            a.driver_features_updated_at, a.login_features_updated_at,
            a.last_source_event_id,
            current_timestamp() AS snapshot_at,
            '{reason}'          AS reason,
            {event_id_col}      AS event_id,
            '{group}'           AS changed_feature_group,
            current_timestamp() AS valid_from,
            NULL                AS valid_to,
            true                AS is_current
        FROM {ACTIVE} a
        INNER JOIN {policy_ids_view} p ON a.auto_policy_id = p.auto_policy_id
    """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Route 1 — Login (hourly_batch groups): append to raw log only

# COMMAND ----------

def append_to_login_events(df):
    df.createOrReplaceTempView("_login_batch")
    spark.sql(f"""
        MERGE INTO {LOGIN_EVENTS} AS t
        USING _login_batch AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, auto_policy_id, policyholder_id,
            login_timestamp, event_received_at
        ) VALUES (
            s.event_id, s.auto_policy_id, s.policyholder_id,
            s.login_timestamp,
            COALESCE(s.event_received_at, s.event_timestamp, current_timestamp())
        )
    """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Route 2 — Future-dated: insert into pending

# COMMAND ----------

def insert_to_pending(df, event_to_groups):
    """
    changed_feature_group is derived from the registry mapping, not hardcoded.
    e.g. if registry says multiple_event → [vehicle, driver], group = "multiple"
    """
    # Build a Spark map literal from registry: event_type → group label
    group_label_map = {}
    for event_type, groups in event_to_groups.items():
        if len(groups) == 1:
            group_label_map[event_type] = groups[0]
        else:
            group_label_map[event_type] = "multiple"

    mapping_expr = F.create_map(
        *[item for k, v in group_label_map.items()
          for item in (F.lit(k), F.lit(v))]
    )

    pending_df = df.withColumn(
        "changed_feature_group",
        mapping_expr[F.col("event_type")]
    ).withColumn(
        "event_received_at",
        F.coalesce(F.col("event_received_at"),
                   F.col("event_timestamp"),
                   F.current_timestamp())
    )

    pending_df.createOrReplaceTempView("_pending_batch")
    spark.sql(f"""
        MERGE INTO {PENDING} AS t
        USING _pending_batch AS s
        ON  t.auto_policy_id  = s.auto_policy_id
        AND t.effective_date  = s.effective_date
        AND t.source_event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT (
            auto_policy_id, policyholder_id,
            no_of_vehicles, newest_vehicle_age, oldest_vehicle_age, avg_vehicle_age,
            no_of_drivers,  avg_driver_age,     youngest_driver_age, oldest_driver_age,
            effective_date, changed_feature_group, source_event_id, event_received_at
        ) VALUES (
            s.auto_policy_id, s.policyholder_id,
            s.no_of_vehicles, s.newest_vehicle_age, s.oldest_vehicle_age, s.avg_vehicle_age,
            s.no_of_drivers,  s.avg_driver_age,     s.youngest_driver_age, s.oldest_driver_age,
            s.effective_date, s.changed_feature_group, s.event_id, s.event_received_at
        )
        WHEN MATCHED THEN UPDATE SET
            t.no_of_vehicles        = s.no_of_vehicles,
            t.newest_vehicle_age    = s.newest_vehicle_age,
            t.oldest_vehicle_age    = s.oldest_vehicle_age,
            t.avg_vehicle_age       = s.avg_vehicle_age,
            t.no_of_drivers         = s.no_of_drivers,
            t.avg_driver_age        = s.avg_driver_age,
            t.youngest_driver_age   = s.youngest_driver_age,
            t.oldest_driver_age     = s.oldest_driver_age,
            t.changed_feature_group = s.changed_feature_group,
            t.event_received_at     = s.event_received_at
    """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Route 3 — Immediate: registry-driven group pass
# MAGIC
# MAGIC For each streaming group in the registry, the pipeline:
# MAGIC 1. Filters events whose event_type touches that group (from registry)
# MAGIC 2. Deduplicates by policy (latest event_received_at)
# MAGIC 3. Builds MERGE SET clause dynamically from registry-owned columns
# MAGIC 4. Archive → MERGE → Snapshot
# MAGIC
# MAGIC Adding a new group = INSERT into registry only. No code change here.

# COMMAND ----------

def _dedup_latest_per_policy(df):
    return (
        df.withColumn("_rn", F.row_number().over(
            Window.partitionBy("auto_policy_id")
                  .orderBy(
                      F.coalesce(F.col("event_received_at"),
                                 F.col("event_timestamp")).desc()
                  )
        ))
        .filter("_rn = 1")
        .drop("_rn")
    )


def _process_group_pass(df, group_name, owned_columns, event_to_groups):
    """
    Generic group processor — works for ANY feature group registered in the registry.
    owned_columns comes directly from feature_group_registry.owned_columns.
    """
    # Find which event_types touch this group (from registry)
    triggering_event_types = [
        et for et, groups in event_to_groups.items()
        if group_name in groups
    ]

    group_df = df.filter(F.col("event_type").isin(triggering_event_types))
    if group_df.limit(1).count() == 0:
        return

    deduped = _dedup_latest_per_policy(group_df)
    view     = f"_batch_{group_name}"
    pol_view = f"_policies_{group_name}"
    deduped.createOrReplaceTempView(view)
    deduped.select("auto_policy_id").distinct().createOrReplaceTempView(pol_view)

    # Step 1 — close current history rows
    _close_history(pol_view)

    # Step 2 — build MERGE SET clause dynamically from registry columns
    metadata_ts_col = f"{group_name}_features_updated_at"
    set_clauses = [f"t.{col} = s.{col}" for col in owned_columns]
    set_clauses += [
        f"t.{metadata_ts_col}    = current_timestamp()",
        "t.feature_updated_at    = current_timestamp()",
        "t.last_source_event_id  = s.event_id",
    ]
    set_sql = ",\n            ".join(set_clauses)

    spark.sql(f"""
        MERGE INTO {ACTIVE} AS t
        USING {view} AS s
        ON t.auto_policy_id = s.auto_policy_id
        WHEN MATCHED THEN UPDATE SET
            {set_sql}
        WHEN NOT MATCHED THEN INSERT (
            auto_policy_id, policyholder_id,
            no_of_vehicles, newest_vehicle_age, oldest_vehicle_age, avg_vehicle_age,
            no_of_drivers,  avg_driver_age, youngest_driver_age, oldest_driver_age,
            login_count_24h, login_count_7d, last_login_time,
            feature_updated_at, vehicle_features_updated_at,
            driver_features_updated_at, login_features_updated_at,
            last_source_event_id
        ) VALUES (
            s.auto_policy_id, s.policyholder_id,
            s.no_of_vehicles, s.newest_vehicle_age, s.oldest_vehicle_age, s.avg_vehicle_age,
            s.no_of_drivers,  s.avg_driver_age, s.youngest_driver_age, s.oldest_driver_age,
            0, 0, NULL,
            current_timestamp(), current_timestamp(), current_timestamp(), NULL, s.event_id
        )
    """)

    # Step 3 — snapshot updated active to history
    _snapshot_active_to_history(pol_view, "s.event_id",
                                 group_name, f"immediate:{group_name}_event")
    print(f"  [{group_name}] pass done.")


def archive_and_merge_immediate(df, event_to_groups, group_to_columns, group_to_mechanism):
    """
    Iterates over every streaming group in the registry.
    Each group runs its own deduplicate → archive → MERGE → snapshot pass.
    Order: vehicle first, then driver (or whatever order registry rows are returned).
    """
    streaming_groups = [
        g for g, mech in group_to_mechanism.items()
        if mech == "streaming_immediate_or_pending"
    ]
    for group in streaming_groups:
        _process_group_pass(df, group, group_to_columns[group], event_to_groups)

# COMMAND ----------
# MAGIC %md
# MAGIC ## foreachBatch — registry read happens here every batch

# COMMAND ----------

def process_micro_batch(df, batch_id):
    # Read registry fresh each micro-batch — picks up any new groups automatically
    event_to_groups, group_to_columns, group_to_mechanism = load_registry()

    today = spark.sql("SELECT current_date()").first()[0]

    # Classify event types using registry
    batch_event_types   = [row["event_type"] for row in
                           df.select("event_type").distinct().collect()]

    hourly_batch_events    = [et for et in batch_event_types
                              if any(group_to_mechanism.get(g) == "hourly_batch"
                                     for g in event_to_groups.get(et, []))]
    streaming_event_types  = [et for et in batch_event_types
                              if any(group_to_mechanism.get(g) == "streaming_immediate_or_pending"
                                     for g in event_to_groups.get(et, []))]

    login_df     = df.filter(F.col("event_type").isin(hourly_batch_events)).cache() \
                   if hourly_batch_events else df.filter(F.lit(False)).cache()
    streaming_df = df.filter(F.col("event_type").isin(streaming_event_types)).cache() \
                   if streaming_event_types else df.filter(F.lit(False)).cache()

    future_df    = streaming_df.filter(F.col("effective_date") > F.lit(today)).cache()
    immediate_df = streaming_df.filter(F.col("effective_date") <= F.lit(today)).cache()

    if login_df.limit(1).count() > 0:
        append_to_login_events(login_df)

    if future_df.limit(1).count() > 0:
        insert_to_pending(future_df, event_to_groups)

    if immediate_df.limit(1).count() > 0:
        archive_and_merge_immediate(immediate_df, event_to_groups,
                                    group_to_columns, group_to_mechanism)

    login_df.unpersist()
    streaming_df.unpersist()
    future_df.unpersist()
    immediate_df.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Drop test events into the landing Volume
# MAGIC
# MAGIC Run this cell first to write sample events, then run the streaming cell below.

# COMMAND ----------

import json

dbutils.fs.mkdirs(SOURCE_PATH)

vehicle_event = {
    "event_id": "EVT001",
    "auto_policy_id": "POL001",
    "policyholder_id": "CUS001",
    "event_type": "vehicle_event",
    "no_of_vehicles": 3,
    "newest_vehicle_age": 1,
    "oldest_vehicle_age": 9,
    "avg_vehicle_age": 5.0,
    "no_of_drivers": None,
    "avg_driver_age": None,
    "youngest_driver_age": None,
    "oldest_driver_age": None,
    "effective_date": None,
    "login_timestamp": None,
    "event_received_at": "2026-06-12T10:00:00"
}

driver_event = {
    "event_id": "EVT003",
    "auto_policy_id": "POL002",
    "policyholder_id": "CUS002",
    "event_type": "driver_event",
    "no_of_vehicles": None,
    "newest_vehicle_age": None,
    "oldest_vehicle_age": None,
    "avg_vehicle_age": None,
    "no_of_drivers": 2,
    "avg_driver_age": 40.0,
    "youngest_driver_age": 30,
    "oldest_driver_age": 50,
    "effective_date": None,
    "login_timestamp": None,
    "event_received_at": "2026-06-12T10:01:00"
}

login_event = {
    "event_id": "EVT002",
    "auto_policy_id": "POL001",
    "policyholder_id": "CUS001",
    "event_type": "login_event",
    "no_of_vehicles": None,
    "newest_vehicle_age": None,
    "oldest_vehicle_age": None,
    "avg_vehicle_age": None,
    "no_of_drivers": None,
    "avg_driver_age": None,
    "youngest_driver_age": None,
    "oldest_driver_age": None,
    "effective_date": None,
    "login_timestamp": "2026-06-12T09:30:00",
    "event_received_at": "2026-06-12T09:30:00"
}

with open(f"{SOURCE_PATH}/vehicle_evt001.json", "w") as f:
    json.dump(vehicle_event, f)

with open(f"{SOURCE_PATH}/driver_evt003.json", "w") as f:
    json.dump(driver_event, f)

with open(f"{SOURCE_PATH}/login_evt002.json", "w") as f:
    json.dump(login_event, f)

print(f"3 test events written to {SOURCE_PATH}")
print("  EVT001 — vehicle_event  for POL001")
print("  EVT003 — driver_event   for POL002")
print("  EVT002 — login_event    for POL001")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start streaming

# COMMAND ----------

query = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.inferColumnTypes", "false")
         .schema(EVENT_SCHEMA)
         .load(SOURCE_PATH)
         .writeStream
         .foreachBatch(process_micro_batch)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(availableNow=True)
         .start()
)

query.awaitTermination()
