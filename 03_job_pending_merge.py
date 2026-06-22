# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Job 1: Daily Pending Merge (runs at midnight)
# MAGIC
# MAGIC Applies pending vehicle/driver changes whose `effective_date` has arrived.
# MAGIC
# MAGIC **Update order for each due policy**
# MAGIC 1. Close existing `is_current = true` history row
# MAGIC 2. Partial MERGE into `auto_features_active` (only columns for `changed_feature_group`)
# MAGIC 3. Insert updated active snapshot into `auto_features_history` as new `is_current = true`
# MAGIC 4. DELETE processed rows from `auto_features_pending`
# MAGIC
# MAGIC **Partial MERGE logic**
# MAGIC - `changed_feature_group = "vehicle"` → update only vehicle columns
# MAGIC - `changed_feature_group = "driver"`  → update only driver columns
# MAGIC
# MAGIC Pending rows with `effective_date > today` are left untouched.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog",          "main")
dbutils.widgets.text("schema",           "auto_insurance_features_new")
dbutils.widgets.text("run_date",         "")        # e.g. "2026-06-25" — leave blank in production
dbutils.widgets.text("sync_pipeline_id", "")        # Catalog → auto_features_active_online → Data Ingest → Pipeline id

CATALOG          = dbutils.widgets.get("catalog")
SCHEMA           = dbutils.widgets.get("schema")
SYNC_PIPELINE_ID = dbutils.widgets.get("sync_pipeline_id")
_date_input      = dbutils.widgets.get("run_date").strip()

HOST  = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Production: leave run_date blank → uses current_date().
# Testing: set run_date = "2026-06-25" to simulate a future effective date.
today = spark.sql("SELECT current_date()").first()[0] if not _date_input else _date_input

print(f"Run date         : {today}")
print(f"Sync pipeline ID : {SYNC_PIPELINE_ID or '(not set)'}")

ACTIVE  = f"{CATALOG}.{SCHEMA}.auto_features_active"
PENDING = f"{CATALOG}.{SCHEMA}.auto_features_pending"
HISTORY = f"{CATALOG}.{SCHEMA}.auto_features_history"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Apply due pending changes

# COMMAND ----------

def apply_due_pending_changes():
    print(f"Run date: {today}")

    due_df = spark.sql(f"""
        SELECT * FROM {PENDING}
        WHERE effective_date <= '{today}'
    """)

    if due_df.limit(1).count() == 0:
        print(f"No pending changes due on or before {today}. Nothing to do.")
        return

    # If the same policy has multiple pending rows due today (e.g., two vehicle events
    # for different effective_dates both <= today), pick the latest one per policy
    # per changed_feature_group so we apply the most recent intent.
    deduped = (
        due_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("auto_policy_id", "changed_feature_group")
                  .orderBy(F.col("effective_date").desc(),
                           F.col("event_received_at").desc())
        ))
        .filter("_rn = 1")
        .drop("_rn")
    )

    count = deduped.count()
    print(f"Applying pending changes for {count} policy-group pair(s) with effective_date <= {today}.")

    deduped.createOrReplaceTempView("_due_pending")
    deduped.select("auto_policy_id").distinct().createOrReplaceTempView("_due_policies")

    # ------------------------------------------------------------------ #
    # Step 1 — Close existing is_current = true history rows              #
    # ------------------------------------------------------------------ #
    spark.sql(f"""
        MERGE INTO {HISTORY} AS h
        USING _due_policies AS p
        ON h.auto_policy_id = p.auto_policy_id AND h.is_current = true
        WHEN MATCHED THEN UPDATE SET
            h.is_current = false,
            h.valid_to   = current_timestamp()
    """)

    # ------------------------------------------------------------------ #
    # Step 2 — Partial MERGE into active                                  #
    # CASE WHEN on changed_feature_group ensures only the right columns   #
    # are touched; all others keep their current active value.            #
    # ------------------------------------------------------------------ #
    spark.sql(f"""
        MERGE INTO {ACTIVE} AS t
        USING _due_pending AS s
        ON t.auto_policy_id = s.auto_policy_id
        WHEN MATCHED THEN UPDATE SET
            -- vehicle columns — COALESCE keeps existing value if this column
            -- wasn't actually part of the change (null in the pending row)
            t.no_of_vehicles      = CASE WHEN s.changed_feature_group = 'vehicle'
                                         THEN COALESCE(s.no_of_vehicles, t.no_of_vehicles)
                                         ELSE t.no_of_vehicles      END,
            t.newest_vehicle_age  = CASE WHEN s.changed_feature_group = 'vehicle'
                                         THEN COALESCE(s.newest_vehicle_age, t.newest_vehicle_age)
                                         ELSE t.newest_vehicle_age  END,
            t.oldest_vehicle_age  = CASE WHEN s.changed_feature_group = 'vehicle'
                                         THEN COALESCE(s.oldest_vehicle_age, t.oldest_vehicle_age)
                                         ELSE t.oldest_vehicle_age  END,
            t.avg_vehicle_age     = CASE WHEN s.changed_feature_group = 'vehicle'
                                         THEN COALESCE(s.avg_vehicle_age, t.avg_vehicle_age)
                                         ELSE t.avg_vehicle_age     END,
            -- driver columns
            t.no_of_drivers       = CASE WHEN s.changed_feature_group = 'driver'
                                         THEN COALESCE(s.no_of_drivers, t.no_of_drivers)
                                         ELSE t.no_of_drivers       END,
            t.avg_driver_age      = CASE WHEN s.changed_feature_group = 'driver'
                                         THEN COALESCE(s.avg_driver_age, t.avg_driver_age)
                                         ELSE t.avg_driver_age      END,
            t.youngest_driver_age = CASE WHEN s.changed_feature_group = 'driver'
                                         THEN COALESCE(s.youngest_driver_age, t.youngest_driver_age)
                                         ELSE t.youngest_driver_age END,
            t.oldest_driver_age   = CASE WHEN s.changed_feature_group = 'driver'
                                         THEN COALESCE(s.oldest_driver_age, t.oldest_driver_age)
                                         ELSE t.oldest_driver_age   END,
            -- metadata
            t.vehicle_features_updated_at = CASE WHEN s.changed_feature_group = 'vehicle'
                                                  THEN current_timestamp()
                                                  ELSE t.vehicle_features_updated_at END,
            t.driver_features_updated_at  = CASE WHEN s.changed_feature_group = 'driver'
                                                  THEN current_timestamp()
                                                  ELSE t.driver_features_updated_at  END,
            t.feature_updated_at   = current_timestamp(),
            t.last_source_event_id = s.source_event_id
    """)

    # ------------------------------------------------------------------ #
    # Step 3 — Snapshot updated active values into history                #
    # ------------------------------------------------------------------ #
    spark.sql(f"""
        INSERT INTO {HISTORY}
        SELECT
            a.auto_policy_id,
            a.policyholder_id,
            a.no_of_vehicles,
            a.newest_vehicle_age,
            a.oldest_vehicle_age,
            a.avg_vehicle_age,
            a.no_of_drivers,
            a.avg_driver_age,
            a.youngest_driver_age,
            a.oldest_driver_age,
            a.login_count_24h,
            a.login_count_7d,
            a.last_login_time,
            a.feature_updated_at,
            a.vehicle_features_updated_at,
            a.driver_features_updated_at,
            a.login_features_updated_at,
            a.last_source_event_id,
            current_timestamp()                                     AS snapshot_at,
            CONCAT('pending_applied:', p.changed_feature_group)     AS reason,
            p.source_event_id                                       AS event_id,
            p.changed_feature_group                                 AS changed_feature_group,
            current_timestamp()                                     AS valid_from,
            NULL                                                    AS valid_to,
            true                                                    AS is_current
        FROM {ACTIVE} a
        INNER JOIN _due_pending p ON a.auto_policy_id = p.auto_policy_id
    """)

    # ------------------------------------------------------------------ #
    # Step 4 — Delete processed pending rows                              #
    # Only rows with effective_date <= today are removed; future rows     #
    # for the same policy stay pending.                                   #
    # ------------------------------------------------------------------ #
    spark.sql(f"""
        DELETE FROM {PENDING}
        WHERE effective_date <= '{today}'
    """)

    print(f"Done. Processed {count} policy-group pair(s). Pending rows deleted.")


import requests as _http

def trigger_online_sync():
    """Wakes up the Lakebase DLT sync pipeline after pending changes are merged into auto_features_active."""
    if not SYNC_PIPELINE_ID:
        print("  [online sync] sync_pipeline_id not set — skipping (set widget to enable)")
        return
    _headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    resp = _http.post(
        f"https://{HOST}/api/2.0/pipelines/{SYNC_PIPELINE_ID}/updates",
        headers=_headers,
        json={"cause": "USER_ACTION", "full_refresh": False},
        timeout=15
    )
    if resp.status_code in (200, 202):
        print(f"  [online sync] sync triggered (pipeline={SYNC_PIPELINE_ID[:8]}...)")
    else:
        print(f"  [online sync] {resp.status_code}: {resp.text[:150]}")


apply_due_pending_changes()
trigger_online_sync()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=== auto_features_active (after pending merge) ===")
display(spark.table(ACTIVE).orderBy("auto_policy_id"))

print("=== auto_features_pending (remaining future rows) ===")
display(spark.table(PENDING).orderBy("effective_date"))

print("=== auto_features_history (latest snapshots) ===")
display(
    spark.table(HISTORY)
         .orderBy("auto_policy_id", "valid_from")
)
