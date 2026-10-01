# Databricks notebook source
# MAGIC %md
# MAGIC # 05 (Timed) — Routing Pipeline with Step-by-Step Timing
# MAGIC
# MAGIC Same routing logic as `05_routing_pipeline` (same registry-driven decisions, same SQL), but
# MAGIC every step is timed so you can see **exactly where the time goes**.
# MAGIC
# MAGIC Use it **instead of** `05` for a run (same checkpoint widget → it continues where `05` left off,
# MAGIC and `05` can continue after it). Do not run both against the same checkpoint at the same time.
# MAGIC
# MAGIC ## What gets timed
# MAGIC | Level | Step | What it is |
# MAGIC |---|---|---|
# MAGIC | Stream | `stream_start` | Creating the CDF stream until it is running |
# MAGIC | Stream | `stream_total` | Whole stream from start to `awaitTermination()` returning |
# MAGIC | Batch | `B1_filter_count` | Filter CDF batch to inserts + count |
# MAGIC | Batch | `B2_read_registry` | Read `FEATURE_GROUP_REGISTRY` |
# MAGIC | Batch | `B3_collect_events` | Sort + collect events to the driver |
# MAGIC | Event | `E1_close_history` | `UPDATE HISTORY` — close old IS_CURRENT row |
# MAGIC | Event | `E2_merge_active:<GROUP>` | `MERGE ACTIVE` — once per immediate feature group |
# MAGIC | Event | `E3_snapshot_history` | `INSERT HISTORY` — new IS_CURRENT row |
# MAGIC | Event | `E4_merge_pending:<GROUP>` | `MERGE PENDING` — future-dated groups |
# MAGIC
# MAGIC Timings are written to `ROUTING_STEP_TIMING` (one write per batch, not per statement, so
# MAGIC logging itself adds almost no time). The **Timing Report** cells at the end read it back —
# MAGIC they work even if `print` inside `foreachBatch` is not shown on Serverless.

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

dbutils.widgets.text("catalog",         "ws_prd_analytics")
dbutils.widgets.text("schema",          "featurestore_test")
dbutils.widgets.text("checkpoint_path", "/Volumes/ws_prd_analytics/featurestore_test/checkpoints/routing")
dbutils.widgets.text("sim_date",        "")   # YYYY-MM-DD HH:MM:SS or blank for real time
dbutils.widgets.text("progress_every",  "100")  # print a progress line every N events

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
PROGRESS_EVERY  = int(dbutils.widgets.get("progress_every") or 100)
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
TIMING   = f"{CATALOG}.{SCHEMA}.ROUTING_STEP_TIMING"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Run id       : {RUN_ID}")
print(f"Checkpoint   : {CHECKPOINT_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 0 — Table sizes before the run
# MAGIC
# MAGIC `UPDATE` / `MERGE` cost grows with table size and **number of files** — compare these
# MAGIC between the slow and the fast workspace.

# COMMAND ----------

def table_stats(tbl):
    d = spark.sql(f"DESCRIBE DETAIL {tbl}").first()
    return {"table": tbl.split(".")[-1], "rows": spark.table(tbl).count(),
            "num_files": d["numFiles"], "size_mb": round((d["sizeInBytes"] or 0) / 1024 / 1024, 2)}


_t = time.time()
BEFORE = [table_stats(t) for t in (COMPUTED, ACTIVE, PENDING, HISTORY)]
for s in BEFORE:
    print(f"  {s['table']:28s} rows={s['rows']:>8}  files={s['num_files']:>6}  size={s['size_mb']:>8} MB")
print(f"(stats took {time.time() - _t:.1f}s)")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TIMING} (
    RUN_ID      STRING,
    BATCH_ID    LONG,
    TRANS_ID    STRING,
    STEP        STRING,
    SECONDS     DOUBLE,
    LOGGED_AT   TIMESTAMP
) USING DELTA
""")

TIMING_SCHEMA = StructType([
    StructField("RUN_ID",    StringType()),
    StructField("BATCH_ID",  LongType()),
    StructField("TRANS_ID",  StringType()),
    StructField("STEP",      StringType()),
    StructField("SECONDS",   DoubleType()),
    StructField("LOGGED_AT", TimestampType()),
])


def write_timings(rows):
    if rows:
        spark.createDataFrame(rows, schema=TIMING_SCHEMA).write.format("delta").mode("append").saveAsTable(TIMING)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers (same SQL as 05)

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
# MAGIC ## Route a Single Event — timed

# COMMAND ----------

def _route_single_event(event, registry, batch_id, timings):
    policy   = event["AUTO_POLICY_ID"]
    trans_id = event["TRANS_ID"]

    def timed(step, fn):
        t0 = time.time()
        fn()
        timings.append((RUN_ID, batch_id, trans_id, step, round(time.time() - t0, 3), datetime.now()))

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
        else:
            pending_groups.append((feature_group, group_eff_date, owned_columns))

    if immediate_groups:
        earliest_eff   = min(d for _, d, _ in immediate_groups)
        changed_groups = ",".join(g for g, _, _ in immediate_groups)

        timed("E1_close_history", lambda: spark.sql(f"""
            UPDATE {HISTORY}
            SET    IS_CURRENT           = false,
                   VALID_TRANSACTION_TO = TIMESTAMP '{earliest_eff}' - INTERVAL 1 MILLISECOND
            WHERE  AUTO_POLICY_ID = '{policy}'
              AND  IS_CURRENT     = true
        """))

        for feature_group, _, owned_columns in immediate_groups:
            timed(f"E2_merge_active:{feature_group}",
                  lambda fg=feature_group, oc=owned_columns: _merge_group_to_active(policy, trans_id, event, fg, oc))

        timed("E3_snapshot_history", lambda: spark.sql(f"""
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
        """))

    for feature_group, group_eff_date, owned_columns in pending_groups:
        timed(f"E4_merge_pending:{feature_group}",
              lambda fg=feature_group, oc=owned_columns, d=group_eff_date:
                  _merge_group_to_pending(policy, trans_id, event, fg, oc, d))

# COMMAND ----------
# MAGIC %md
# MAGIC ## foreachBatch — timed

# COMMAND ----------

def process_micro_batch(df, batch_id):
    import traceback
    timings = []
    batch_t0 = time.time()

    def btimed(step, fn):
        t0 = time.time()
        out = fn()
        timings.append((RUN_ID, batch_id, None, step, round(time.time() - t0, 3), datetime.now()))
        return out

    try:
        df    = df.filter(F.col("_change_type") == "insert")
        count = btimed("B1_filter_count", lambda: df.count())
        print(f"\n[batch {batch_id}] {count} computed event(s)")
        if count == 0:
            write_timings(timings)
            return

        registry = btimed("B2_read_registry", lambda: spark.table(REGISTRY).collect())
        events   = btimed("B3_collect_events", lambda: df.orderBy("SRC_TRANS_TMSP", "TRANS_ID").collect())

        for i, event in enumerate(events, 1):
            _route_single_event(event, registry, batch_id, timings)
            if i % PROGRESS_EVERY == 0 or i == len(events):
                elapsed = time.time() - batch_t0
                print(f"  [batch {batch_id}] {i}/{len(events)} events | {elapsed:.0f}s elapsed | "
                      f"{elapsed / i:.2f}s per event")

        timings.append((RUN_ID, batch_id, None, "B_total", round(time.time() - batch_t0, 3), datetime.now()))
        write_timings(timings)
        print(f"[batch {batch_id}] done in {time.time() - batch_t0:.1f}s")

    except Exception as e:
        write_timings(timings)   # keep whatever was measured before the failure
        print(f"[ERROR] batch {batch_id}: {e}")
        traceback.print_exc()
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start Streaming from COMPUTED_FEATURE_EVENTS via CDF — timed

# COMMAND ----------

stream_t0 = time.time()

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
STREAM_START_SECONDS = round(time.time() - stream_t0, 3)
print(f"Stream started in {STREAM_START_SECONDS}s")

query.awaitTermination()
STREAM_TOTAL_SECONDS = round(time.time() - stream_t0, 3)

write_timings([
    (RUN_ID, None, None, "stream_start", STREAM_START_SECONDS, datetime.now()),
    (RUN_ID, None, None, "stream_total", STREAM_TOTAL_SECONDS, datetime.now()),
])
print(f"\nRouting pipeline finished in {STREAM_TOTAL_SECONDS:.1f}s ({STREAM_TOTAL_SECONDS / 60:.1f} min).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Timing Report 1 — Where did the time go? (per step)
# MAGIC
# MAGIC `pct_of_run` = share of the whole stream time. The step with the largest `total_sec` is the bottleneck.

# COMMAND ----------

display(spark.sql(f"""
    WITH t AS (SELECT * FROM {TIMING} WHERE RUN_ID = '{RUN_ID}')
    SELECT
        STEP,
        COUNT(*)                                   AS calls,
        ROUND(SUM(SECONDS), 1)                     AS total_sec,
        ROUND(AVG(SECONDS), 3)                     AS avg_sec,
        ROUND(PERCENTILE(SECONDS, 0.5), 3)         AS median_sec,
        ROUND(MAX(SECONDS), 3)                     AS max_sec,
        ROUND(100 * SUM(SECONDS) /
              (SELECT MAX(SECONDS) FROM t WHERE STEP = 'stream_total'), 1) AS pct_of_run
    FROM t
    WHERE STEP NOT IN ('stream_total', 'B_total')
    GROUP BY STEP
    ORDER BY total_sec DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Timing Report 2 — Overhead outside the measured SQL
# MAGIC
# MAGIC If `unmeasured_sec` is large, the time is going to streaming/Serverless overhead
# MAGIC (stream startup, batch planning, waiting for compute), not to the SQL statements themselves.

# COMMAND ----------

display(spark.sql(f"""
    WITH t AS (SELECT * FROM {TIMING} WHERE RUN_ID = '{RUN_ID}')
    SELECT
        ROUND((SELECT MAX(SECONDS) FROM t WHERE STEP = 'stream_total'), 1)                        AS stream_total_sec,
        ROUND((SELECT MAX(SECONDS) FROM t WHERE STEP = 'stream_start'), 1)                        AS stream_start_sec,
        ROUND((SELECT SUM(SECONDS) FROM t WHERE STEP = 'B_total'), 1)                             AS inside_batches_sec,
        ROUND((SELECT SUM(SECONDS) FROM t WHERE STEP LIKE 'E%'), 1)                               AS event_sql_sec,
        ROUND((SELECT MAX(SECONDS) FROM t WHERE STEP = 'stream_total')
            - COALESCE((SELECT SUM(SECONDS) FROM t WHERE STEP = 'B_total'), 0), 1)                AS unmeasured_sec,
        (SELECT COUNT(DISTINCT TRANS_ID) FROM t WHERE TRANS_ID IS NOT NULL)                       AS events_routed,
        ROUND((SELECT SUM(SECONDS) FROM t WHERE STEP LIKE 'E%')
            / NULLIF((SELECT COUNT(DISTINCT TRANS_ID) FROM t WHERE TRANS_ID IS NOT NULL), 0), 3)   AS sql_sec_per_event
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Timing Report 3 — Does it slow down as the tables grow?
# MAGIC
# MAGIC Average seconds per step for each block of 500 events. If the numbers climb from block to block,
# MAGIC the tables (or their small files) are growing and making each statement slower → run `OPTIMIZE`.

# COMMAND ----------

display(spark.sql(f"""
    WITH ev AS (
        SELECT TRANS_ID, MIN(LOGGED_AT) AS first_at
        FROM {TIMING}
        WHERE RUN_ID = '{RUN_ID}' AND TRANS_ID IS NOT NULL
        GROUP BY TRANS_ID
    ),
    numbered AS (
        SELECT TRANS_ID, FLOOR((ROW_NUMBER() OVER (ORDER BY first_at) - 1) / 500) AS block
        FROM ev
    )
    SELECT
        CONCAT(CAST(block * 500 + 1 AS STRING), '-', CAST(block * 500 + 500 AS STRING)) AS events,
        ROUND(AVG(CASE WHEN t.STEP = 'E1_close_history'    THEN t.SECONDS END), 3) AS E1_close_history,
        ROUND(AVG(CASE WHEN t.STEP LIKE 'E2_merge_active%' THEN t.SECONDS END), 3) AS E2_merge_active,
        ROUND(AVG(CASE WHEN t.STEP = 'E3_snapshot_history' THEN t.SECONDS END), 3) AS E3_snapshot_history,
        ROUND(AVG(CASE WHEN t.STEP LIKE 'E4_merge_pending%' THEN t.SECONDS END), 3) AS E4_merge_pending
    FROM numbered n
    JOIN {TIMING} t ON t.TRANS_ID = n.TRANS_ID AND t.RUN_ID = '{RUN_ID}'
    GROUP BY block
    ORDER BY block
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Timing Report 4 — Slowest 20 statements

# COMMAND ----------

display(spark.sql(f"""
    SELECT BATCH_ID, TRANS_ID, STEP, SECONDS, LOGGED_AT
    FROM {TIMING}
    WHERE RUN_ID = '{RUN_ID}' AND TRANS_ID IS NOT NULL
    ORDER BY SECONDS DESC
    LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table sizes after the run

# COMMAND ----------

AFTER = [table_stats(t) for t in (COMPUTED, ACTIVE, PENDING, HISTORY)]
print(f"  {'table':28s} {'rows before':>12} {'rows after':>11} {'files before':>13} {'files after':>12}")
for b, a in zip(BEFORE, AFTER):
    print(f"  {a['table']:28s} {b['rows']:>12} {a['rows']:>11} {b['num_files']:>13} {a['num_files']:>12}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## How to read the result
# MAGIC
# MAGIC | What you see | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `unmeasured_sec` is most of `stream_total_sec` | Streaming / Serverless overhead (startup, waiting for compute) | Not the SQL — compare cluster/warehouse size between workspaces |
# MAGIC | `E1_close_history` is the biggest step | `UPDATE` scans the whole HISTORY table for every event | `OPTIMIZE ... ZORDER BY (AUTO_POLICY_ID)` on HISTORY, or batch rewrite of 05 |
# MAGIC | `E2_merge_active` is the biggest | MERGE scans ACTIVE per event | `OPTIMIZE` ACTIVE; batch rewrite of 05 |
# MAGIC | Report 3 numbers climb block by block | Tables / small files growing during the run | `OPTIMIZE` HISTORY + ACTIVE before the run |
# MAGIC | Every step is slow from the start (high `median_sec`) | Each `spark.sql` call itself is slow in this workspace (overhead per statement) | Compare with the fast workspace; batch rewrite of 05 removes ~40K statements |
# MAGIC | `num_files` is in the thousands | Many small files from per-event writes | `OPTIMIZE`; enable `delta.autoOptimize.optimizeWrite` / `autoCompact` |
