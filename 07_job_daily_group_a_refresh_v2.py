# Databricks notebook source
# MAGIC %md
# MAGIC # 07 (v2) — Job: Daily GROUP_A Refresh, using AUTO_POLICY_SNAPSHOT_STATE
# MAGIC
# MAGIC Same job and same output as `07_job_daily_group_a_refresh`: recompute the 5 GROUP_A features
# MAGIC (`TXN_CNT_1D/1W/1M`, `OUTSTANDING_TXN_IND`, `TENURE_YRS`) for every policy as of `run_date`,
# MAGIC update `AUTO_FEATURES_ACTIVE` where they changed, and add a `group_a_daily_refresh` row to
# MAGIC `AUTO_FEATURES_HISTORY`.
# MAGIC
# MAGIC **What's different:** it no longer scans all of `RAW_POLICY_EVENTS`.
# MAGIC
# MAGIC | Feature | v1 (original 07) | v2 (this notebook) |
# MAGIC |---|---|---|
# MAGIC | `TXN_CNT_1D/1W/1M` | Every transaction of every policy in `RAW_POLICY_EVENTS` | `STATE.RECENT_TXN_TMSPS` — the last 35 days, already kept by `04` |
# MAGIC | `TENURE_YRS` | `MIN(EFF_DT)` over `RAW_POLICY_EVENTS` | `STATE.POLICY_INCEPTION_EFF_DT` |
# MAGIC | `OUTSTANDING_TXN_IND` | Latest transaction's `EFF_DT`, for every policy | Same rule, but only checked for policies whose flag is **1** in ACTIVE (time moving forward can only turn 1 → 0) — reads 3 small columns of RAW for those policies only |
# MAGIC
# MAGIC ## Requirements
# MAGIC - The optimized design (`AUTO_POLICY_SNAPSHOT_STATE` exists and is maintained by `04`).
# MAGIC - Run **after** the day's `04` has finished, and **not at the same time** as `04` — the counts come from
# MAGIC   the state `04` writes.
# MAGIC - `run_date` must be **today or later** than the last `04` run. `04` prunes `RECENT_TXN_TMSPS` to 35 days
# MAGIC   before *its* run, so simulating a date in the past can undercount, and the 1 → 0 shortcut for
# MAGIC   `OUTSTANDING_TXN_IND` assumes time only moves forward.
# MAGIC
# MAGIC Leave `run_date` blank in production (uses current_timestamp).

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("catalog",  "ws_prd_analytics")
dbutils.widgets.text("schema",   "featurestore_test")
dbutils.widgets.text("run_date", "")  # format: YYYY-MM-DD HH:MM:SS

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
_date_input = dbutils.widgets.get("run_date").strip()

if _date_input:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y"):
        try:
            run_ts = datetime.strptime(_date_input, fmt).strftime("%Y-%m-%d %H:%M:%S")
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse run_date '{_date_input}'. Use YYYY-MM-DD HH:MM:SS.")
else:
    run_ts = str(spark.sql("SELECT current_timestamp()").first()[0])

print(f"Run timestamp: {run_ts}")

RAW_EVENTS = f"{CATALOG}.{SCHEMA}.RAW_POLICY_EVENTS"
STATE      = f"{CATALOG}.{SCHEMA}.AUTO_POLICY_SNAPSHOT_STATE"
ACTIVE     = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_ACTIVE"
HISTORY    = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_HISTORY"

GROUP_A_COLUMNS = ["TXN_CNT_1D", "TXN_CNT_1W", "TXN_CNT_1M", "OUTSTANDING_TXN_IND", "TENURE_YRS"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Recompute GROUP_A for every policy as of `run_ts` — from the state table

# COMMAND ----------

def refresh_group_a():
    run_date = F.lit(run_ts).cast("date")

    current = spark.table(ACTIVE).select("AUTO_POLICY_ID", *GROUP_A_COLUMNS)
    state   = spark.table(STATE).select("AUTO_POLICY_ID", "RECENT_TXN_TMSPS", "POLICY_INCEPTION_EFF_DT")

    # ── Transaction counts: explode each policy's last-35-days list and count per window ──
    counts = (
        state.select("AUTO_POLICY_ID", F.explode_outer("RECENT_TXN_TMSPS").alias("TMSP"))
        .groupBy("AUTO_POLICY_ID")
        .agg(
            F.sum(F.when(F.col("TMSP") >= F.expr(f"TIMESTAMP '{run_ts}' - INTERVAL 1 DAY"), 1)
                   .otherwise(0)).alias("_cnt_1d"),
            F.sum(F.when(F.col("TMSP") >= F.expr(f"TIMESTAMP '{run_ts}' - INTERVAL 7 DAYS"), 1)
                   .otherwise(0)).alias("_cnt_1w"),
            F.sum(F.when(F.col("TMSP") >= F.expr(f"TIMESTAMP '{run_ts}' - INTERVAL 1 MONTH"), 1)
                   .otherwise(0)).alias("_cnt_1m"),
        )
    )

    # ── Outstanding flag: only policies currently flagged 1 can change (1 → 0 once EFF_DT passes) ──
    # Reads only 3 small columns of RAW (not RAW_PAYLOAD) and only for those policies.
    flagged = current.filter(F.col("OUTSTANDING_TXN_IND") == 1).select("AUTO_POLICY_ID")
    outstanding = (
        spark.table(RAW_EVENTS).select("AUTO_POLICY_ID", "SRC_TRANS_TMSP", "EFF_DT")
        .join(flagged, "AUTO_POLICY_ID", "inner")
        .groupBy("AUTO_POLICY_ID")
        .agg(F.max(F.struct("SRC_TRANS_TMSP", "EFF_DT")).alias("_latest"))
        .select("AUTO_POLICY_ID",
                (F.col("_latest.EFF_DT") > run_date).cast("int").alias("_outstanding"))
    )

    # ── Fresh GROUP_A values. A policy with no state row keeps its current values. ──
    fresh = (
        current.alias("c")
        .join(state.alias("s"), "AUTO_POLICY_ID", "left")
        .join(counts, "AUTO_POLICY_ID", "left")
        .join(outstanding, "AUTO_POLICY_ID", "left")
        .select(
            "AUTO_POLICY_ID",
            F.coalesce(F.col("_cnt_1d"), F.col("c.TXN_CNT_1D")).cast("int").alias("TXN_CNT_1D"),
            F.coalesce(F.col("_cnt_1w"), F.col("c.TXN_CNT_1W")).cast("int").alias("TXN_CNT_1W"),
            F.coalesce(F.col("_cnt_1m"), F.col("c.TXN_CNT_1M")).cast("int").alias("TXN_CNT_1M"),
            F.coalesce(F.col("_outstanding"), F.col("c.OUTSTANDING_TXN_IND")).cast("int").alias("OUTSTANDING_TXN_IND"),
            F.coalesce(
                F.floor(F.months_between(run_date, F.col("s.POLICY_INCEPTION_EFF_DT")) / 12),
                F.col("c.TENURE_YRS")).cast("int").alias("TENURE_YRS"),
        )
    )

    # ── Keep only policies where at least one GROUP_A value changed ──
    diff_expr = None
    for c in GROUP_A_COLUMNS:
        cond = ~F.col(f"f.{c}").eqNullSafe(F.col(f"c.{c}"))
        diff_expr = cond if diff_expr is None else (diff_expr | cond)

    changed = (
        fresh.alias("f")
        .join(current.alias("c"), "AUTO_POLICY_ID", "inner")
        .where(diff_expr)
        .select("f.*")
    )

    changed_count = changed.count()
    print(f"Policies with GROUP_A drift as of {run_ts}: {changed_count}")

    if changed_count == 0:
        print("Nothing to refresh.")
        return

    changed.createOrReplaceTempView("_group_a_refresh")

    # Step 1 — MERGE fresh GROUP_A values into ACTIVE
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in GROUP_A_COLUMNS)
    spark.sql(f"""
        MERGE INTO {ACTIVE} AS t
        USING _group_a_refresh AS s
        ON t.AUTO_POLICY_ID = s.AUTO_POLICY_ID
        WHEN MATCHED THEN UPDATE SET
            {set_clause},
            t.GROUP_A_UPDATED_AT = TIMESTAMP '{run_ts}',
            t.FEATURE_UPDATED_AT = TIMESTAMP '{run_ts}'
    """)
    print(f"  Step 1: Merged GROUP_A for {changed_count} polic(y/ies) into AUTO_FEATURES_ACTIVE.")

    # Step 2 — Close existing IS_CURRENT history rows for changed policies
    spark.sql(f"""
        UPDATE {HISTORY}
        SET    IS_CURRENT           = false,
               VALID_TRANSACTION_TO = TIMESTAMP '{run_ts}' - INTERVAL 1 MILLISECOND
        WHERE  AUTO_POLICY_ID IN (SELECT AUTO_POLICY_ID FROM _group_a_refresh)
          AND  IS_CURRENT = true
    """)
    print("  Step 2: Closed IS_CURRENT history rows for changed policies.")

    # Step 3 — Snapshot updated ACTIVE → HISTORY (one IS_CURRENT=true row per changed policy)
    spark.sql(f"""
        INSERT INTO {HISTORY}
        SELECT
            a.*,
            TIMESTAMP '{run_ts}'    AS SNAPSHOT_AT,
            'group_a_daily_refresh' AS REASON,
            NULL                    AS EVENT_ID,
            'GROUP_A'               AS CHANGED_FEATURE_GROUP,
            TIMESTAMP '{run_ts}'    AS VALID_TRANSACTION_FROM,
            NULL                    AS VALID_TRANSACTION_TO,
            true                    AS IS_CURRENT
        FROM {ACTIVE} a
        WHERE a.AUTO_POLICY_ID IN (SELECT AUTO_POLICY_ID FROM _group_a_refresh)
    """)
    print(f"  Step 3: Snapshotted {changed_count} polic(y/ies) to AUTO_FEATURES_HISTORY (IS_CURRENT=true).")
    print(f"\nDone. {changed_count} polic(y/ies) refreshed.")


refresh_group_a()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=== AUTO_FEATURES_ACTIVE (GROUP_A columns) ===")
display(spark.table(ACTIVE).select("AUTO_POLICY_ID", *GROUP_A_COLUMNS, "GROUP_A_UPDATED_AT").orderBy("AUTO_POLICY_ID"))

print("=== AUTO_FEATURES_HISTORY (group_a_daily_refresh rows) ===")
display(spark.sql(f"""
    SELECT * FROM {HISTORY}
    WHERE REASON = 'group_a_daily_refresh'
    ORDER BY AUTO_POLICY_ID, SNAPSHOT_AT DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optional — compare with the original full-RAW calculation
# MAGIC
# MAGIC Recomputes GROUP_A the original way (full `RAW_POLICY_EVENTS` scan) and compares it with ACTIVE
# MAGIC after this refresh. `mismatches` should be **0**. Run it once or twice to confirm v2 matches v1,
# MAGIC then you can skip this cell.

# COMMAND ----------

check = (
    spark.table(RAW_EVENTS)
    .groupBy("AUTO_POLICY_ID")
    .agg(
        F.sum(F.when(F.col("SRC_TRANS_TMSP") >= F.expr(f"TIMESTAMP '{run_ts}' - INTERVAL 1 DAY"), 1).otherwise(0)).alias("TXN_CNT_1D"),
        F.sum(F.when(F.col("SRC_TRANS_TMSP") >= F.expr(f"TIMESTAMP '{run_ts}' - INTERVAL 7 DAYS"), 1).otherwise(0)).alias("TXN_CNT_1W"),
        F.sum(F.when(F.col("SRC_TRANS_TMSP") >= F.expr(f"TIMESTAMP '{run_ts}' - INTERVAL 1 MONTH"), 1).otherwise(0)).alias("TXN_CNT_1M"),
        F.min("EFF_DT").alias("_inception"),
        F.max(F.struct("SRC_TRANS_TMSP", "EFF_DT")).alias("_latest"),
    )
    .withColumn("TENURE_YRS", F.floor(F.months_between(F.lit(run_ts).cast("date"), F.col("_inception")) / 12))
    .withColumn("OUTSTANDING_TXN_IND", (F.col("_latest.EFF_DT") > F.lit(run_ts).cast("date")).cast("int"))
    .select("AUTO_POLICY_ID", *GROUP_A_COLUMNS)
)

mismatch_cond = None
for c in GROUP_A_COLUMNS:
    cond = ~F.col(f"v1.{c}").eqNullSafe(F.col(f"a.{c}"))
    mismatch_cond = cond if mismatch_cond is None else (mismatch_cond | cond)

mismatches = (
    check.alias("v1")
    .join(spark.table(ACTIVE).alias("a"), "AUTO_POLICY_ID", "inner")
    .where(mismatch_cond)
)
print(f"mismatches vs original calculation: {mismatches.count()}")
display(mismatches.select("AUTO_POLICY_ID",
                          *[F.col(f"v1.{c}").alias(f"v1_{c}") for c in GROUP_A_COLUMNS],
                          *[F.col(f"a.{c}").alias(f"v2_{c}") for c in GROUP_A_COLUMNS]).limit(50))
