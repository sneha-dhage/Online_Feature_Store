# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — PySpark: Compute Features → COMPUTED_FEATURE_EVENTS (Silver)
# MAGIC
# MAGIC Streams new transactions from `RAW_POLICY_EVENTS` via CDF. For every policy touched in a
# MAGIC micro-batch, re-reads that policy's **full** transaction history (not just the new row) and
# MAGIC recomputes GROUP_A / GROUP_B features with PySpark DataFrame ops (explode / groupBy / window
# MAGIC `lag()`) — no SP, no row-by-row parsing. Only the newly-landed `TRANS_ID`s are written out.
# MAGIC
# MAGIC ## Feature Logic
# MAGIC
# MAGIC | Feature group | Source | Key logic |
# MAGIC |---|---|---|
# MAGIC | Snapshot (`veh_cnt`, `avg_veh_age`, `drvr_cnt`, `*_drvr_age`, `prem_change_amt`) | latest transaction's arrays | explode + aggregate |
# MAGIC | Lag (`*_added_latest_txn`, `*_coverage_added/removed`, `*_change_ind`) | latest vs previous transaction | `lag()` over `SRC_TRANS_TMSP`, then a real set difference (`array_except`) — match key = `vin` for vehicles, `drvr_id_sk` for drivers, `(vin, cvg_typ_cd)` for coverage. A swap (one VIN removed, a different one added) correctly shows up as 1 added, not 0 — it's never a plain count delta. |
# MAGIC | History (`txn_cnt_1d/1w/1m`, `tenure_yrs`) | all transactions for the policy | driven by wall-clock `current_timestamp()`, not event time — see `07_job_daily_group_a_refresh` for the daily drift |
# MAGIC
# MAGIC Coverage category filter: only `mva_cvg_ctgy_cd IN (BI, PD, MD, COLL, COMP, PIP)` feeds the
# MAGIC added/removed flags — everything else (`BIPD`, `UM`, `TOW`, ...) is ignored.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

dbutils.widgets.text("catalog",         "main")
dbutils.widgets.text("schema",          "final_database")
dbutils.widgets.text("checkpoint_path", "/Volumes/main/final_database/checkpoints/sp_compute")

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")

RAW_EVENTS = f"{CATALOG}.{SCHEMA}.RAW_POLICY_EVENTS"
COMPUTED   = f"{CATALOG}.{SCHEMA}.COMPUTED_FEATURE_EVENTS"

CVG_CATEGORIES = ["BI", "PD", "MD", "COLL", "COMP", "PIP"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## JSON Schema (partial — only the fields features are computed from)

# COMMAND ----------

VEHICLE_SCHEMA = StructType([
    StructField("vin",               StringType()),
    StructField("mdl_yr",            IntegerType()),
    StructField("auto_soi_trans_sk", StringType()),
])

DRIVER_SCHEMA = StructType([
    StructField("drvr_id_sk",   StringType()),
    StructField("drvr_brth_dt", StringType()),
    StructField("drvr_gndr_cd", StringType()),
    StructField("drvr_mrtl_cd", StringType()),
])

COVERAGE_SCHEMA = StructType([
    StructField("auto_soi_trans_sk",       StringType()),
    StructField("mva_cvg_ctgy_cd",         StringType()),
    StructField("cvg_typ_cd",              StringType()),
    StructField("ded_amt",                 DoubleType()),
    StructField("old_full_term_prem_amt",  DoubleType()),
    StructField("new_full_term_prem_amt",  DoubleType()),
])

ARRAYS_SCHEMA = StructType([
    StructField("1_vehicle_snapshot",  ArrayType(VEHICLE_SCHEMA)),
    StructField("2_driver_snapshot",   ArrayType(DRIVER_SCHEMA)),
    StructField("3_coverage_snapshot", ArrayType(COVERAGE_SCHEMA)),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: map-based change indicator
# MAGIC
# MAGIC 1 if any key present in BOTH maps has a different value between current and previous transaction.

# COMMAND ----------

def map_change_ind(cur_map, prev_map):
    return F.when(
        F.expr(f"""
            exists(map_entries({cur_map}), e ->
                {prev_map}[e.key] IS NOT NULL
                AND e.value IS NOT NULL
                AND {prev_map}[e.key] != e.value)
        """), F.lit(1)).otherwise(F.lit(0))

# COMMAND ----------
# MAGIC %md
# MAGIC ## foreachBatch — Recompute Touched Policies from Full History

# COMMAND ----------

def compute_batch(df, batch_id):
    new_events = df.filter(F.col("_change_type") == "insert")
    count = new_events.count()
    print(f"\n[batch {batch_id}] {count} new raw event(s)")
    if count == 0:
        return

    touched_policies = [r["AUTO_POLICY_ID"] for r in new_events.select("AUTO_POLICY_ID").distinct().collect()]
    new_trans_ids     = [r["TRANS_ID"] for r in new_events.select("TRANS_ID").distinct().collect()]
    print(f"  policies touched : {len(touched_policies)}")
    print(f"  new transactions : {len(new_trans_ids)}")

    txn_keys = ["TRANS_ID", "AUTO_POLICY_ID", "PLCY_CNTRCT_NUM", "SRC_HH_NUM", "EFF_DT", "SRC_TRANS_TMSP"]

    # ── Re-read FULL history for touched policies so lag()/history features are correct ──
    txn_base = (
        spark.table(RAW_EVENTS)
             .filter(F.col("AUTO_POLICY_ID").isin(touched_policies))
             .withColumn("arrays", F.from_json(F.col("RAW_PAYLOAD"), ARRAYS_SCHEMA))
             .select(
                 *txn_keys,
                 F.col("arrays.`1_vehicle_snapshot`").alias("vehicles"),
                 F.col("arrays.`2_driver_snapshot`").alias("drivers"),
                 F.col("arrays.`3_coverage_snapshot`").alias("coverages"),
             )
    )

    # ── Explode the three snapshot arrays ────────────────────────────────
    df_veh = (
        txn_base.select(*txn_keys, F.explode_outer("vehicles").alias("v"))
        .select(*txn_keys,
                F.col("v.vin").alias("vin"),
                F.col("v.mdl_yr").alias("mdl_yr"),
                F.col("v.auto_soi_trans_sk").alias("auto_soi_trans_sk"))
    )

    df_drv = (
        txn_base.select(*txn_keys, F.explode_outer("drivers").alias("d"))
        .select(*txn_keys,
                F.col("d.drvr_id_sk").alias("drvr_id_sk"),
                F.to_date("d.drvr_brth_dt").alias("drvr_brth_dt"),
                F.col("d.drvr_gndr_cd").alias("drvr_gndr_cd"),
                F.col("d.drvr_mrtl_cd").alias("drvr_mrtl_cd"))
    )

    df_cvg = (
        txn_base.select(*txn_keys, F.explode_outer("coverages").alias("c"))
        .select(*txn_keys,
                F.col("c.auto_soi_trans_sk").alias("auto_soi_trans_sk"),
                F.col("c.mva_cvg_ctgy_cd").alias("cvg_ctgy"),
                F.col("c.cvg_typ_cd").alias("cvg_typ_cd"),
                F.col("c.ded_amt").alias("ded_amt"),
                F.col("c.old_full_term_prem_amt").alias("old_ft_prem"),
                F.col("c.new_full_term_prem_amt").alias("new_ft_prem"))
        .join(df_veh.select("TRANS_ID", "auto_soi_trans_sk", "vin"),
              on=["TRANS_ID", "auto_soi_trans_sk"], how="left")
    )

    # ── Snapshot aggregates per transaction ──────────────────────────────
    agg_veh = (
        df_veh.groupBy(*txn_keys)
        .agg(F.countDistinct("vin").alias("veh_cnt"),
             F.floor(F.avg(F.year(F.current_date()) - F.col("mdl_yr"))).alias("avg_veh_age"),
             F.collect_set("vin").alias("vin_set"))
    )

    agg_drv = (
        df_drv
        .withColumn("drvr_age", F.floor(F.months_between(F.current_date(), "drvr_brth_dt") / 12))
        .groupBy(*txn_keys)
        .agg(F.countDistinct("drvr_id_sk").alias("drvr_cnt"),
             F.max("drvr_age").alias("max_drvr_age"),
             F.min("drvr_age").alias("min_drvr_age"),
             F.floor(F.avg("drvr_age")).alias("avg_drvr_age"),
             F.map_from_entries(F.collect_list(
                 F.struct(F.col("drvr_id_sk").cast("string"), F.col("drvr_gndr_cd")))).alias("gndr_map"),
             F.map_from_entries(F.collect_list(
                 F.struct(F.col("drvr_id_sk").cast("string"), F.col("drvr_mrtl_cd")))).alias("mrtl_map"),
             F.collect_set(F.col("drvr_id_sk").cast("string")).alias("drvr_id_set"))
    )

    agg_prem = (
        df_cvg.groupBy(*txn_keys)
        .agg(F.sum(F.coalesce(F.col("new_ft_prem"), F.lit(0.0)) - F.coalesce(F.col("old_ft_prem"), F.lit(0.0)))
              .alias("prem_change_amt"))
    )

    # Match key for coverage lag = (vin, cvg_typ_cd) — scoped to the 6 target categories
    df_cvg_scoped = (
        df_cvg.filter(F.col("cvg_ctgy").isin(CVG_CATEGORIES))
              .withColumn("cvg_key", F.concat_ws("|", F.col("vin"), F.col("cvg_typ_cd")))
    )

    agg_cvg = (
        df_cvg_scoped.groupBy(*txn_keys)
        .agg(
            *[F.collect_set(F.when(F.col("cvg_ctgy") == cat, F.col("cvg_key"))).alias(f"{cat}_key_set")
              for cat in CVG_CATEGORIES],
            F.map_from_entries(F.collect_list(
                F.when(F.col("cvg_ctgy") == "COMP", F.struct(F.col("cvg_key"), F.col("ded_amt")))
            )).alias("comp_ded_map"),
        )
    )

    txn_snapshot = (
        txn_base.select(*txn_keys)
        .join(agg_veh, txn_keys, "left")
        .join(agg_drv, txn_keys, "left")
        .join(agg_prem, txn_keys, "left")
        .join(agg_cvg, txn_keys, "left")
    )

    # ── LAG layer: latest vs previous transaction, ordered by SRC_TRANS_TMSP ──
    w_asc = Window.partitionBy("AUTO_POLICY_ID").orderBy(F.col("SRC_TRANS_TMSP").asc())

    txn_lag = (
        txn_snapshot
        .withColumn("prev_drvr_cnt",     F.lag("drvr_cnt").over(w_asc))
        .withColumn("prev_veh_cnt",      F.lag("veh_cnt").over(w_asc))
        .withColumn("prev_vin_set",      F.lag("vin_set").over(w_asc))
        .withColumn("prev_drvr_id_set",  F.lag("drvr_id_set").over(w_asc))
        .withColumn("prev_gndr_map",     F.lag("gndr_map").over(w_asc))
        .withColumn("prev_mrtl_map",     F.lag("mrtl_map").over(w_asc))
        .withColumn("prev_comp_ded_map", F.lag("comp_ded_map").over(w_asc))
    )
    for cat in CVG_CATEGORIES:
        txn_lag = txn_lag.withColumn(f"prev_{cat}_key_set", F.lag(f"{cat}_key_set").over(w_asc))

    # veh/drvr "added" = count of genuinely NEW vin/drvr_id_sk values vs the previous transaction
    # (not a net count delta — swapping one vehicle for a different one nets to 0 on count alone,
    # but is exactly the case that should show up as 1 added here)
    txn_lag = (
        txn_lag
        .withColumn("drvrs_added_latest_txn",
                    F.size(F.array_except(F.coalesce(F.col("drvr_id_set"), F.array()),
                                           F.coalesce(F.col("prev_drvr_id_set"), F.array()))))
        .withColumn("veh_added_latest_txn",
                    F.size(F.array_except(F.coalesce(F.col("vin_set"), F.array()),
                                           F.coalesce(F.col("prev_vin_set"), F.array()))))
        .withColumn("gndr_change_ind",        map_change_ind("gndr_map", "prev_gndr_map"))
        .withColumn("mrtl_change_ind",        map_change_ind("mrtl_map", "prev_mrtl_map"))
        .withColumn("comp_ded_change_ind",    map_change_ind("comp_ded_map", "prev_comp_ded_map"))
    )

    for cat in CVG_CATEGORIES:
        cur_set, prev_set = f"{cat}_key_set", f"prev_{cat}_key_set"
        txn_lag = (
            txn_lag
            .withColumn(f"{cat}_coverage_added",
                        (F.size(F.array_except(F.coalesce(F.col(cur_set), F.array()),
                                                F.coalesce(F.col(prev_set), F.array()))) > 0).cast("int"))
            .withColumn(f"{cat}_coverage_removed",
                        (F.size(F.array_except(F.coalesce(F.col(prev_set), F.array()),
                                                F.coalesce(F.col(cur_set), F.array()))) > 0).cast("int"))
        )

    # ── HISTORY layer: driven by wall-clock time, same for every txn of a policy right now ──
    hist_agg = (
        txn_base.groupBy("AUTO_POLICY_ID")
        .agg(
            F.sum(F.when(F.col("SRC_TRANS_TMSP") >= F.expr("current_timestamp() - interval 1 day"), 1)
                   .otherwise(0)).alias("txn_cnt_1d"),
            F.sum(F.when(F.col("SRC_TRANS_TMSP") >= F.expr("current_timestamp() - interval 7 days"), 1)
                   .otherwise(0)).alias("txn_cnt_1w"),
            F.sum(F.when(F.col("SRC_TRANS_TMSP") >= F.expr("current_timestamp() - interval 1 month"), 1)
                   .otherwise(0)).alias("txn_cnt_1m"),
            F.min("EFF_DT").alias("policy_inception_eff_dt"),
        )
    )

    # ── Final: only the newly-landed TRANS_IDs, GROUP_A + GROUP_B side by side ──
    final = (
        txn_lag.filter(F.col("TRANS_ID").isin(new_trans_ids))
        .withColumn("outstanding_txn_ind", (F.col("EFF_DT") > F.current_date()).cast("int"))
        .join(hist_agg, "AUTO_POLICY_ID", "left")
        .withColumn("tenure_yrs", F.floor(F.months_between(F.current_date(), F.col("policy_inception_eff_dt")) / 12))
        .select(
            "TRANS_ID", "AUTO_POLICY_ID", "PLCY_CNTRCT_NUM", "SRC_HH_NUM",
            "SRC_TRANS_TMSP", "EFF_DT",
            # GROUP_A
            "txn_cnt_1d", "txn_cnt_1w", "txn_cnt_1m", "outstanding_txn_ind", "tenure_yrs",
            # GROUP_B
            "veh_cnt", "avg_veh_age", "veh_added_latest_txn",
            "drvr_cnt", "max_drvr_age", "min_drvr_age", "avg_drvr_age", "drvrs_added_latest_txn",
            "prem_change_amt",
            *[c for cat in CVG_CATEGORIES for c in (f"{cat}_coverage_added", f"{cat}_coverage_removed")],
            "comp_ded_change_ind", "gndr_change_ind", "mrtl_change_ind",
        )
        .withColumn("COMPUTED_AT", F.current_timestamp())
    )

    # Rename to match COMPUTED_FEATURE_EVENTS DDL exactly (uppercase columns)
    rename_map = {c: c.upper() for c in final.columns if c != c.upper()}
    for old, new in rename_map.items():
        final = final.withColumnRenamed(old, new)

    written = final.count()
    if written == 0:
        print(f"[batch {batch_id}] nothing to write (all new events failed to join back).")
        return

    final.write.format("delta").mode("append").saveAsTable(COMPUTED)

    for row in final.select("TRANS_ID", "AUTO_POLICY_ID", "VEH_CNT", "DRVR_CNT",
                             "PREM_CHANGE_AMT", "TXN_CNT_1D", "OUTSTANDING_TXN_IND").collect():
        print(f"  [{row['TRANS_ID']}] policy={row['AUTO_POLICY_ID']} | veh_cnt={row['VEH_CNT']} | "
              f"drvr_cnt={row['DRVR_CNT']} | prem_change={row['PREM_CHANGE_AMT']} | "
              f"txn_cnt_1d={row['TXN_CNT_1D']} | outstanding={row['OUTSTANDING_TXN_IND']}")

    print(f"\n[batch {batch_id}] wrote {written} row(s) to COMPUTED_FEATURE_EVENTS.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start Streaming from RAW_POLICY_EVENTS via CDF

# COMMAND ----------

query = (
    spark.readStream
         .format("delta")
         .option("readChangeFeed",  "true")
         .option("startingVersion", "0")
         .table(RAW_EVENTS)
         .writeStream
         .foreachBatch(compute_batch)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(availableNow=True)
         .start()
)

query.awaitTermination()
print("\nPySpark compute — finished.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=== COMPUTED_FEATURE_EVENTS ===")
display(spark.table(COMPUTED).orderBy("COMPUTED_AT"))

print("\n>>> Next: Run 05_routing_pipeline <<<")
