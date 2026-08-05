# Databricks notebook source
# MAGIC %md
# MAGIC # Debug: Compute Features Step-by-Step (inspection utility — not a pipeline step)
# MAGIC
# MAGIC Runs the exact same logic as `04_sp_compute_features.py`'s `compute_batch()`, but as a
# MAGIC plain one-shot script (no streaming, no writing to `COMPUTED_FEATURE_EVENTS`) with a
# MAGIC `display()` after every stage — so you can see what each intermediate DataFrame actually
# MAGIC looks like for one policy.
# MAGIC
# MAGIC Set `POLICY_ID` below to whichever `AUTO_POLICY_ID` you just published a test transaction
# MAGIC for, then run the whole notebook top to bottom.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

dbutils.widgets.text("catalog",   "main")
dbutils.widgets.text("schema",    "final_database")
dbutils.widgets.text("policy_id", "503203043")

CATALOG   = dbutils.widgets.get("catalog")
SCHEMA    = dbutils.widgets.get("schema")
POLICY_ID = dbutils.widgets.get("policy_id")

RAW_EVENTS = f"{CATALOG}.{SCHEMA}.RAW_POLICY_EVENTS"
CVG_CATEGORIES = ["BI", "PD", "MD", "COLL", "COMP", "PIP"]

print(f"Inspecting AUTO_POLICY_ID = {POLICY_ID}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Same JSON schema + helper as 04_sp_compute_features

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
    StructField("auto_soi_trans_sk",      StringType()),
    StructField("mva_cvg_ctgy_cd",        StringType()),
    StructField("cvg_typ_cd",             StringType()),
    StructField("ded_amt",                DoubleType()),
    StructField("old_full_term_prem_amt", DoubleType()),
    StructField("new_full_term_prem_amt", DoubleType()),
])
ARRAYS_SCHEMA = StructType([
    StructField("1_vehicle_snapshot",  ArrayType(VEHICLE_SCHEMA)),
    StructField("2_driver_snapshot",   ArrayType(DRIVER_SCHEMA)),
    StructField("3_coverage_snapshot", ArrayType(COVERAGE_SCHEMA)),
])

def map_change_ind(cur_map, prev_map):
    return F.when(
        F.expr(f"""
            exists(map_entries({cur_map}), e ->
                {prev_map}[e.key] IS NOT NULL
                AND e.value IS NOT NULL
                AND {prev_map}[e.key] != e.value)
        """), F.lit(1)).otherwise(F.lit(0))

txn_keys = ["TRANS_ID", "AUTO_POLICY_ID", "PLCY_CNTRCT_NUM", "SRC_HH_NUM", "EFF_DT", "SRC_TRANS_TMSP"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 1 — Raw rows for this policy (RAW_POLICY_EVENTS)

# COMMAND ----------

raw_for_policy = spark.table(RAW_EVENTS).filter(F.col("AUTO_POLICY_ID") == POLICY_ID)
print(f"{raw_for_policy.count()} raw transaction(s) landed for this policy")
display(raw_for_policy.orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 2 — txn_base: JSON parsed into the 3 arrays

# COMMAND ----------

txn_base = (
    raw_for_policy
    .withColumn("arrays", F.from_json(F.col("RAW_PAYLOAD"), ARRAYS_SCHEMA))
    .select(
        *txn_keys,
        F.col("arrays.`1_vehicle_snapshot`").alias("vehicles"),
        F.col("arrays.`2_driver_snapshot`").alias("drivers"),
        F.col("arrays.`3_coverage_snapshot`").alias("coverages"),
    )
)
display(txn_base.orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 3 — Exploded vehicles / drivers / coverages (one row per array item)

# COMMAND ----------

df_veh = (
    txn_base.select(*txn_keys, F.explode_outer("vehicles").alias("v"))
    .select(*txn_keys,
            F.col("v.vin").alias("vin"),
            F.col("v.mdl_yr").alias("mdl_yr"),
            F.col("v.auto_soi_trans_sk").alias("auto_soi_trans_sk"))
)
print("df_veh — one row per vehicle per transaction")
display(df_veh.orderBy("SRC_TRANS_TMSP"))

df_drv = (
    txn_base.select(*txn_keys, F.explode_outer("drivers").alias("d"))
    .select(*txn_keys,
            F.col("d.drvr_id_sk").alias("drvr_id_sk"),
            F.to_date("d.drvr_brth_dt").alias("drvr_brth_dt"),
            F.col("d.drvr_gndr_cd").alias("drvr_gndr_cd"),
            F.col("d.drvr_mrtl_cd").alias("drvr_mrtl_cd"))
)
print("df_drv — one row per driver per transaction")
display(df_drv.orderBy("SRC_TRANS_TMSP"))

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
print("df_cvg — one row per coverage per transaction, joined to its vehicle's VIN")
display(df_cvg.orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 4 — Snapshot aggregates (one row per transaction again)

# COMMAND ----------

agg_veh = (
    df_veh.groupBy(*txn_keys)
    .agg(F.countDistinct("vin").alias("veh_cnt"),
         F.floor(F.avg(F.year(F.current_date()) - F.col("mdl_yr"))).alias("avg_veh_age"),
         F.collect_set("vin").alias("vin_set"))
)
print("agg_veh")
display(agg_veh.orderBy("SRC_TRANS_TMSP"))

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
print("agg_drv — note the gndr_map / mrtl_map columns")
display(agg_drv.orderBy("SRC_TRANS_TMSP"))

agg_prem = (
    df_cvg.groupBy(*txn_keys)
    .agg(F.sum(F.coalesce(F.col("new_ft_prem"), F.lit(0.0)) - F.coalesce(F.col("old_ft_prem"), F.lit(0.0)))
          .alias("prem_change_amt"))
)
print("agg_prem")
display(agg_prem.orderBy("SRC_TRANS_TMSP"))

df_cvg_scoped = (
    df_cvg.filter(F.col("cvg_ctgy").isin(CVG_CATEGORIES))
          .withColumn("cvg_key", F.concat_ws("|", F.col("vin"), F.col("cvg_typ_cd")))
)
print("df_cvg_scoped — only BI/PD/MD/COLL/COMP/PIP rows, with the (vin|cvg_typ_cd) match key")
display(df_cvg_scoped.orderBy("SRC_TRANS_TMSP"))

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
print("agg_cvg — one key_set array per category, plus comp_ded_map")
display(agg_cvg.orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 5 — txn_snapshot: everything joined, one row per transaction

# COMMAND ----------

txn_snapshot = (
    txn_base.select(*txn_keys)
    .join(agg_veh, txn_keys, "left")
    .join(agg_drv, txn_keys, "left")
    .join(agg_prem, txn_keys, "left")
    .join(agg_cvg, txn_keys, "left")
)
display(txn_snapshot.orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 6 — txn_lag: previous-transaction values attached via lag()

# COMMAND ----------

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

print("txn_lag (before deltas) — compare cur vs prev_* columns side by side")
display(txn_lag.select("TRANS_ID", "SRC_TRANS_TMSP", "vin_set", "prev_vin_set",
                        "drvr_id_set", "prev_drvr_id_set").orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 7 — Deltas computed from the lag columns

# COMMAND ----------

# veh/drvr "added" = count of genuinely NEW vin/drvr_id_sk values vs the previous transaction
# (not a net count delta — a swap nets to 0 on count alone, but should show up as 1 added here)
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

print("txn_lag (with deltas) — the actual GROUP_B lag columns")
display(txn_lag.select(
    "TRANS_ID", "SRC_TRANS_TMSP",
    "drvrs_added_latest_txn", "veh_added_latest_txn",
    "gndr_change_ind", "mrtl_change_ind", "comp_ded_change_ind",
    *[c for cat in CVG_CATEGORIES for c in (f"{cat}_coverage_added", f"{cat}_coverage_removed")],
).orderBy("SRC_TRANS_TMSP"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 8 — hist_agg: GROUP_A history features (wall-clock driven)

# COMMAND ----------

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
display(hist_agg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## STEP 9 — Final: every transaction for this policy, all ~29 feature columns
# MAGIC
# MAGIC (04_sp_compute_features would filter this down to only the newly-landed TRANS_IDs before
# MAGIC writing — here we show every transaction for this policy so you can see the full timeline.)

# COMMAND ----------

final_preview = (
    txn_lag
    .withColumn("outstanding_txn_ind", (F.col("EFF_DT") > F.current_date()).cast("int"))
    .join(hist_agg, "AUTO_POLICY_ID", "left")
    .withColumn("tenure_yrs", F.floor(F.months_between(F.current_date(), F.col("policy_inception_eff_dt")) / 12))
    .select(
        "TRANS_ID", "AUTO_POLICY_ID", "PLCY_CNTRCT_NUM", "SRC_HH_NUM",
        "SRC_TRANS_TMSP", "EFF_DT",
        "txn_cnt_1d", "txn_cnt_1w", "txn_cnt_1m", "outstanding_txn_ind", "tenure_yrs",
        "veh_cnt", "avg_veh_age", "veh_added_latest_txn",
        "drvr_cnt", "max_drvr_age", "min_drvr_age", "avg_drvr_age", "drvrs_added_latest_txn",
        "prem_change_amt",
        *[c for cat in CVG_CATEGORIES for c in (f"{cat}_coverage_added", f"{cat}_coverage_removed")],
        "comp_ded_change_ind", "gndr_change_ind", "mrtl_change_ind",
    )
)
display(final_preview.orderBy("SRC_TRANS_TMSP"))
