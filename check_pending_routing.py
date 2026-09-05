# Databricks notebook source
# MAGIC %md
# MAGIC # Check Pending Routing — how many COMPUTED_FEATURE_EVENTS rows still need `05`
# MAGIC
# MAGIC There's no "pending" flag column anywhere — `05`/`05_routing_pipeline_fast_lane`/
# MAGIC `route_single_transaction` all track progress via their own CDF checkpoints, not via table
# MAGIC content. But every event, by design, ALWAYS touches GROUP_A (always `immediate` in the
# MAGIC registry), which always creates a `HISTORY` row with `EVENT_ID = TRANS_ID` once routed —
# MAGIC regardless of which script did the routing. So comparing `COMPUTED_FEATURE_EVENTS.TRANS_ID`
# MAGIC against `AUTO_FEATURES_HISTORY.EVENT_ID` gives an accurate, content-based answer to
# MAGIC "has this transaction actually been routed yet," independent of any checkpoint state.
# MAGIC
# MAGIC Standalone — read-only, does not modify anything.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ws_prd_analytics")
dbutils.widgets.text("schema",  "default")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

COMPUTED = f"{CATALOG}.{SCHEMA}.COMPUTED_FEATURE_EVENTS"
HISTORY  = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_HISTORY"

# COMMAND ----------

computed_df = spark.table(COMPUTED).select("TRANS_ID", "AUTO_POLICY_ID", "SRC_TRANS_TMSP", "COMPUTED_AT")
routed_ids  = spark.table(HISTORY).select(F.col("EVENT_ID").alias("TRANS_ID")).distinct()

pending_df = computed_df.join(routed_ids, on="TRANS_ID", how="left_anti")

total_computed = computed_df.count()
total_routed   = computed_df.count() - pending_df.count()
total_pending  = pending_df.count()

print("=" * 60)
print("ROUTING STATUS")
print("=" * 60)
print(f"Total rows in COMPUTED_FEATURE_EVENTS : {total_computed}")
print(f"Already routed (found in HISTORY)     : {total_routed}")
print(f"Still PENDING (not yet routed)        : {total_pending}")
print(f"Distinct policies still pending       : {pending_df.select('AUTO_POLICY_ID').distinct().count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preview the pending rows

# COMMAND ----------

print("=== Sample of pending (not yet routed) transactions ===")
display(pending_df.orderBy("SRC_TRANS_TMSP").limit(200))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pending count per policy (top 20 policies with the most pending transactions)

# COMMAND ----------

display(
    pending_df.groupBy("AUTO_POLICY_ID")
    .count()
    .orderBy(F.col("count").desc())
    .limit(20)
)
