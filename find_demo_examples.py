# Databricks notebook source
# MAGIC %md
# MAGIC # Find Real High-Risk / Low-Risk Examples for the Demo
# MAGIC
# MAGIC The cancellation model is overfit (train accuracy ~0.985) — it's confidently right on most
# MAGIC of the rows it trained on, even though it barely generalizes to new data. Rather than guessing
# MAGIC which feature edits move the score, this pulls two REAL historical policy transactions from
# MAGIC the actual training set:
# MAGIC   - the one the model is MOST confident is a cancellation (for your "high risk" demo state)
# MAGIC   - the one the model is MOST confident is NOT a cancellation (for your "low risk" demo state)
# MAGIC
# MAGIC Both are genuine feature values from real transactions — not fabricated — so this is an honest
# MAGIC demonstration of the model's actual (if flawed) behavior.
# MAGIC
# MAGIC Standalone — does not modify any other notebook. Reconstructs the exact same train/test split
# MAGIC as the original training notebook (same table, same FEATURE_COLS, same stratified group split,
# MAGIC same random_state=42) so X_train here matches what the model actually trained on.

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
import pandas as pd
import mlflow
from sklearn.model_selection import StratifiedShuffleSplit

dbutils.widgets.text("catalog",         "ws_prd_analytics")
dbutils.widgets.text("schema",          "default")
dbutils.widgets.text("source_table",    "cancellation_auto_features")
dbutils.widgets.text("base_model_name", "cancellation_transaction_model")

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
SOURCE_TABLE    = dbutils.widgets.get("source_table")
BASE_MODEL_NAME = dbutils.widgets.get("base_model_name")

mlflow.set_registry_uri("databricks-uc")
BASE_MODEL_URI = f"models:/{CATALOG}.{SCHEMA}.{BASE_MODEL_NAME}@champion"

print(f"Source table : {CATALOG}.{SCHEMA}.{SOURCE_TABLE}")
print(f"Model        : {BASE_MODEL_URI}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load the existing trained model (no retraining)

# COMMAND ----------

model = mlflow.sklearn.load_model(BASE_MODEL_URI)

if getattr(model, "feature_names_in_", None) is not None:
    FEATURE_COLS = list(model.feature_names_in_)
else:
    raise ValueError("model.feature_names_in_ unavailable — paste FEATURE_COLS from the training notebook here.")

print(f"Model loaded. Feature count: {len(FEATURE_COLS)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Reconstruct the same data + target the model trained on

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE}").toPandas()

df["target"] = np.where(
    df["DAYS_TIL_NEXT_CNCL"].notna()
    & (df["DAYS_TIL_NEXT_CNCL"] < 9999)
    & (df["DAYS_TIL_NEXT_CNCL"] > 0),
    1, 0,
)

print(f"Rows: {len(df)}   Cancellation rate: {df['target'].mean():.2%}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Reconstruct the exact same stratified group split (random_state=42)

# COMMAND ----------

policy_df = df.groupby("PLCY_CNTRCT_NUM").agg(
    has_cancel=("target", "max"),
    n_rows=("target", "count"),
).reset_index()

splitter = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
train_idx, test_idx = next(splitter.split(policy_df, policy_df["has_cancel"]))

train_policy_set = set(policy_df.iloc[train_idx]["PLCY_CNTRCT_NUM"])
test_policy_set  = set(policy_df.iloc[test_idx]["PLCY_CNTRCT_NUM"])

train_df = df[df["PLCY_CNTRCT_NUM"].isin(train_policy_set)].reset_index(drop=True)

X_train = train_df[FEATURE_COLS]
y_train = train_df["target"]

print(f"Train rows: {len(X_train)}   Train cancellation rate: {y_train.mean():.2%}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Score every training row, find the most confident examples of each class

# COMMAND ----------

probs = model.predict_proba(X_train)[:, 1]
train_df_scored = train_df.copy()
train_df_scored["predicted_probability"] = probs
train_df_scored["predicted_percentage"] = (probs * 100).round(2)

print("Predicted probability distribution on training data:")
print(train_df_scored["predicted_percentage"].describe())

# COMMAND ----------

cancel_rows    = train_df_scored[train_df_scored["target"] == 1]
no_cancel_rows = train_df_scored[train_df_scored["target"] == 0]

high_risk_row = cancel_rows.loc[cancel_rows["predicted_percentage"].idxmax()]
low_risk_row  = no_cancel_rows.loc[no_cancel_rows["predicted_percentage"].idxmin()]

print("=" * 70)
print(f"HIGH-RISK example (real cancellation, model confidence {high_risk_row['predicted_percentage']}%)")
print("=" * 70)
print(f"AUTO_POLICY_ID: {high_risk_row.get('AUTO_POLICY_ID', 'n/a')}")
for c in FEATURE_COLS:
    print(f"  {c:<25} = {high_risk_row[c]}")

print("\n" + "=" * 70)
print(f"LOW-RISK example (real non-cancellation, model confidence {low_risk_row['predicted_percentage']}%)")
print("=" * 70)
print(f"AUTO_POLICY_ID: {low_risk_row.get('AUTO_POLICY_ID', 'n/a')}")
for c in FEATURE_COLS:
    print(f"  {c:<25} = {low_risk_row[c]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ready-to-paste JSON snippets for your demo transactions

# COMMAND ----------

import json

high_risk_json = {c: (int(high_risk_row[c]) if float(high_risk_row[c]).is_integer() else float(high_risk_row[c])) for c in FEATURE_COLS}
low_risk_json  = {c: (int(low_risk_row[c]) if float(low_risk_row[c]).is_integer() else float(low_risk_row[c])) for c in FEATURE_COLS}

print("HIGH-RISK feature values (paste into your 'after' demo JSON):")
print(json.dumps(high_risk_json, indent=2))

print("\nLOW-RISK feature values (paste into your 'before' demo JSON):")
print(json.dumps(low_risk_json, indent=2))
