# Databricks notebook source
# MAGIC %md
# MAGIC # Cancellation Prediction — Scoring Wrapper + Serving Endpoint
# MAGIC
# MAGIC Wraps the EXISTING trained cancellation model (`cancellation_transaction_model@champion`)
# MAGIC so it returns both the binary prediction and the probability, registers that wrapper as its
# MAGIC own model, and stands up a Model Serving Endpoint for it — so Postman can POST a feature
# MAGIC record and get back:
# MAGIC ```
# MAGIC { "prediction": 0 or 1, "cancellation_probability": 0.3476, "cancellation_percentage": 34.76 }
# MAGIC ```
# MAGIC
# MAGIC Standalone — does not modify `01`-`09` or the original trained model. Run top to bottom.
# MAGIC
# MAGIC Requires the feature-serving endpoint (`08_online_feature_store.py`) to already be `READY`,
# MAGIC since this notebook fetches one real record from it to use as the test/registration example.

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time
import requests
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow import MlflowClient

dbutils.widgets.text("catalog",               "ws_prd_analytics")
dbutils.widgets.text("schema",                "default")
dbutils.widgets.text("base_model_name",       "cancellation_transaction_model")
dbutils.widgets.text("scoring_model_name",    "cancellation_transaction_scoring_model")
dbutils.widgets.text("feature_endpoint_name", "auto-policy-feature-serving")
dbutils.widgets.text("scoring_endpoint_name", "cancellation-scoring-serving")
dbutils.widgets.text("demo_policy_id",        "503203043")

CATALOG               = dbutils.widgets.get("catalog")
SCHEMA                = dbutils.widgets.get("schema")
BASE_MODEL_NAME        = dbutils.widgets.get("base_model_name")
SCORING_MODEL_NAME_RAW = dbutils.widgets.get("scoring_model_name")
FEATURE_ENDPOINT_NAME  = dbutils.widgets.get("feature_endpoint_name")
SCORING_ENDPOINT_NAME  = dbutils.widgets.get("scoring_endpoint_name")
DEMO_POLICY_ID         = dbutils.widgets.get("demo_policy_id")

mlflow.set_registry_uri("databricks-uc")

BASE_MODEL_URI     = f"models:/{CATALOG}.{SCHEMA}.{BASE_MODEL_NAME}@champion"
SCORING_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{SCORING_MODEL_NAME_RAW}"

HOST    = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

print(f"Base model      : {BASE_MODEL_URI}")
print(f"Scoring model   : {SCORING_MODEL_NAME}")
print(f"Feature endpoint: {FEATURE_ENDPOINT_NAME}")
print(f"Scoring endpoint: {SCORING_ENDPOINT_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load the existing trained model (no retraining)

# COMMAND ----------

print("Loading existing trained model...")
model = mlflow.sklearn.load_model(BASE_MODEL_URI)
print("Loaded:", BASE_MODEL_URI)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Get the model's feature list

# COMMAND ----------

if getattr(model, "feature_names_in_", None) is not None:
    FEATURE_COLS = list(model.feature_names_in_)
else:
    raise ValueError(
        "model.feature_names_in_ is not available. "
        "Paste the exact FEATURE_COLS list from the original training notebook here."
    )

print(f"Model feature count: {len(FEATURE_COLS)}")
print("Model features:", FEATURE_COLS)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Simple scoring function

# COMMAND ----------

def score_policy(policy_json: dict) -> dict:
    """Score one policy feature record. Extra fields (e.g. AUTO_POLICY_ID) are ignored."""
    df = pd.DataFrame([policy_json])

    missing_features = [f for f in FEATURE_COLS if f not in df.columns]
    if missing_features:
        raise ValueError(f"Missing required model features: {missing_features}")

    X = df[FEATURE_COLS].copy()
    prediction   = int(model.predict(X)[0])
    probability  = float(model.predict_proba(X)[0][1])
    status       = "Cancellation Is Coming!" if prediction == 1 else "Not Today!"

    return {
        "cancellation_probability": probability,
        "cancellation_status": status,
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Fetch one REAL feature record from the online feature store
# MAGIC
# MAGIC Uses the same Feature Serving Endpoint you already query from Postman — no manual
# MAGIC copy/paste of feature values needed.

# COMMAND ----------

def fetch_online_features(policy_id: str) -> dict:
    url     = f"https://{HOST}/serving-endpoints/{FEATURE_ENDPOINT_NAME}/invocations"
    payload = {"dataframe_records": [{"AUTO_POLICY_ID": policy_id}]}
    resp    = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Feature lookup failed {resp.status_code}: {resp.text}")
    return resp.json()["outputs"][0]


demo_policy = fetch_online_features(DEMO_POLICY_ID)
print(f"Fetched features for policy {DEMO_POLICY_ID}:")
print(demo_policy)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Local test — score the fetched record

# COMMAND ----------

result = score_policy(demo_policy)
print("\nDemo prediction:")
print(result)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. MLflow pyfunc wrapper

# COMMAND ----------

class CancellationScoringWrapper(mlflow.pyfunc.PythonModel):
    """Thin inference wrapper around the existing trained model. Does not train anything."""

    def __init__(self, trained_model, feature_cols):
        self.trained_model = trained_model
        self.feature_cols  = list(feature_cols)

    def predict(self, context, model_input, params=None):
        missing_features = [f for f in self.feature_cols if f not in model_input.columns]
        if missing_features:
            raise ValueError(f"Missing required model features: {missing_features}")

        X = model_input[self.feature_cols].copy()
        prediction  = self.trained_model.predict(X)
        probability = self.trained_model.predict_proba(X)[:, 1]
        status      = np.where(prediction == 1, "Cancellation Is Coming!", "Not Today!")

        return pd.DataFrame({
            "cancellation_probability": probability.astype(float),
            "cancellation_status": status,
        })


wrapper = CancellationScoringWrapper(trained_model=model, feature_cols=FEATURE_COLS)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Test the wrapper with the fetched record

# COMMAND ----------

input_example = pd.DataFrame([demo_policy])
test_result    = wrapper.predict(context=None, model_input=input_example)
display(test_result)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Register the scoring wrapper in Unity Catalog

# COMMAND ----------

sample_output = wrapper.predict(context=None, model_input=input_example)
signature      = infer_signature(input_example, sample_output)

with mlflow.start_run(run_name="cancellation_scoring_wrapper"):
    model_info = mlflow.pyfunc.log_model(
        name="cancellation_scoring_model",
        python_model=wrapper,
        input_example=input_example,
        signature=signature,
        registered_model_name=SCORING_MODEL_NAME,
    )

print("Scoring wrapper registered:", SCORING_MODEL_NAME)
print("Logged model URI:", model_info.model_uri)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Set the newest version as `champion`

# COMMAND ----------

client   = MlflowClient()
versions = client.search_model_versions(f"name='{SCORING_MODEL_NAME}'")
if not versions:
    raise ValueError(f"No versions found for {SCORING_MODEL_NAME}")

latest_version = max(versions, key=lambda v: int(v.version)).version
client.set_registered_model_alias(SCORING_MODEL_NAME, "champion", latest_version)
print(f"Alias set: {SCORING_MODEL_NAME}@champion -> version {latest_version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Create the Model Serving Endpoint, or update it to this version if it already exists
# MAGIC
# MAGIC Re-running this whole notebook always keeps the LIVE endpoint pointed at whatever version was
# MAGIC just registered above — no separate manual update step needed.

# COMMAND ----------

# NOTE: the create API (POST /serving-endpoints) accepts "served_entities" with
# entity_name/entity_version, but THIS workspace's update API (PUT .../config) requires the
# older "served_models" field name with model_name/model_version instead — confirmed via the
# actual 400 error: "config.served_models must contain at least one element". Using the wrong
# field name doesn't error loudly on its own terms, so verify with a GET after any change.

ep_resp = requests.get(
    f"https://{HOST}/api/2.0/serving-endpoints/{SCORING_ENDPOINT_NAME}", headers=HEADERS
)

if ep_resp.status_code == 200:
    print(f"Endpoint exists — updating to version {latest_version} ...")
    ur = requests.put(
        f"https://{HOST}/api/2.0/serving-endpoints/{SCORING_ENDPOINT_NAME}/config",
        headers=HEADERS,
        json={"served_models": [{
            "name": "cancellation-scorer",
            "model_name": SCORING_MODEL_NAME,
            "model_version": str(latest_version),
            "workload_size": "Small",
            "scale_to_zero_enabled": True,
        }]},
    )
    if ur.status_code not in (200, 201):
        raise RuntimeError(f"Update failed {ur.status_code}: {ur.text[:400]}")
else:
    print(f"Creating endpoint: {SCORING_ENDPOINT_NAME} ...")
    body = {
        "name": SCORING_ENDPOINT_NAME,
        "config": {"served_entities": [{
            "name": "cancellation-scorer",
            "entity_name": SCORING_MODEL_NAME,
            "entity_version": str(latest_version),
            "workload_size": "Small",
            "scale_to_zero_enabled": True,
        }]},
    }
    cr = requests.post(f"https://{HOST}/api/2.0/serving-endpoints", headers=HEADERS, json=body)
    if cr.status_code not in (200, 201):
        raise RuntimeError(f"Create failed {cr.status_code}: {cr.text[:400]}")

print("Waiting for READY (up to 20 min)...")
for i in range(60):
    poll   = requests.get(f"https://{HOST}/api/2.0/serving-endpoints/{SCORING_ENDPOINT_NAME}", headers=HEADERS)
    ep     = poll.json().get("state", {})
    ready  = ep.get("ready", "unknown")
    update = ep.get("config_update", "")
    print(f"  [{i*20}s] ready={ready}  config_update={update}")
    if ready == "READY" and update != "IN_PROGRESS":
        print("\nEndpoint is READY.")
        break
    if update == "UPDATE_FAILED":
        print("\nFAILED. Check Databricks UI > Serving > Logs.")
        break
    time.sleep(20)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. Verify — call the new scoring endpoint end to end

# COMMAND ----------

def score_via_endpoint(policy_id: str) -> dict:
    features = fetch_online_features(policy_id)
    url      = f"https://{HOST}/serving-endpoints/{SCORING_ENDPOINT_NAME}/invocations"
    payload  = {"dataframe_records": [features]}
    resp     = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Scoring failed {resp.status_code}: {resp.text}")
    return resp.json()


final_check = score_via_endpoint(DEMO_POLICY_ID)
print(f"\nEnd-to-end score for policy {DEMO_POLICY_ID}:")
print(final_check)

print(f"""
>>> Postman setup for the demo <<<

Request A (already have) — Feature lookup:
  POST https://{HOST}/serving-endpoints/{FEATURE_ENDPOINT_NAME}/invocations
  Body: {{ "dataframe_records": [{{ "AUTO_POLICY_ID": "<policy_id>" }}] }}

Request B (new) — Cancellation score:
  POST https://{HOST}/serving-endpoints/{SCORING_ENDPOINT_NAME}/invocations
  Body: {{ "dataframe_records": [ <29 feature values from Request A's response> ] }}
  Response: {{ "predictions": [{{ "prediction": 0/1, "cancellation_probability": ..., "cancellation_percentage": ... }}] }}
""")
