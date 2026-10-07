# Databricks notebook source
# MAGIC %md
# MAGIC # Cancellation Prediction Model — EDA, Feature Engineering, Tuning, Model Comparison
# MAGIC
# MAGIC Predicts whether a policy **cancels after a transaction** (`target = 1`) from the feature-store
# MAGIC features (one row per transaction, e.g. `AUTO_FEATURES_HISTORY` joined with the cancellation labels).
# MAGIC
# MAGIC | Section | What happens |
# MAGIC |---|---|
# MAGIC | 0. Config | **Set `TABLE_NAME`** (left blank) and check the column names |
# MAGIC | 1. Load + target | Read the table, build / check the 0/1 target, remove label-leakage columns |
# MAGIC | 2. EDA | Shape, missing values, target rate, target over time, distributions, correlations, leakage scan |
# MAGIC | 3. Feature engineering | Domain features + per-policy sequence features (past rows only) |
# MAGIC | 4. Split | Hold-out test set **by policy** (no policy in both train and test) |
# MAGIC | 5. Feature selection | Drop constant / near-constant / highly correlated columns (train only) |
# MAGIC | 6. Tuning | Optuna, policy-grouped stratified CV, metric = PR-AUC |
# MAGIC | 7. Models | LightGBM, XGBoost, CatBoost (if available), Random Forest, Logistic Regression |
# MAGIC | 8. Evaluation | ROC/PR curves, threshold, confusion matrix, lift by decile, calibration |
# MAGIC | 9. Explainability | Feature importance + SHAP |
# MAGIC | 10. MLflow | Every model logged; best model saved (optionally registered in Unity Catalog) |
# MAGIC | 11. Scoring helper | Score new rows with the same features and threshold |
# MAGIC
# MAGIC **Why PR-AUC:** cancellations are the minority class (~20%), and PR-AUC rewards finding them
# MAGIC without too many false alarms. ROC-AUC is reported as well.

# COMMAND ----------

# MAGIC %pip install lightgbm xgboost optuna shap catboost seaborn matplotlib scikit-learn mlflow -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Config — fill in `TABLE_NAME`

# COMMAND ----------

TABLE_NAME = ""          # e.g. "ws_prd_analytics.default.cancellation_auto_features"

# Target: use an existing 0/1 column, or derive it from DAYS_COL
TARGET_COL   = "target"              # used as-is if it exists in the table
DAYS_COL     = "DAYS_TIL_NEXT_CNCL"  # used to derive the target if TARGET_COL is missing
HORIZON_DAYS = None                  # e.g. 90 → target = 1 if 0 < days <= 90 ; None → 0 < days < 9999

# Keys and dates
POLICY_COL = "PLCY_ID_SK"            # one policy can have many rows → used to group the split
DATE_COL   = "SRC_TRANS_DT"          # transaction date → time features + sequence order
EFF_COL    = "EFF_DT"                # effective date (optional) → "days until effective" feature
TXN_COL    = "AUTO_PLCY_TRANS_SK"    # transaction id (optional) → tie-breaker for ordering

# Columns that must never be features (ids, label sources, cancellation info, feature-store metadata)
ID_COLS = ["PLCY_ID_SK", "PLCY_CNTRCT_NUM", "AUTO_PLCY_TRANS_SK", "TRANS_ID", "AUTO_POLICY_ID",
           "SRC_HH_NUM", "LAST_SOURCE_EVENT_ID", "EVENT_ID"]
LEAKAGE_COLS = ["NEXT_CNCL_DT", "DAYS_TIL_NEXT_CNCL", "CNCL_TYP_CD", "CNCL_RSN_CD", "CNCL_TYP_DESC",
                "EXCLUDED_CNCL_RSN", "NO_CNCL_RSN_JOIN"]
METADATA_COLS = ["VALID_TRANSACTION_FROM", "VALID_TRANSACTION_TO", "IS_CURRENT", "SNAPSHOT_AT", "REASON",
                 "CHANGED_FEATURE_GROUP", "GROUP_A_UPDATED_AT", "GROUP_B_UPDATED_AT", "FEATURE_UPDATED_AT",
                 "COMPUTED_AT", "INGESTED_AT"]
# Any column whose name contains one of these is treated as leakage too
LEAKAGE_KEYWORDS = ["CNCL", "CANCEL", "NEXT_", "DAYS_TIL"]

# Rows too recent to have a complete label (only when HORIZON_DAYS is set): drop rows with
# DATE_COL > (max date - HORIZON_DAYS), because they haven't had HORIZON_DAYS to cancel yet.
DROP_INCOMPLETE_LABELS = True

# Features to leave out of the model. Calendar features let the model learn "which month the row
# is from" instead of policy behaviour — that doesn't carry over to future months.
DROP_FEATURES = ["FE_TXN_MONTH", "FE_TXN_DAYOFWEEK"]

# How to hold out the test set:
#   "time"   → train on the oldest rows, test on the newest TEST_FRACTION (how the model is used in production)
#   "policy" → random policies, all rows of a policy on one side
SPLIT_MODE = "time"

# Training
SEED          = 42
TEST_FRACTION = 0.2       # ≈ share of policies held out for the final test
CV_FOLDS      = 5
N_TRIALS      = 40        # Optuna trials per model (lower = faster)
CORR_DROP     = 0.98      # drop one of any two features correlated above this
NEAR_CONSTANT = 0.995     # drop a feature if one value covers more than this share of rows

# MLflow
EXPERIMENT_PATH = ""      # blank → /Users/<you>/cancellation_model
UC_MODEL_NAME   = ""      # e.g. "ws_prd_analytics.default.cancellation_model" → registers the best model

assert TABLE_NAME, "Set TABLE_NAME in the config cell before running."

# COMMAND ----------

import re
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import StratifiedGroupKFold, StratifiedKFold, cross_val_score, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import calibration_curve
from sklearn.metrics import (roc_auc_score, average_precision_score, f1_score, precision_score,
                             recall_score, brier_score_loss, log_loss, confusion_matrix,
                             roc_curve, precision_recall_curve)
from lightgbm import LGBMClassifier
from xgboost import XGBClassifier
import optuna
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

try:
    from catboost import CatBoostClassifier
    HAS_CATBOOST = True
except ImportError:
    HAS_CATBOOST = False

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)
sns.set_theme(style="whitegrid")
np.random.seed(SEED)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load data and build the target

# COMMAND ----------

df = spark.table(TABLE_NAME).toPandas()
df.columns = [c.upper() for c in df.columns]
for name in ["TARGET_COL", "DAYS_COL", "POLICY_COL", "DATE_COL", "EFF_COL", "TXN_COL"]:
    globals()[name] = globals()[name].upper()
print(f"Loaded {TABLE_NAME}: {df.shape[0]:,} rows × {df.shape[1]} columns")

# Dates
for c in [DATE_COL, EFF_COL]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

# Target
if TARGET_COL in df.columns:
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce").fillna(0).astype(int)
    print(f"Using existing target column {TARGET_COL}")
elif DAYS_COL in df.columns:
    days = pd.to_numeric(df[DAYS_COL], errors="coerce")
    upper = HORIZON_DAYS if HORIZON_DAYS else 9998
    df[TARGET_COL] = ((days > 0) & (days <= upper)).astype(int)
    print(f"Derived {TARGET_COL} from {DAYS_COL}: 1 if 0 < days <= {upper}")
else:
    raise ValueError(f"Neither {TARGET_COL} nor {DAYS_COL} found in the table.")

# Rows whose label window isn't complete yet
if HORIZON_DAYS and DROP_INCOMPLETE_LABELS and DATE_COL in df.columns:
    cutoff = df[DATE_COL].max() - pd.Timedelta(days=HORIZON_DAYS)
    before = len(df)
    df = df[df[DATE_COL] <= cutoff].copy()
    print(f"Dropped {before - len(df):,} rows after {cutoff.date()} (label window not complete)")

# Duplicate transactions
if TXN_COL in df.columns:
    dups = df.duplicated(subset=[TXN_COL]).sum()
    print(f"Duplicate {TXN_COL}: {dups}")
    if dups:
        df = df.drop_duplicates(subset=[TXN_COL]).copy()

print(f"\nTarget rate: {df[TARGET_COL].mean():.2%}  ({df[TARGET_COL].sum():,} cancellations / {len(df):,} rows)")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Remove columns that can't be features

# COMMAND ----------

def is_leakage(col):
    return col in LEAKAGE_COLS or any(k in col for k in LEAKAGE_KEYWORDS)

excluded = [c for c in df.columns
            if c in ID_COLS or c in METADATA_COLS or c == TARGET_COL or is_leakage(c)]
print("Excluded from features (ids / labels / metadata):")
for c in excluded:
    print(f"  - {c}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. EDA

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.1 Overview

# COMMAND ----------

raw_feature_cols = [c for c in df.columns if c not in excluded and c not in [DATE_COL, EFF_COL]]
print(f"Rows: {len(df):,}   Candidate feature columns: {len(raw_feature_cols)}")
if POLICY_COL in df.columns:
    per_policy = df.groupby(POLICY_COL).size()
    print(f"Policies: {per_policy.size:,}   rows per policy: mean {per_policy.mean():.1f}, max {per_policy.max()}")
if DATE_COL in df.columns:
    print(f"Date range: {df[DATE_COL].min().date()} → {df[DATE_COL].max().date()}")

dtype_summary = df[raw_feature_cols].dtypes.astype(str).value_counts()
display(dtype_summary.rename("columns").to_frame())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.2 Missing values

# COMMAND ----------

missing = (df[raw_feature_cols].isna().mean().sort_values(ascending=False) * 100).round(2)
missing = missing[missing > 0]
print(f"{len(missing)} column(s) have missing values")
if len(missing):
    display(missing.rename("missing_%").to_frame())
    plt.figure(figsize=(10, max(3, len(missing.head(30)) * 0.3)))
    missing.head(30).sort_values().plot.barh(color="#d95f02")
    plt.title("Missing values (%) — top 30")
    plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.3 Target balance and target over time

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(14, 4))
df[TARGET_COL].value_counts().sort_index().plot.bar(ax=axes[0], color=["#1b9e77", "#d95f02"])
axes[0].set_title("Target distribution (0 = stays, 1 = cancels)")
axes[0].set_xlabel(TARGET_COL)

if DATE_COL in df.columns:
    monthly = df.set_index(DATE_COL).resample("M")[TARGET_COL].agg(["mean", "size"])
    ax2 = axes[1]
    ax2.bar(monthly.index, monthly["size"], width=20, color="#cccccc", label="rows")
    ax2.set_ylabel("rows")
    ax3 = ax2.twinx()
    ax3.plot(monthly.index, monthly["mean"], color="#d95f02", marker="o", label="cancel rate")
    ax3.set_ylabel("cancel rate")
    ax2.set_title("Rows and cancel rate by month")
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.4 Numeric summary by target

# COMMAND ----------

num_cols = [c for c in raw_feature_cols if pd.api.types.is_numeric_dtype(df[c])]
summary = df.groupby(TARGET_COL)[num_cols].mean().T
summary.columns = ["mean_target_0", "mean_target_1"]
summary["diff_%"] = ((summary["mean_target_1"] - summary["mean_target_0"])
                     / summary["mean_target_0"].replace(0, np.nan) * 100).round(1)
display(summary.sort_values("diff_%", key=lambda s: s.abs(), ascending=False))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.5 Single-feature strength + leakage scan
# MAGIC
# MAGIC AUC of each feature on its own. **AUC above 0.95 on a single feature is suspicious** — it often
# MAGIC means the column contains the answer (label leakage). Check those before trusting the model.

# COMMAND ----------

def single_feature_auc(x, y):
    x = pd.to_numeric(x, errors="coerce")
    if x.nunique(dropna=True) < 2:
        return np.nan
    x = x.fillna(x.median())
    auc = roc_auc_score(y, x)
    return max(auc, 1 - auc)

feat_auc = pd.Series({c: single_feature_auc(df[c], df[TARGET_COL]) for c in num_cols}).dropna()
feat_auc = feat_auc.sort_values(ascending=False)
display(feat_auc.rename("single_feature_auc").to_frame().head(40))

suspicious = feat_auc[feat_auc > 0.95]
if len(suspicious):
    print("⚠️  Possible leakage — these alone almost predict the target:")
    for c, v in suspicious.items():
        print(f"   {c}: {v:.3f}")
else:
    print("✅ No single feature above 0.95 AUC.")

top = feat_auc.head(20)
plt.figure(figsize=(10, 6))
top.sort_values().plot.barh(color="#7570b3")
plt.axvline(0.5, color="grey", linestyle="--")
plt.title("Top 20 features by single-feature AUC")
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.6 Distributions of the strongest features

# COMMAND ----------

plot_cols = list(feat_auc.head(9).index)
if plot_cols:
    fig, axes = plt.subplots(3, 3, figsize=(15, 11))
    for ax, c in zip(axes.ravel(), plot_cols):
        if df[c].nunique() <= 10:
            rate = df.groupby(c)[TARGET_COL].mean()
            rate.plot.bar(ax=ax, color="#d95f02")
            ax.set_ylabel("cancel rate")
        else:
            sns.kdeplot(data=df, x=c, hue=TARGET_COL, common_norm=False, fill=True, ax=ax, warn_singular=False)
        ax.set_title(c)
    for ax in axes.ravel()[len(plot_cols):]:
        ax.axis("off")
    plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.7 Correlation between features

# COMMAND ----------

corr_cols = list(feat_auc.head(25).index)
if len(corr_cols) > 1:
    plt.figure(figsize=(14, 11))
    sns.heatmap(df[corr_cols].corr(), cmap="RdBu_r", center=0, vmin=-1, vmax=1, square=True,
                cbar_kws={"shrink": 0.6})
    plt.title("Correlation — top 25 features")
    plt.tight_layout(); plt.show()

    corr = df[num_cols].corr().abs()
    pairs = (corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
                 .stack().sort_values(ascending=False))
    display(pairs[pairs > 0.9].rename("abs_corr").to_frame().head(30))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Feature engineering
# MAGIC
# MAGIC Every feature below uses **only the current row and earlier rows of the same policy**, so it
# MAGIC can be computed at scoring time without looking into the future. A feature is only created if
# MAGIC its source columns exist in the table.

# COMMAND ----------

CVG = ["BI", "PD", "MD", "COLL", "COMP", "PIP"]
CHANGE_FLAGS = ([f"{c}_COVERAGE_ADDED" for c in CVG] + [f"{c}_COVERAGE_REMOVED" for c in CVG]
                + ["COMP_DED_CHANGE_IND", "COLL_DED_CHANGE_IND", "GNDR_CHANGE_IND", "MRTL_CHANGE_IND"])


def _has(df, *cols):
    return all(c in df.columns for c in cols)


def add_domain_features(df):
    df = df.copy()
    added   = [f"{c}_COVERAGE_ADDED" for c in CVG if f"{c}_COVERAGE_ADDED" in df.columns]
    removed = [f"{c}_COVERAGE_REMOVED" for c in CVG if f"{c}_COVERAGE_REMOVED" in df.columns]
    if added:
        df["FE_CVG_ADDED_CNT"] = df[added].sum(axis=1)
    if removed:
        df["FE_CVG_REMOVED_CNT"] = df[removed].sum(axis=1)
    if added and removed:
        df["FE_CVG_NET_CHANGE"] = df["FE_CVG_ADDED_CNT"] - df["FE_CVG_REMOVED_CNT"]
        df["FE_ANY_CVG_CHANGE"] = ((df["FE_CVG_ADDED_CNT"] + df["FE_CVG_REMOVED_CNT"]) > 0).astype(int)
    if _has(df, "COMP_DED_CHANGE_IND", "COLL_DED_CHANGE_IND"):
        df["FE_ANY_DED_CHANGE"] = ((df["COMP_DED_CHANGE_IND"] + df["COLL_DED_CHANGE_IND"]) > 0).astype(int)
    if _has(df, "GNDR_CHANGE_IND", "MRTL_CHANGE_IND"):
        df["FE_ANY_DRIVER_ATTR_CHANGE"] = ((df["GNDR_CHANGE_IND"] + df["MRTL_CHANGE_IND"]) > 0).astype(int)
    flags = [c for c in CHANGE_FLAGS if c in df.columns]
    if flags:
        df["FE_TOTAL_CHANGES"] = df[flags].sum(axis=1)

    if _has(df, "MAX_DRVR_AGE", "MIN_DRVR_AGE"):
        df["FE_DRVR_AGE_RANGE"] = df["MAX_DRVR_AGE"] - df["MIN_DRVR_AGE"]
    if "MIN_DRVR_AGE" in df.columns:
        df["FE_HAS_YOUNG_DRIVER"] = (df["MIN_DRVR_AGE"] < 25).astype(int)
    if "MAX_DRVR_AGE" in df.columns:
        df["FE_HAS_SENIOR_DRIVER"] = (df["MAX_DRVR_AGE"] >= 70).astype(int)
    if _has(df, "VEH_CNT", "DRVR_CNT"):
        df["FE_VEH_PER_DRIVER"] = df["VEH_CNT"] / df["DRVR_CNT"].replace(0, np.nan)
    if "AVG_VEH_AGE" in df.columns:
        df["FE_OLD_VEHICLES"] = (df["AVG_VEH_AGE"] >= 10).astype(int)

    if "PREM_CHANGE_AMT" in df.columns:
        p = df["PREM_CHANGE_AMT"]
        df["FE_PREM_INCREASE"] = (p > 0).astype(int)
        df["FE_PREM_DECREASE"] = (p < 0).astype(int)
        df["FE_PREM_CHANGE_LOG"] = np.sign(p) * np.log1p(p.abs())

    if _has(df, "TXN_CNT_1D", "TXN_CNT_1M"):
        df["FE_TXN_1D_SHARE_OF_1M"] = df["TXN_CNT_1D"] / df["TXN_CNT_1M"].replace(0, np.nan)
    if _has(df, "TXN_CNT_1W", "TXN_CNT_1M"):
        df["FE_TXN_1W_SHARE_OF_1M"] = df["TXN_CNT_1W"] / df["TXN_CNT_1M"].replace(0, np.nan)
    if "TENURE_YRS" in df.columns:
        df["FE_NEW_POLICY"] = (df["TENURE_YRS"] < 1).astype(int)

    if DATE_COL in df.columns:
        df["FE_TXN_MONTH"] = df[DATE_COL].dt.month
        df["FE_TXN_DAYOFWEEK"] = df[DATE_COL].dt.dayofweek
    if _has(df, DATE_COL, EFF_COL):
        df["FE_DAYS_TO_EFFECTIVE"] = (df[EFF_COL] - df[DATE_COL]).dt.days
    return df


def add_sequence_features(df):
    """Per-policy history features, using only the current and earlier transactions."""
    if not _has(df, POLICY_COL, DATE_COL):
        return df
    order = [POLICY_COL, DATE_COL] + ([TXN_COL] if TXN_COL in df.columns else [])
    df = df.sort_values(order).copy()
    g = df.groupby(POLICY_COL, sort=False)

    df["FE_TXN_SEQ_NO"] = g.cumcount() + 1
    df["FE_DAYS_SINCE_PREV_TXN"] = g[DATE_COL].diff().dt.days
    df["FE_DAYS_SINCE_FIRST_TXN"] = (df[DATE_COL] - g[DATE_COL].transform("min")).dt.days

    for c in ["FE_TOTAL_CHANGES", "FE_CVG_REMOVED_CNT", "FE_CVG_ADDED_CNT", "FE_ANY_DED_CHANGE",
              "VEH_ADDED_LATEST_TXN", "DRVRS_ADDED_LATEST_TXN"]:
        if c in df.columns:
            df[f"FE_CUM_{c.replace('FE_', '')}"] = g[c].cumsum()
    for c in ["VEH_CNT", "DRVR_CNT"]:
        if c in df.columns:
            df[f"FE_{c}_DELTA"] = g[c].diff()
    if "PREM_CHANGE_AMT" in df.columns:
        df["FE_CUM_PREM_CHANGE"] = g["PREM_CHANGE_AMT"].cumsum()
        df["_inc"] = (df["PREM_CHANGE_AMT"] > 0).astype(int)
        df["FE_PREM_INCREASES_SO_FAR"] = df.groupby(POLICY_COL, sort=False)["_inc"].cumsum()
        df = df.drop(columns="_inc")
    if "FE_TXN_SEQ_NO" in df.columns and "FE_DAYS_SINCE_FIRST_TXN" in df.columns:
        df["FE_TXN_PER_30D"] = df["FE_TXN_SEQ_NO"] / (df["FE_DAYS_SINCE_FIRST_TXN"] / 30 + 1)
    return df


def encode_categoricals(df, feature_cols, max_levels=20):
    """One-hot low-cardinality text columns, drop high-cardinality ones."""
    df = df.copy()
    cat_cols = [c for c in feature_cols if not pd.api.types.is_numeric_dtype(df[c])]
    keep, drop = [], []
    for c in cat_cols:
        (keep if df[c].nunique(dropna=True) <= max_levels else drop).append(c)
    if keep:
        df = pd.get_dummies(df, columns=keep, prefix=keep, dummy_na=True, dtype=int)
    if drop:
        df = df.drop(columns=drop)
    return df, keep, drop


def sanitize(name):
    return re.sub(r"[^0-9a-zA-Z_]", "_", name)


df_fe = add_sequence_features(add_domain_features(df))
base_features = [c for c in df_fe.columns if c not in excluded and c not in [DATE_COL, EFF_COL]]
df_fe, onehot_cols, dropped_text = encode_categoricals(df_fe, base_features)
df_fe.columns = [sanitize(c) for c in df_fe.columns]

feature_cols = [c for c in df_fe.columns
                if c not in {sanitize(x) for x in excluded} | {sanitize(DATE_COL), sanitize(EFF_COL)}
                and c not in {sanitize(x) for x in DROP_FEATURES}]
new_features = [c for c in feature_cols if c.startswith("FE_")]
print(f"Engineered {len(new_features)} new features; total features: {len(feature_cols)}")
if onehot_cols:
    print(f"One-hot encoded: {onehot_cols}")
if dropped_text:
    print(f"Dropped high-cardinality text columns: {dropped_text}")
display(pd.DataFrame({"new_feature": new_features}))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Train / test split — by policy
# MAGIC
# MAGIC All rows of a policy go to **either** train **or** test. Otherwise the model can memorise a
# MAGIC policy from its earlier rows and look better on test than it really is.

# COMMAND ----------

y_all = df_fe[TARGET_COL].astype(int).values
groups_all = df_fe[sanitize(POLICY_COL)].values if sanitize(POLICY_COL) in df_fe.columns else None

n_holdout_splits = max(2, int(round(1 / TEST_FRACTION)))
if SPLIT_MODE == "time" and sanitize(DATE_COL) in df_fe.columns:
    # Out-of-time: everything after the cutoff date is test
    dates = df_fe[sanitize(DATE_COL)]
    split_date = dates.quantile(1 - TEST_FRACTION)
    train_idx = np.where(dates <= split_date)[0]
    test_idx = np.where(dates > split_date)[0]
    print(f"Out-of-time split: train ≤ {split_date.date()} < test")
elif groups_all is not None:
    splitter = StratifiedGroupKFold(n_splits=n_holdout_splits, shuffle=True, random_state=SEED)
    train_idx, test_idx = next(splitter.split(df_fe, y_all, groups_all))
else:
    splitter = StratifiedKFold(n_splits=n_holdout_splits, shuffle=True, random_state=SEED)
    train_idx, test_idx = next(splitter.split(df_fe, y_all))

train_df, test_df = df_fe.iloc[train_idx].copy(), df_fe.iloc[test_idx].copy()
y_train, y_test = y_all[train_idx], y_all[test_idx]
groups_train = groups_all[train_idx] if groups_all is not None else None

print(f"Train: {len(train_df):,} rows, cancel rate {y_train.mean():.2%} ({y_train.sum()} cancels)")
print(f"Test : {len(test_df):,} rows, cancel rate {y_test.mean():.2%} ({y_test.sum()} cancels)")
if y_test.sum() < 10:
    print("⚠️ Fewer than 10 cancellations in test — test metrics will be very noisy.")
if groups_all is not None:
    overlap = set(groups_all[train_idx]) & set(groups_all[test_idx])
    if SPLIT_MODE == "time":
        # Expected: a policy's older row is in train, its newer row in test (same as production)
        print(f"Policies with rows in both train and test: {len(overlap)} (OK for a time split)")
    else:
        print(f"Policies in both train and test: {len(overlap)} (must be 0)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Feature selection (fitted on train only)

# COMMAND ----------

X_train = train_df[feature_cols].astype(float)
X_test  = test_df[feature_cols].astype(float)

# Constant / near-constant
top_share = X_train.apply(lambda s: s.value_counts(normalize=True, dropna=False).iloc[0])
near_constant = list(top_share[top_share > NEAR_CONSTANT].index)

# Highly correlated (keep the one with the higher single-feature AUC)
remaining = [c for c in feature_cols if c not in near_constant]
train_auc = pd.Series({c: single_feature_auc(X_train[c], y_train) for c in remaining}).fillna(0.5)
corr = X_train[remaining].corr().abs()
to_drop = set()
for i, a in enumerate(remaining):
    for b in remaining[i + 1:]:
        if a in to_drop or b in to_drop:
            continue
        if corr.loc[a, b] > CORR_DROP:
            to_drop.add(b if train_auc[a] >= train_auc[b] else a)

selected = [c for c in remaining if c not in to_drop]
print(f"Dropped {len(near_constant)} near-constant and {len(to_drop)} highly correlated features")
print(f"Features used for modelling: {len(selected)}")

X_train, X_test = X_train[selected], X_test[selected]
neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
POS_WEIGHT = neg / max(pos, 1)
print(f"Class ratio (neg/pos) in train: {POS_WEIGHT:.2f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Hyperparameter tuning (Optuna, policy-grouped CV, PR-AUC)

# COMMAND ----------

if groups_train is not None:
    cv = StratifiedGroupKFold(n_splits=CV_FOLDS, shuffle=True, random_state=SEED)
else:
    cv = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=SEED)


def cv_pr_auc(model):
    scores = cross_val_score(model, X_train, y_train, groups=groups_train, cv=cv,
                             scoring="average_precision", n_jobs=1)
    return scores.mean()


def build_lgbm(p):
    return LGBMClassifier(objective="binary", random_state=SEED, verbose=-1, n_jobs=-1, subsample_freq=1, **p)


def space_lgbm(t):
    return dict(
        # Small trees + bigger leaves: the training set is only a few hundred rows
        n_estimators=t.suggest_int("n_estimators", 100, 1000, step=50),
        learning_rate=t.suggest_float("learning_rate", 0.01, 0.2, log=True),
        num_leaves=t.suggest_int("num_leaves", 4, 31, log=True),
        max_depth=t.suggest_int("max_depth", 2, 5),
        min_child_samples=t.suggest_int("min_child_samples", 20, 100),
        subsample=t.suggest_float("subsample", 0.5, 1.0),
        colsample_bytree=t.suggest_float("colsample_bytree", 0.4, 1.0),
        reg_alpha=t.suggest_float("reg_alpha", 1e-8, 10, log=True),
        reg_lambda=t.suggest_float("reg_lambda", 1e-8, 10, log=True),
        scale_pos_weight=t.suggest_float("scale_pos_weight", 1.0, max(1.0, POS_WEIGHT * 1.5)),
    )


def build_xgb(p):
    return XGBClassifier(objective="binary:logistic", eval_metric="aucpr", tree_method="hist",
                         random_state=SEED, n_jobs=-1, **p)


def space_xgb(t):
    return dict(
        n_estimators=t.suggest_int("n_estimators", 100, 1000, step=50),
        learning_rate=t.suggest_float("learning_rate", 0.01, 0.2, log=True),
        max_depth=t.suggest_int("max_depth", 2, 5),
        min_child_weight=t.suggest_float("min_child_weight", 3, 20, log=True),
        subsample=t.suggest_float("subsample", 0.5, 1.0),
        colsample_bytree=t.suggest_float("colsample_bytree", 0.4, 1.0),
        gamma=t.suggest_float("gamma", 0, 5),
        reg_alpha=t.suggest_float("reg_alpha", 1e-8, 10, log=True),
        reg_lambda=t.suggest_float("reg_lambda", 1e-8, 10, log=True),
        scale_pos_weight=t.suggest_float("scale_pos_weight", 1.0, max(1.0, POS_WEIGHT * 1.5)),
    )


def build_cat(p):
    p = dict(p)
    if p.get("auto_class_weights") == "None":      # Optuna stores the choice as text; CatBoost wants it absent
        p.pop("auto_class_weights")
    return CatBoostClassifier(random_seed=SEED, verbose=0, eval_metric="PRAUC", **p)


def space_cat(t):
    return dict(
        iterations=t.suggest_int("iterations", 200, 1000, step=100),
        learning_rate=t.suggest_float("learning_rate", 0.01, 0.2, log=True),
        depth=t.suggest_int("depth", 2, 5),
        l2_leaf_reg=t.suggest_float("l2_leaf_reg", 1, 10, log=True),
        auto_class_weights=t.suggest_categorical("auto_class_weights", ["None", "Balanced"]),
    )


def build_rf(p):
    return Pipeline([("impute", SimpleImputer(strategy="median")),
                     ("model", RandomForestClassifier(random_state=SEED, n_jobs=-1, **p))])


def space_rf(t):
    return dict(
        n_estimators=t.suggest_int("n_estimators", 200, 800, step=100),
        max_depth=t.suggest_int("max_depth", 3, 8),
        min_samples_leaf=t.suggest_int("min_samples_leaf", 5, 30),
        max_features=t.suggest_categorical("max_features", ["sqrt", 0.3, 0.5]),
        class_weight=t.suggest_categorical("class_weight", ["balanced", "balanced_subsample", None]),
    )


def build_lr(p):
    return Pipeline([("impute", SimpleImputer(strategy="median")),
                     ("scale", StandardScaler()),
                     ("model", LogisticRegression(solver="liblinear", max_iter=2000,
                                                  class_weight="balanced", random_state=SEED, **p))])


def space_lr(t):
    return dict(
        C=t.suggest_float("C", 1e-3, 10, log=True),
        penalty=t.suggest_categorical("penalty", ["l1", "l2"]),
    )


MODELS = {
    "LightGBM":           (space_lgbm, build_lgbm),
    "XGBoost":            (space_xgb,  build_xgb),
    "RandomForest":       (space_rf,   build_rf),
    "LogisticRegression": (space_lr,   build_lr),
}
if HAS_CATBOOST:
    MODELS["CatBoost"] = (space_cat, build_cat)

print("Models to tune:", list(MODELS))

# COMMAND ----------

tuning = {}
for name, (space, build) in MODELS.items():
    trials = N_TRIALS if name != "LogisticRegression" else min(N_TRIALS, 20)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=SEED),
                                study_name=name)
    study.optimize(lambda t: cv_pr_auc(build(space(t))), n_trials=trials, show_progress_bar=False)
    tuning[name] = study
    print(f"{name:20s} best CV PR-AUC = {study.best_value:.4f}  ({trials} trials)")

cv_table = pd.DataFrame({n: {"cv_pr_auc": s.best_value} for n, s in tuning.items()}).T
display(cv_table.sort_values("cv_pr_auc", ascending=False))

# COMMAND ----------

best_name = cv_table["cv_pr_auc"].idxmax()
print(f"Best model by CV PR-AUC: {best_name}")
display(pd.Series(tuning[best_name].best_params).rename("value").to_frame())

try:
    optuna.visualization.matplotlib.plot_optimization_history(tuning[best_name]); plt.show()
    optuna.visualization.matplotlib.plot_param_importances(tuning[best_name]); plt.show()
except Exception as e:
    print(f"(Optuna plots skipped: {e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Fit the tuned models, choose a threshold, evaluate on the hold-out test set
# MAGIC
# MAGIC The decision threshold is chosen on **out-of-fold predictions from train** (max F1), never on test.

# COMMAND ----------

def best_threshold(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec * rec / np.clip(prec + rec, 1e-12, None)
    i = np.nanargmax(f1[:-1])
    return float(thr[i])


def evaluate(y, p, thr):
    pred = (p >= thr).astype(int)
    return {
        "roc_auc":   roc_auc_score(y, p),
        "pr_auc":    average_precision_score(y, p),
        "f1":        f1_score(y, pred),
        "precision": precision_score(y, pred, zero_division=0),
        "recall":    recall_score(y, pred),
        "brier":     brier_score_loss(y, p),
        "log_loss":  log_loss(y, np.clip(p, 1e-6, 1 - 1e-6)),
        "threshold": thr,
    }


fitted, results, test_probs = {}, {}, {}
for name, (space, build) in MODELS.items():
    params = tuning[name].best_params
    oof = cross_val_predict(build(params), X_train, y_train, groups=groups_train, cv=cv,
                            method="predict_proba")[:, 1]
    thr = best_threshold(y_train, oof)
    model = build(params).fit(X_train, y_train)
    p_test = model.predict_proba(X_test)[:, 1]
    fitted[name], test_probs[name] = model, p_test
    results[name] = {"cv_pr_auc": tuning[name].best_value, **evaluate(y_test, p_test, thr)}

results_df = pd.DataFrame(results).T.sort_values("cv_pr_auc", ascending=False)
display(results_df.round(4))
print(f"Baseline PR-AUC (random) = test cancel rate = {y_test.mean():.4f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 7.1 ROC and precision-recall curves (test)

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for name, p in test_probs.items():
    fpr, tpr, _ = roc_curve(y_test, p)
    axes[0].plot(fpr, tpr, label=f"{name} ({roc_auc_score(y_test, p):.3f})")
    prec, rec, _ = precision_recall_curve(y_test, p)
    axes[1].plot(rec, prec, label=f"{name} ({average_precision_score(y_test, p):.3f})")
axes[0].plot([0, 1], [0, 1], "k--", alpha=0.4)
axes[0].set(title="ROC curve", xlabel="False positive rate", ylabel="True positive rate")
axes[1].axhline(y_test.mean(), color="k", linestyle="--", alpha=0.4, label="random")
axes[1].set(title="Precision-recall curve", xlabel="Recall", ylabel="Precision")
for ax in axes:
    ax.legend(loc="best", fontsize=9)
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 7.2 Best model — confusion matrix, lift by decile, calibration

# COMMAND ----------

best_model = fitted[best_name]
p_best = test_probs[best_name]
thr_best = results[best_name]["threshold"]

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

cm = confusion_matrix(y_test, (p_best >= thr_best).astype(int))
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False, ax=axes[0],
            xticklabels=["pred stays", "pred cancels"], yticklabels=["stays", "cancels"])
axes[0].set_title(f"{best_name} — confusion matrix (threshold {thr_best:.2f})")

lift = pd.DataFrame({"y": y_test, "p": p_best})
lift["decile"] = pd.qcut(lift["p"].rank(method="first", ascending=False), 10, labels=range(1, 11))
lift_table = lift.groupby("decile", observed=True).agg(rows=("y", "size"), cancels=("y", "sum"), cancel_rate=("y", "mean"))
lift_table["lift"] = lift_table["cancel_rate"] / y_test.mean()
lift_table["cum_capture_%"] = (lift_table["cancels"].cumsum() / lift_table["cancels"].sum() * 100).round(1)
axes[1].bar(lift_table.index.astype(str), lift_table["lift"], color="#d95f02")
axes[1].axhline(1, color="k", linestyle="--", alpha=0.4)
axes[1].set(title="Lift by risk decile (1 = highest risk)", xlabel="decile", ylabel="lift")

frac_pos, mean_pred = calibration_curve(y_test, p_best, n_bins=10, strategy="quantile")
axes[2].plot(mean_pred, frac_pos, marker="o", label=best_name)
axes[2].plot([0, 1], [0, 1], "k--", alpha=0.4, label="perfect")
axes[2].set(title="Calibration", xlabel="predicted probability", ylabel="observed cancel rate")
axes[2].legend()
plt.tight_layout(); plt.show()

display(lift_table.round(3))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Explainability

# COMMAND ----------

def tree_model(m):
    return m.named_steps["model"] if isinstance(m, Pipeline) else m

inner = tree_model(best_model)
if hasattr(inner, "feature_importances_"):
    imp = pd.Series(inner.feature_importances_, index=selected).sort_values(ascending=False)
elif hasattr(inner, "coef_"):
    imp = pd.Series(np.abs(inner.coef_[0]), index=selected).sort_values(ascending=False)
else:
    imp = pd.Series(dtype=float)

if len(imp):
    plt.figure(figsize=(10, 8))
    imp.head(25).sort_values().plot.barh(color="#1b9e77")
    plt.title(f"{best_name} — top 25 feature importances")
    plt.tight_layout(); plt.show()

# COMMAND ----------

try:
    import shap
    sample = X_test.sample(min(len(X_test), 1000), random_state=SEED)
    if best_name in ("LightGBM", "XGBoost", "CatBoost", "RandomForest"):
        X_shap = sample if best_name != "RandomForest" else pd.DataFrame(
            best_model.named_steps["impute"].transform(sample), columns=selected, index=sample.index)
        explainer = shap.TreeExplainer(inner)
        sv = explainer.shap_values(X_shap)
        if isinstance(sv, list):
            sv = sv[1]
        if sv.ndim == 3:
            sv = sv[:, :, 1]
        shap.summary_plot(sv, X_shap, max_display=20, show=False)
        plt.title(f"{best_name} — SHAP summary"); plt.tight_layout(); plt.show()
    else:
        print("SHAP summary skipped for a linear model — see coefficients above.")
except Exception as e:
    print(f"SHAP skipped: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Log everything to MLflow

# COMMAND ----------

if not EXPERIMENT_PATH:
    me = spark.sql("SELECT current_user()").first()[0]
    EXPERIMENT_PATH = f"/Users/{me}/cancellation_model"
mlflow.set_experiment(EXPERIMENT_PATH)

for name, model in fitted.items():
    with mlflow.start_run(run_name=name) as run:
        mlflow.log_params({f"hp_{k}": v for k, v in tuning[name].best_params.items()})
        mlflow.log_params({"table": TABLE_NAME, "n_features": len(selected), "cv_folds": CV_FOLDS,
                           "n_trials": N_TRIALS, "horizon_days": HORIZON_DAYS})
        mlflow.log_metrics({f"test_{k}": float(v) for k, v in results[name].items() if k != "cv_pr_auc"})
        mlflow.log_metric("cv_pr_auc", float(results[name]["cv_pr_auc"]))
        mlflow.log_dict({"features": selected, "threshold": results[name]["threshold"]}, "model_config.json")
        sig = infer_signature(X_train.head(50), model.predict_proba(X_train.head(50))[:, 1])
        mlflow.sklearn.log_model(model, artifact_path="model", signature=sig,
                                 input_example=X_train.head(5))
        if name == best_name:
            best_run_id = run.info.run_id
    print(f"Logged {name}")

print(f"\nExperiment: {EXPERIMENT_PATH}\nBest model run: {best_run_id} ({best_name})")

if UC_MODEL_NAME:
    mlflow.set_registry_uri("databricks-uc")
    mv = mlflow.register_model(f"runs:/{best_run_id}/model", UC_MODEL_NAME)
    print(f"Registered {UC_MODEL_NAME} version {mv.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Scoring helper
# MAGIC
# MAGIC Applies the same feature engineering and the chosen threshold to new rows. The input must have
# MAGIC the same raw columns as the training table (and, for the sequence features, the policy's earlier
# MAGIC transactions — pass the policy's history, then keep the latest row's score).

# COMMAND ----------

def score(new_pdf, model=best_model, threshold=thr_best):
    new_pdf = new_pdf.copy()
    new_pdf.columns = [c.upper() for c in new_pdf.columns]
    for c in [DATE_COL, EFF_COL]:
        if c in new_pdf.columns:
            new_pdf[c] = pd.to_datetime(new_pdf[c], errors="coerce")
    fe = add_sequence_features(add_domain_features(new_pdf))
    fe, _, _ = encode_categoricals(fe, [c for c in fe.columns if c not in excluded and c not in [DATE_COL, EFF_COL]])
    fe.columns = [sanitize(c) for c in fe.columns]
    X = fe.reindex(columns=selected).astype(float)          # missing one-hot columns → NaN
    out = fe[[c for c in [sanitize(POLICY_COL), sanitize(TXN_COL)] if c in fe.columns]].copy()
    out["cancel_probability"] = model.predict_proba(X)[:, 1]
    out["cancel_flag"] = (out["cancel_probability"] >= threshold).astype(int)
    return out

preview = score(df.head(20))
display(preview)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Table           : {TABLE_NAME}")
print(f"Rows / policies : {len(df):,} / {df[POLICY_COL].nunique() if POLICY_COL in df.columns else 'n/a'}")
print(f"Cancel rate     : {df[TARGET_COL].mean():.2%}")
print(f"Features used   : {len(selected)} ({len(new_features)} engineered)")
print(f"Best model      : {best_name}")
for k in ["roc_auc", "pr_auc", "f1", "precision", "recall", "threshold"]:
    print(f"  test {k:10s}: {results[best_name][k]:.4f}")
print(f"Top-decile lift : {lift_table['lift'].iloc[0]:.2f}x  "
      f"(captures {lift_table['cum_capture_%'].iloc[0]}% of cancellations)")
