# Pipeline Walkthrough — Notebooks 01 to 08, Step by Step

A reference for the **optimized** pipeline (the version with `AUTO_POLICY_SNAPSHOT_STATE`).
Every notebook is broken into steps: what the code does, the key lines, and what the data looks
like afterwards. **One running example is used throughout**, so you can follow a single policy
from raw JSON to the online endpoint.

---

## Contents
- [The running example](#the-running-example)
- [Pipeline at a glance](#pipeline-at-a-glance)
- [01 — Setup Tables](#01--setup-tables)
- [02 — Seed Data](#02--seed-data)
- [03 — Auto Loader Landing Zone](#03--auto-loader-landing-zone)
- [Bulk Backfill from CSV (alternative to 03)](#bulk-backfill-from-csv-alternative-to-03)
- [04 — Compute Features](#04--compute-features)
- [05 — Routing Pipeline](#05--routing-pipeline)
- [06 — Daily Pending Promotion](#06--daily-pending-promotion)
- [07 — Daily GROUP_A Refresh](#07--daily-group_a-refresh)
- [08 — Online Feature Store](#08--online-feature-store)
- [Run order and checks](#run-order-and-checks)
- [Known limitations](#known-limitations)

---

## The running example

**Policy `P1001`** (contract `C2001`, household `H3001`). Its transactions:

| Txn | SRC_TRANS_TMSP (entered) | EFF_DT (takes effect) | What happened |
|---|---|---|---|
| T1 | 2024-01-10 09:00 | 2024-01-10 | New business: car **A** (2016), driver **D1** (born 1985-01-15, M, S); BI, PD and COMP (deductible 500) on car A |
| T2 | 2025-02-05 09:00 | 2025-02-05 | Car **B** (2021) added, with BI and PD |
| T3 | 2026-09-06 09:00 | 2026-09-06 | COMP added on car B (deductible 500) |
| **T4** | **2026-10-04 15:00** | **2026-10-04** | Driver **D2** added (born 1990-01-15, F, M); COMP deductible on car A changed 500 → 1000 |
| **T5** | **2026-10-05 09:00** | **2026-10-17** (future) | Car **A removed** (with its coverages) |

- **T1–T3 were processed in earlier runs**, so the state table holds T3's snapshot.
- **T4 and T5 arrive in today's batch.** Today = **2026-10-05, 10:00**.
- **06** runs on 2026-10-17 (when T5 becomes effective). **07** runs every night.

Coverage keys used below (`vin|cvg_typ_cd`): BI = `13023`, PD = `13024`, COMP = `21000`.

---

## Pipeline at a glance

```
JSON files ─► 03 Auto Loader ─┐
CSV export ─► bulk backfill ──┴─► RAW_POLICY_EVENTS (Bronze) ──CDF──► 04 Compute ─► COMPUTED_FEATURE_EVENTS (Silver)
                                   RAW_EVENTS_DLQ (bad rows)            ▲   │                     │
                                                                        └───┘                   CDF
                                                     AUTO_POLICY_SNAPSHOT_STATE                   ▼
                                                     (last snapshot per policy)            05 Routing ◄── FEATURE_GROUP_REGISTRY
                                                                                     ┌──────┼──────────┐
                                                                                     ▼      ▼          ▼
                                                                                  ACTIVE  HISTORY   PENDING
                                                                                     ▲      ▲          │
                                                       07 daily GROUP_A refresh ─────┤      │          │
                                                       06 daily promotion ◄──────────┴──────┴──────────┘
                                                                                     │
                                                                                     ▼
                                                                    08 Online store + Feature Serving endpoint
```

**Two feature groups** (from `FEATURE_GROUP_REGISTRY`):

| Group | Features | Applies | Date column |
|---|---|---|---|
| **GROUP_A** (5) | `TXN_CNT_1D/1W/1M`, `OUTSTANDING_TXN_IND`, `TENURE_YRS` | **Immediately** | `SRC_TRANS_TMSP` |
| **GROUP_B** (25) | Vehicles, drivers, coverages, deductibles, premium change | **On `EFF_DT`** (may be future) | `EFF_DT` |

---

## 01 — Setup Tables

**Purpose:** create every table and volume the pipeline needs. Run once (or to reset).

### Step 1 — Widgets
```python
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema",  "featurestore_test")
dbutils.widgets.dropdown("reset", "true", ["false", "true"])
```
- `catalog` / `schema` → where the tables go (`ws_prd_analytics` / `featurestore_test` in prod).
- `reset = true` → **drops all 8 tables and the checkpoints volume** first. ⚠️ The default is `true`;
  set it to `false` once data is loaded.

### Step 2 — Feature column lists
```python
GROUP_A_COLUMNS_DDL = """ TXN_CNT_1D INT, TXN_CNT_1W INT, TXN_CNT_1M INT, OUTSTANDING_TXN_IND INT, TENURE_YRS INT """
GROUP_B_COLUMNS_DDL = """ VEH_CNT INT, AVG_VEH_AGE DOUBLE, ... MRTL_CHANGE_IND INT """
```
Defined **once** and reused in every table, so `COMPUTED`, `ACTIVE`, `PENDING` and `HISTORY` always
have the same feature columns.

### Step 3 — Pre-flight checks
Checks the runtime, Unity Catalog, the catalog and schema, write permission, CDF support, volume
support and Liquid Clustering. Any ❌ stops the notebook.

**Output:**
```
✅  CHECK 2 — Unity Catalog       : Enabled
✅  CHECK 3 — Catalog 'ws_prd_analytics' : Exists
...
✅  CHECK 8 — Liquid Clustering   : Supported
  RESULT: ✅  ALL CHECKS PASSED — proceeding
```
(On Serverless, CHECK 1 may show ⚠️ "Could not determine DBR version" — harmless.)

### Step 4 — Volumes
| Volume | Used for |
|---|---|
| `checkpoints` | Stream progress for 03, 04, 05 |
| `incoming_json` | Where JSON files / the CSV are uploaded |

### Step 5 — Tables
| Table | Layer | Key settings | One row per |
|---|---|---|---|
| `RAW_POLICY_EVENTS` | Bronze | **append-only**, **CDF on** | Transaction |
| `RAW_EVENTS_DLQ` | Bronze | — | Rejected record |
| `AUTO_POLICY_SNAPSHOT_STATE` | State | PK, **CLUSTER BY (AUTO_POLICY_ID)** | Policy |
| `COMPUTED_FEATURE_EVENTS` | Silver | **CDF on** | Transaction |
| `AUTO_FEATURES_ACTIVE` | Feature store | PK, **CDF on** (needed by 08) | Policy |
| `AUTO_FEATURES_PENDING` | Feature store | — | Future-dated GROUP_B change |
| `AUTO_FEATURES_HISTORY` | Feature store | — | Version (SCD2) |
| `FEATURE_GROUP_REGISTRY` | Feature store | PK | Feature group |

### Step 6 — Registry rows
**Output:**
| FEATURE_GROUP | OWNED_COLUMNS | ROUTING_TYPE | EFFECTIVE_DATE_COL |
|---|---|---|---|
| GROUP_A | [5 columns] | immediate | SRC_TRANS_TMSP |
| GROUP_B | [25 columns] | effective_date | EFF_DT |

### Step 7 — Verify
```
RAW_POLICY_EVENTS          : 0 rows
...
FEATURE_GROUP_REGISTRY     : 2 rows
```

---

## 02 — Seed Data

**Purpose:** optional demo data — one ready-made policy (`503203043`) so ACTIVE/HISTORY aren't empty.
Not part of the `P1001` example. ⚠️ Widget defaults are `main` / `final_database` — change them.

| Step | Table | What is inserted |
|---|---|---|
| 1 | `AUTO_FEATURES_ACTIVE` | `503203043`: VEH_CNT=2, DRVR_CNT=2, other features 0 |
| 2 | `AUTO_FEATURES_HISTORY` | Same values, `IS_CURRENT=true`, `VALID_TRANSACTION_FROM=2026-07-01` |
| 3 | `AUTO_POLICY_SNAPSHOT_STATE` | Matching **placeholder** sets (`SEED-VIN-1`, `SEED-DRVR-1`, ...) |

Each step is skipped if the row already exists (safe to rerun).

⚠️ Because the state holds placeholder VINs/drivers, the **first real transaction** for `503203043`
will show all its vehicles, drivers and coverages as "added". Fine for a demo; don't use 02 in production.

---

## 03 — Auto Loader Landing Zone

**Purpose:** load **one JSON file per transaction** from `incoming_json` into `RAW_POLICY_EVENTS`.

### Step 1 — Widgets and paths
| Variable | Value |
|---|---|
| `LANDING_VOLUME` | `/Volumes/<catalog>/<schema>/incoming_json` — where to look |
| `CHECKPOINT_PATH` | `/Volumes/<catalog>/<schema>/checkpoints/autoloader_landing` — which files are already done |

### Step 2 — The stream
```python
spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "binaryFile")       # each file = raw bytes
     .option("pathGlobFilter", "*.json")              # only .json files
     .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema")
     .load(LANDING_VOLUME)
     .writeStream.foreachBatch(process_landing_batch)
     .option("checkpointLocation", CHECKPOINT_PATH)
     .trigger(availableNow=True).start()
```
- The checkpoint remembers **file names** already loaded (Auto Loader keeps this list internally in a
  small RocksDB inside `checkpoints/autoloader_landing/sources/`). Each file is loaded once.
- `availableNow` → process all new files, then stop ("Stream stopped..." is normal).

### Step 3 — Per file (`process_landing_batch`)
1. Decode the bytes → `json.loads` → dict.
2. Check `auto_plcy_trans_sk`, `plcy_id_sk`, `eff_dt`, `src_trans_tmsp` are present and `1_vehicle_snapshot` is a non-empty list.
3. Skip if the `TRANS_ID` is already in RAW.
4. `INSERT` one row into `RAW_POLICY_EVENTS` (full JSON kept as `RAW_PAYLOAD`).
5. On any error → one row into `RAW_EVENTS_DLQ` with the file path and error message.

### Output for the example
Files `P1001_T4.json`, `P1001_T5.json` uploaded → **`RAW_POLICY_EVENTS`**:
| TRANS_ID | AUTO_POLICY_ID | EFF_DT | SRC_TRANS_TMSP | RAW_PAYLOAD | INGESTED_AT |
|---|---|---|---|---|---|
| T4 | P1001 | 2026-10-04 00:00 | 2026-10-04 15:00 | `{"1_vehicle_snapshot":[...A,B...], ...}` | 2026-10-05 09:55 |
| T5 | P1001 | 2026-10-17 00:00 | 2026-10-05 09:00 | `{"1_vehicle_snapshot":[...B...], ...}` | 2026-10-05 09:55 |

⚠️ A file name is only loaded once. If a file went to the DLQ, fix it and upload it **under a new name**.

---

## Bulk Backfill from CSV (alternative to 03)

**Purpose:** load **many transactions from one CSV** in a single write (e.g. a Snowflake export).
After it, **skip 03** and run 04 → 05.

**Expected columns:**
`SRC_HH_NUM | PLCY_CNTRCT_NUM | PLCY_ID_SK | SRC_TRANS_TMSP | EFF_DT | AUTO_PLCY_TRANS_SK | JSON_FIELD`

| Step | Code (key line) | What it does |
|---|---|---|
| 1 | `dbutils.widgets.text("csv_path", ...)` | Choose the CSV file |
| 2 | `spark.read.option("multiLine",True).option("quote",'"').option("escape",'"').csv(...)` | Read the CSV; quote/escape keep the JSON cell intact |
| 3 | `normalize_ts(value)` | Accept several date formats → `YYYY-MM-DD HH:MM:SS` (milliseconds dropped) |
| 4 | `for r in raw_df.toLocalIterator(): ... json.loads(...)` | Parse each row; fill missing header fields from the CSV columns; validate → `good_rows` or `dlq_rows` |
| 5 | `good_df.join(existing_ids, "TRANS_ID", "left_anti")` then `.write.mode("append")` | Skip already-loaded TRANS_IDs; **one bulk write** to RAW; DLQ rows in one write |

**A row goes to the DLQ when:** the JSON is invalid · a required header field is missing in both JSON and
CSV · `1_vehicle_snapshot` is missing/empty · `eff_dt` or `src_trans_tmsp` can't be parsed.

**My version adds** (`bulk_backfill_from_csv.py` in this folder): file-exists check, column check,
skip-if-already-loaded (`BULK_LOAD_LOG`), a post-write check that every TRANS_ID landed, moving the file
to `processed/`, the correct inserted count, and a ✅ / ❌ summary.

**Output (example):**
```
Rows in CSV: 82   Parsed OK: 82   Failed: 0
Inserted 82 new row(s) into ws_prd_analytics.featurestore_test.RAW_POLICY_EVENTS
```

---

## 04 — Compute Features

**Purpose:** for each **new** transaction, compare it with the policy's **previous snapshot** (from the
state table) and compute the **30 features** → `COMPUTED_FEATURE_EVENTS`. Then save the newest snapshot
back to the state table.

```
new rows (CDF) ─► parse JSON ─► explode ─► aggregate ─┐
                                                      ├─► union ─► lag() compare ─► 30 features ─► COMPUTED
AUTO_POLICY_SNAPSHOT_STATE ─► prior_state row ────────┘                                      └─► state MERGE
```

### Step 1 — Setup
| Variable | Value |
|---|---|
| `RAW_EVENTS` / `COMPUTED` / `STATE` | Input / output / state tables |
| `CVG_CATEGORIES` | `BI, PD, MD, COLL, COMP, PIP` — other categories (UM, BIPD, ...) are ignored for added/removed |
| `RECENT_TMSP_WINDOW_DAYS = 35` | How many days of transaction times the state keeps (1 month + margin) |

### Step 2 — JSON schema (only the 13 fields features need)
| List | Fields |
|---|---|
| `1_vehicle_snapshot` | `vin`, `mdl_yr`, `auto_soi_trans_sk` |
| `2_driver_snapshot` | `drvr_id_sk`, `drvr_brth_dt`, `drvr_gndr_cd`, `drvr_mrtl_cd` |
| `3_coverage_snapshot` | `auto_soi_trans_sk`, `mva_cvg_ctgy_cd`, `cvg_typ_cd`, `ded_amt`, `old/new_full_term_prem_amt` |

All other JSON fields are ignored. A value with the wrong type becomes null (no error).

### Step 3 — New rows and parsing
```python
new_events = df.filter(F.col("_change_type") == "insert")
new_base   = new_events.withColumn("arrays", F.from_json("RAW_PAYLOAD", ARRAYS_SCHEMA)) ...
```
Only **this batch's** rows are parsed — no re-read of past transactions.

**Output — `new_base`:**
| TRANS_ID | AUTO_POLICY_ID | vehicles | drivers | coverages |
|---|---|---|---|---|
| T4 | P1001 | [A 2016, B 2021] | [D1, D2] | [BI A, PD A, COMP A ded 1000, BI B, PD B, COMP B ded 500] |
| T5 | P1001 | [B 2021] | [D1, D2] | [BI B, PD B, COMP B ded 500] |

### Step 4 — Explode the lists
`F.explode_outer` turns each list into one row per item. Coverages get their `vin` by joining on
`TRANS_ID + auto_soi_trans_sk` (the link to the vehicle within the same transaction).

**Output — `df_cvg` (T4 only):**
| TRANS_ID | vin | cvg_ctgy | cvg_typ_cd | ded_amt |
|---|---|---|---|---|
| T4 | A | BI | 13023 | null |
| T4 | A | PD | 13024 | null |
| T4 | A | COMP | 21000 | 1000 |
| T4 | B | BI | 13023 | null |
| T4 | B | PD | 13024 | null |
| T4 | B | COMP | 21000 | 500 |

### Step 5 — Aggregate back to one row per transaction
| Output | Formula |
|---|---|
| `veh_cnt`, `vin_set` | `countDistinct(vin)`, `collect_set(vin)` |
| `avg_veh_age` | `floor(avg(current_year − mdl_yr))` |
| `drvr_cnt`, `max/min/avg_drvr_age`, `drvr_id_set` | Ages = `floor(months_between(today, birth)/12)` |
| `gndr_map`, `mrtl_map` | Map driver id → gender / marital code |
| `prem_change_amt` | `sum(new − old premium)` over all coverages |
| `BI_key_set` … `PIP_key_set` | Set of `vin|cvg_typ_cd` per category |
| `comp_ded_map`, `coll_ded_map` | Map `vin|cvg_typ_cd` → deductible |

**Output — `new_snapshot`:**
| TRANS_ID | veh_cnt | avg_veh_age | vin_set | drvr_id_set | COMP_key_set | comp_ded_map | prem_change_amt | IS_STATE_ROW |
|---|---|---|---|---|---|---|---|---|
| T4 | 2 | 7 | [A, B] | [D1, D2] | [A\|21000, B\|21000] | {A\|21000: 1000, B\|21000: 500} | 30.0 | false |
| T5 | 1 | 5 | [B] | [D1, D2] | [B\|21000] | {B\|21000: 500} | −250.0 | false |

### Step 6 — Prior state row + union
```python
prior_state = spark.table(STATE).join(F.broadcast(touched_policies_df), "AUTO_POLICY_ID", "inner") ...
              .withColumn("SRC_TRANS_TMSP", F.lit("1900-01-01 00:00:00").cast("timestamp"))
              .withColumn("IS_STATE_ROW", F.lit(True)) ...
combined = prior_state_for_union.unionByName(new_snapshot_for_union)
```
- The state table has **one row per policy**, always overwritten with the latest snapshot, so a lookup by
  `AUTO_POLICY_ID` returns the previous transaction (T3).
- `1900-01-01` is a placeholder date so the state row sorts **first** (the state table doesn't store the
  real time). The state row is removed before anything is written.
- A brand-new policy has no state row → its first transaction compares with nothing → everything counts as added.

**Output — `combined`:**
| TRANS_ID | SRC_TRANS_TMSP | IS_STATE_ROW | vin_set | drvr_id_set | COMP_key_set | comp_ded_map |
|---|---|---|---|---|---|---|
| null | 1900-01-01 | **true** | [A, B] | [D1] | [A\|21000, B\|21000] | {A\|21000: 500, B\|21000: 500} |
| T4 | 2026-10-04 15:00 | false | [A, B] | [D1, D2] | [A\|21000, B\|21000] | {A\|21000: 1000, B\|21000: 500} |
| T5 | 2026-10-05 09:00 | false | [B] | [D1, D2] | [B\|21000] | {B\|21000: 500} |

### Step 7 — `lag()` comparisons
```python
w_asc = Window.partitionBy("AUTO_POLICY_ID").orderBy(IS_STATE_ROW.desc(), SRC_TRANS_TMSP.asc(), TRANS_ID.asc())
prev_vin_set = F.lag("vin_set").over(w_asc)                           # value from the row above
veh_added    = F.size(F.array_except(vin_set, prev_vin_set))          # new items
cat_added    = size(array_except(cur_set, prev_set)) > 0              # flag 1/0
cat_removed  = size(array_except(prev_set, cur_set)) > 0
map_change_ind(cur_map, prev_map)   # 1 if a key in both maps has a different value
```
Then the state row is filtered out.

**Output (comparison features):**
| TRANS_ID | Compared with | veh_added | drvrs_added | BI_removed | PD_removed | COMP_removed | comp_ded_change_ind |
|---|---|---|---|---|---|---|---|
| T4 | state (T3) | 0 | **1** (D2) | 0 | 0 | 0 | **1** (A: 500→1000) |
| T5 | T4 | 0 | 0 | **1** | **1** | **1** | 0 |

### Step 8 — Time-based features (GROUP_A)
- **Counts:** state's `RECENT_TXN_TMSPS` `[2026-09-06 09:00]` + this batch's times `[10-04 15:00, 10-05 09:00]`,
  counted against `current_timestamp()` (10-05 10:00).
- **Tenure:** `POLICY_INCEPTION_EFF_DT` from the state (2024-01-10), else the batch's earliest `EFF_DT`.
- **Outstanding:** `EFF_DT > current_date()`.

**Output:**
| TRANS_ID | TXN_CNT_1D | TXN_CNT_1W | TXN_CNT_1M | TENURE_YRS | OUTSTANDING_TXN_IND |
|---|---|---|---|---|---|
| T4 | 2 | 2 | 3 | 2 | 0 |
| T5 | 2 | 2 | 3 | 2 | **1** (effective 10-17) |

Counts are "as of now" and the same for every transaction of a policy in the batch.

### Step 9 — Write `COMPUTED_FEATURE_EVENTS`
```python
written = final.count()
final.write.format("delta").mode("append").saveAsTable(COMPUTED)
```
Columns renamed to uppercase; `COMPUTED_AT` added. CDF on this table feeds 05.

**Output — `COMPUTED_FEATURE_EVENTS` (selected columns):**
| TRANS_ID | EFF_DT | TXN_CNT_1D | VEH_CNT | DRVR_CNT | DRVRS_ADDED_LATEST_TXN | COMP_COVERAGE_REMOVED | COMP_DED_CHANGE_IND | PREM_CHANGE_AMT |
|---|---|---|---|---|---|---|---|---|
| T4 | 2026-10-04 | 2 | 2 | 2 | 1 | 0 | 1 | 30.0 |
| T5 | 2026-10-17 | 2 | 1 | 2 | 0 | 1 | 0 | −250.0 |

### Step 10 — Save the new state (`_update_snapshot_state`)
1. Newest transaction per policy in the batch (`row_number` over `SRC_TRANS_TMSP desc`) → **T5**.
2. Recent times = old + new, keep the last 35 days.
3. `MERGE INTO STATE ... WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *` — one MERGE per batch.

**Output — `AUTO_POLICY_SNAPSHOT_STATE`:**
| AUTO_POLICY_ID | VIN_SET | DRVR_ID_SET | COMP_KEY_SET | RECENT_TXN_TMSPS | POLICY_INCEPTION_EFF_DT | LAST_TRANS_ID |
|---|---|---|---|---|---|---|
| P1001 | [B] | [D1, D2] | [B\|21000] | [09-06 09:00, 10-04 15:00, 10-05 09:00] | 2024-01-10 | **T5** |

T4 isn't lost — it's in RAW and COMPUTED. The state only needs the latest snapshot for the next comparison.

---

## 05 — Routing Pipeline

**Purpose:** decide for each new transaction and feature group: **apply now** (ACTIVE + HISTORY) or
**wait** (PENDING). Batch-wide — a few SQL statements per batch, not per event.

### Step 1 — Setup
- `sim_date` widget: blank = real time; a date = pretend today is that date (testing).
- `EFFECTIVE_TS` = the "now" used for decisions (2026-10-05 10:00 in the example).
- Reads new rows of `COMPUTED_FEATURE_EVENTS` via CDF (`startingVersion = 0`).

### Step 2 — Now or later, per (transaction, group)
```python
IS_IMMEDIATE = F.lit(is_always_immediate) | (F.col("group_eff_dt") <= F.lit(EFFECTIVE_TS))
```
**Output — `group_routing`:**
| TRANS_ID | FEATURE_GROUP | group_eff_dt | IS_IMMEDIATE |
|---|---|---|---|
| T4 | GROUP_A | 10-04 15:00 | true |
| T5 | GROUP_A | 10-05 09:00 | true |
| T4 | GROUP_B | 10-04 00:00 | true |
| T5 | GROUP_B | **10-17 00:00** | **false** → PENDING |

### Step 3 — One closing point and group flags per transaction
- `closing_meta`: `earliest_eff = min(group_eff_dt)` over the transaction's immediate groups; `CHANGED_FEATURE_GROUP`.
- `immediate_flags`: pivot → `_is_immediate_GROUP_A` / `_is_immediate_GROUP_B` (true or null).

**Output:**
| TRANS_ID | earliest_eff | CHANGED_FEATURE_GROUP | _is_immediate_GROUP_A | _is_immediate_GROUP_B |
|---|---|---|---|---|
| T4 | 10-04 00:00 | GROUP_A,GROUP_B | true | true |
| T5 | 10-05 09:00 | GROUP_A | true | null |

### Step 4 — Keep only what applies now (`closing_rows`)
```python
F.when(group_applies_now, F.col(c)).otherwise(None)
```
**Output:**
| TRANS_ID | TXN_CNT_1D (A) | VEH_CNT (B) | GROUP_B_UPDATED_AT | IS_SENTINEL |
|---|---|---|---|---|
| T4 | 2 | 2 | 10-05 10:00 | false |
| T5 | 2 | **null** (not effective yet) | null | false |

### Step 5 — Fill the blanks from the current ACTIVE row
The policy's current ACTIVE row is added on top as the **sentinel**; `F.last(col, ignorenulls=True)` fills
each null with the value above it.

**Output — `combined`:**
| Row | TXN_CNT_1D | VEH_CNT |
|---|---|---|
| sentinel (ACTIVE before) | 0 | 2 |
| T4 | 2 | 2 |
| T5 | 2 | **2** (filled — car A still counts until 10-17) |

### Step 6 — History dates and HISTORY writes
```python
next_eff             = F.lead("earliest_eff").over(w_order)       # next row's start
VALID_TRANSACTION_TO = next_eff - INTERVAL 1 MILLISECOND
IS_CURRENT           = next_eff IS NULL
```
1. **Close** the old current row: `MERGE INTO HISTORY ... SET IS_CURRENT=false, VALID_TRANSACTION_TO=<sentinel's next − 1ms>`.
2. **Insert** the new rows (sentinel dropped) in one write.

**Output — `AUTO_FEATURES_HISTORY`:**
| TRANS_ID | REASON | VALID_TRANSACTION_FROM | VALID_TRANSACTION_TO | IS_CURRENT | VEH_CNT | TXN_CNT_1D |
|---|---|---|---|---|---|---|
| T3 | immediate_merge | 2026-09-06 00:00 | 2026-10-03 23:59:59.999 | false | 2 | … |
| T4 | immediate_merge | 2026-10-04 00:00 | 2026-10-05 08:59:59.999 | false | 2 | 2 |
| T5 | immediate_merge | 2026-10-05 09:00 | — | **true** | 2 | 2 |

### Step 7 — Update ACTIVE (one MERGE per group)
For each group: the newest transaction where the group applies now → `MERGE INTO ACTIVE ... UPDATE SET <group columns>`.
Other groups' columns are untouched.

| Group | Rows where it applies now | Used |
|---|---|---|
| GROUP_A | T4, T5 | **T5** |
| GROUP_B | T4 | **T4** |

**Output — `AUTO_FEATURES_ACTIVE`:**
| AUTO_POLICY_ID | TXN_CNT_1D | OUTSTANDING_TXN_IND | VEH_CNT | DRVR_CNT | COMP_DED_CHANGE_IND | GROUP_A_UPDATED_AT | GROUP_B_UPDATED_AT |
|---|---|---|---|---|---|---|---|
| P1001 | 2 | 1 | **2** | 2 | 1 | 10-05 10:00 | 10-05 10:00 |

### Step 8 — Write PENDING
```python
group_pending = pending_by_event.filter(FEATURE_GROUP == group)
                .join(df.select("TRANS_ID", "AUTO_POLICY_ID", *owned_cols), ...)
                .select(..., F.col("group_eff_dt").alias("EFFECTIVE_DATE"))
MERGE INTO PENDING ... ON AUTO_POLICY_ID AND SOURCE_EVENT_ID = TRANS_ID
```
**Output — `AUTO_FEATURES_PENDING`:**
| AUTO_POLICY_ID | TRANS_ID | VEH_CNT | BI_COVERAGE_REMOVED | COMP_COVERAGE_REMOVED | EFFECTIVE_DATE | CHANGED_FEATURE_GROUP |
|---|---|---|---|---|---|---|
| P1001 | T5 | 1 | 1 | 1 | **2026-10-17** | GROUP_B |

---

## 06 — Daily Pending Promotion

**Purpose:** every day, move PENDING rows whose `EFFECTIVE_DATE` has arrived into ACTIVE + HISTORY.
Only GROUP_B can be pending. ⚠️ Widget defaults are `main` / `final_database`.

### Step 1 — Find due rows
```python
SELECT * FROM PENDING WHERE EFFECTIVE_DATE <= TIMESTAMP '<run_ts>'
```
On **2026-10-17** → T5's GROUP_B row is due.

### Step 2 — Keep the latest per (policy, group)
`row_number()` over `EFFECTIVE_DATE desc, EVENT_RECEIVED_AT desc` → keep 1. Saved as view `_due_pending`.

### Step 3 — Close the current history row
`MERGE INTO HISTORY ... SET IS_CURRENT=false, VALID_TRANSACTION_TO = MIN(EFFECTIVE_DATE) − 1ms` (one per policy).

### Step 4 — MERGE GROUP_B into ACTIVE
`SET t.c = COALESCE(s.c, t.c)` for each GROUP_B column; `GROUP_B_UPDATED_AT = now`.

### Step 5 — Snapshot ACTIVE into HISTORY
`INSERT INTO HISTORY SELECT a.*, ..., REASON = 'pending_promoted:T5', VALID_TRANSACTION_FROM = EFFECTIVE_DATE, IS_CURRENT = true`.

### Step 6 — Delete promoted rows
`DELETE FROM PENDING WHERE EFFECTIVE_DATE <= run_ts`.

**Output on 2026-10-17:**

`AUTO_FEATURES_ACTIVE`
| AUTO_POLICY_ID | VEH_CNT | BI_COVERAGE_REMOVED | COMP_COVERAGE_REMOVED | GROUP_B_UPDATED_AT |
|---|---|---|---|---|
| P1001 | **1** | 1 | 1 | 10-17 |

`AUTO_FEATURES_HISTORY` (latest rows)
| REASON | VALID_TRANSACTION_FROM | VALID_TRANSACTION_TO | IS_CURRENT | VEH_CNT |
|---|---|---|---|---|
| (previous current row) | … | 2026-10-16 23:59:59.999 | false | 2 |
| pending_promoted:T5 | 2026-10-17 00:00 | — | **true** | 1 |

`AUTO_FEATURES_PENDING` → T5's row deleted.

---

## 07 — Daily GROUP_A Refresh

**Purpose:** GROUP_A changes with the calendar even without new transactions (counts drop off, tenure
grows, outstanding flips to 0). Every day, recompute GROUP_A for all policies and update where it changed.

Two versions:
| | `07_job_daily_group_a_refresh` (original) | `07_job_daily_group_a_refresh_v2` (optimized) |
|---|---|---|
| Counts | Full scan of `RAW_POLICY_EVENTS` | `STATE.RECENT_TXN_TMSPS` (last 35 days) |
| Tenure | `MIN(EFF_DT)` over RAW | `STATE.POLICY_INCEPTION_EFF_DT` |
| Outstanding | Latest `EFF_DT` for every policy | Only policies currently flagged 1 (can only turn 1 → 0) |
| Must run after 04? | No | **Yes**, and not at the same time |

### Step 1 — Recompute GROUP_A as of `run_ts`
On **2026-10-06 02:00** for P1001:
| Feature | Calculation | Value |
|---|---|---|
| TXN_CNT_1D | times ≥ 10-05 02:00 → 10-05 09:00 | **1** (was 2) |
| TXN_CNT_1W | times ≥ 09-29 02:00 → 10-04, 10-05 | 2 |
| TXN_CNT_1M | times ≥ 09-06 02:00 → 09-06 09:00, 10-04, 10-05 | 3 |
| OUTSTANDING_TXN_IND | latest EFF_DT 10-17 > 10-06 | 1 |
| TENURE_YRS | 2024-01-10 → 2026-10-06 | 2 |

### Step 2 — Keep only policies that changed
`eqNullSafe` comparison with ACTIVE → P1001 changed (`TXN_CNT_1D` 2 → 1).

### Step 3 — Update ACTIVE, close history, insert history
Same three statements as the end of 06, with `REASON = 'group_a_daily_refresh'` and
`VALID_TRANSACTION_FROM = run_ts`.

**Output:**
`AUTO_FEATURES_ACTIVE` → P1001 `TXN_CNT_1D = 1`, `GROUP_A_UPDATED_AT = 2026-10-06 02:00`.

`AUTO_FEATURES_HISTORY` (latest rows)
| REASON | VALID_TRANSACTION_FROM | VALID_TRANSACTION_TO | IS_CURRENT | TXN_CNT_1D |
|---|---|---|---|---|
| immediate_merge (T5) | 2026-10-05 09:00 | 2026-10-06 01:59:59.999 | false | 2 |
| group_a_daily_refresh | 2026-10-06 02:00 | — | **true** | 1 |

(v2 only) The last cell compares with the original calculation — `mismatches` should be 0.

---

## 08 — Online Feature Store

**Purpose:** serve ACTIVE for real-time lookups: send a policy id, get its 30 features back over REST.
⚠️ Widget defaults are `main` / `final_database`. Provisions paid resources.

| Step | Code (key call) | Output |
|---|---|---|
| 1 | `fe.create_online_store(name=..., capacity="CU_1")` + wait loop | Lakebase online store `auto-policy-online-store`, state AVAILABLE |
| 2 | `fe.publish_table(source_table_name=ACTIVE_TABLE, online_table_name=...)` | `AUTO_FEATURES_ACTIVE_ONLINE`, kept in sync from ACTIVE (needs ACTIVE's PK + CDF) |
| 3 | `fe.create_feature_spec(name=..., features=[FeatureLookup(..., lookup_key="AUTO_POLICY_ID", feature_names=30 cols)])` | Feature spec `AUTO_POLICY_FEATURE_SPEC` |
| 4 | `POST /api/2.0/serving-endpoints` (deletes and recreates a failed one) | Endpoint `auto-policy-feature-serving`, READY (scale-to-zero on) |
| 5 | `POST /serving-endpoints/<name>/invocations` with `{"dataframe_records":[{"AUTO_POLICY_ID": "..."}]}` | The 30 features + latency |

**Output — Step 5 for P1001 (after 05 on 2026-10-05):**
```
AUTO_POLICY_ID : P1001
Latency        : 45.2 ms
---------------------------------------------
  TXN_CNT_1D                     = 2
  TXN_CNT_1W                     = 2
  TXN_CNT_1M                     = 3
  OUTSTANDING_TXN_IND            = 1
  TENURE_YRS                     = 2
  VEH_CNT                        = 2
  AVG_VEH_AGE                    = 7.0
  DRVR_CNT                       = 2
  DRVRS_ADDED_LATEST_TXN         = 1
  COMP_DED_CHANGE_IND            = 1
  ...
```
`09_endpoint_warm_toggle` turns scale-to-zero off before a demo (no cold start) and back on after.

---

## Run order and checks

```
00_reset_all (optional) → 01 → 02 (optional) → 03 or bulk → 04 → 05 → 06 (daily) → 07 (daily) → 08 (once) → 09 (as needed)
```
For each new batch of data: **03 or bulk → 04 → 05**.

**Widgets to set in every notebook (prod):** `catalog = ws_prd_analytics`, `schema = featurestore_test`,
`checkpoint_path = /Volumes/ws_prd_analytics/featurestore_test/checkpoints/<sp_compute | routing | autoloader_landing>`.

**Counts after 04 → 05:**
```sql
SELECT 'raw' t, COUNT(*) FROM ws_prd_analytics.featurestore_test.raw_policy_events
UNION ALL SELECT 'computed', COUNT(*) FROM ws_prd_analytics.featurestore_test.computed_feature_events
UNION ALL SELECT 'state',    COUNT(*) FROM ws_prd_analytics.featurestore_test.auto_policy_snapshot_state
UNION ALL SELECT 'active',   COUNT(*) FROM ws_prd_analytics.featurestore_test.auto_features_active
UNION ALL SELECT 'pending',  COUNT(*) FROM ws_prd_analytics.featurestore_test.auto_features_pending
UNION ALL SELECT 'history',  COUNT(*) FROM ws_prd_analytics.featurestore_test.auto_features_history;
```
Expect `raw = computed` (transactions) and `state = active` (policies, +1 if 02 ran).

**Exactly one current history row per policy (must return 0 rows):**
```sql
SELECT AUTO_POLICY_ID, COUNT(*) FROM ws_prd_analytics.featurestore_test.auto_features_history
WHERE IS_CURRENT GROUP BY AUTO_POLICY_ID HAVING COUNT(*) <> 1;
```

---

## Known limitations

| Area | Issue | Effect |
|---|---|---|
| 04 | **Late / out-of-order transactions** — the state row is always treated as "before" new rows | A transaction older than the state is compared with the wrong snapshot and overwrites the state backwards. Fix: store `LAST_SRC_TRANS_TMSP` in the state and recompute late policies from full history. |
| 04 | **Not safe to rerun a failed batch** — COMPUTED, state and checkpoint are saved separately | Duplicate COMPUTED rows, or comparison features of 0 after a partial failure |
| 04 | `collect_set` on recent times | Two transactions with the same timestamp count once in `TXN_CNT_*` (use `collect_list`) |
| 04 | Ages use `current_date()` and aren't refreshed daily | `AVG_VEH_AGE`, driver ages drift until the policy's next transaction |
| 05 | `last(col, ignorenulls=True)` per column | A genuinely null GROUP_B value on an immediate transaction is replaced by the previous value |
| 05 | Rerun after a failure | Duplicate HISTORY rows (`05_routing_pipeline_batch.py` handles this) |
| 02 | Placeholder state for `503203043` | Its first real transaction shows everything as "added" |
| Widgets | Defaults differ per notebook (`workspace`, `main`, `ws_prd_analytics`) | Always set catalog / schema / checkpoint_path explicitly |
