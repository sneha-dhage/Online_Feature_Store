# 05 — Routing Pipeline: Code Reference

**What it does:** takes new rows from `COMPUTED_FEATURE_EVENTS` and, for each feature group, either
applies them **now** (to `AUTO_FEATURES_ACTIVE` + `AUTO_FEATURES_HISTORY`) or **parks** them in
`AUTO_FEATURES_PENDING` until their effective date.

**Rule (from `FEATURE_GROUP_REGISTRY`):**
| Group | ROUTING_TYPE | EFFECTIVE_DATE_COL | Applied |
|---|---|---|---|
| GROUP_A (5 columns) | `immediate` | `SRC_TRANS_TMSP` | Always now |
| GROUP_B (25 columns) | `effective_date` | `EFF_DT` | Now if `EFF_DT <= now`, else PENDING |

## Tables used
| Table | Read / Write | Columns used |
|---|---|---|
| `COMPUTED_FEATURE_EVENTS` | **Read** (CDF stream) | `TRANS_ID`, `AUTO_POLICY_ID`, `PLCY_CNTRCT_NUM`, `SRC_HH_NUM`, `SRC_TRANS_TMSP`, `EFF_DT`, all 30 feature columns, `_change_type` |
| `FEATURE_GROUP_REGISTRY` | **Read** | `FEATURE_GROUP`, `ROUTING_TYPE`, `EFFECTIVE_DATE_COL`, `OWNED_COLUMNS` |
| `AUTO_FEATURES_ACTIVE` | **Read** (current row) + **Write** (MERGE) | All columns |
| `AUTO_FEATURES_HISTORY` | **Write** (close old row + insert new rows) | All ACTIVE columns + `SNAPSHOT_AT`, `REASON`, `EVENT_ID`, `CHANGED_FEATURE_GROUP`, `VALID_TRANSACTION_FROM`, `VALID_TRANSACTION_TO`, `IS_CURRENT` |
| `AUTO_FEATURES_PENDING` | **Write** (MERGE) | `AUTO_POLICY_ID`, `TRANS_ID`, 25 GROUP_B columns, `EFFECTIVE_DATE`, `CHANGED_FEATURE_GROUP`, `SOURCE_EVENT_ID`, `EVENT_RECEIVED_AT` |

## Flow
```
COMPUTED_FEATURE_EVENTS (new rows)
   1. now or later?  ──────────────────────────────►  later → 8. PENDING
   2. one closing date per transaction
   3. blank out values that aren't effective yet
   4. add current ACTIVE row on top, fill blanks
   5. history dates (from / to / current)
   6. close old HISTORY row  →  7. insert new HISTORY rows
   7. MERGE each group into ACTIVE
```

---

## 0. Setup

**Tables:** none read yet — builds names and the "now" used for decisions.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

dbutils.widgets.text("catalog",         "workspace")
dbutils.widgets.text("schema",          "featurestore_test")
dbutils.widgets.text("checkpoint_path", "/Volumes/workspace/featurestore_test/checkpoints/routing_v2")
dbutils.widgets.text("sim_date",        "")   # YYYY-MM-DD HH:MM:SS or blank for real time

CATALOG         = dbutils.widgets.get("catalog")
SCHEMA          = dbutils.widgets.get("schema")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
_sim            = dbutils.widgets.get("sim_date").strip()

if _sim:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            EFFECTIVE_TS = datetime.strptime(_sim, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse sim_date '{_sim}'. Use YYYY-MM-DD HH:MM:SS")
else:
    EFFECTIVE_TS = datetime.now()

COMPUTED = f"{CATALOG}.{SCHEMA}.COMPUTED_FEATURE_EVENTS"
ACTIVE   = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_ACTIVE"
PENDING  = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_PENDING"
HISTORY  = f"{CATALOG}.{SCHEMA}.AUTO_FEATURES_HISTORY"
REGISTRY = f"{CATALOG}.{SCHEMA}.FEATURE_GROUP_REGISTRY"
```
- `EFFECTIVE_TS` = "now" for routing. `sim_date` lets you test with another date.
- In ws_prd set `catalog = ws_prd_analytics` and `checkpoint_path = /Volumes/ws_prd_analytics/featurestore_test/checkpoints/routing`.

---

## 1. Now or later, per transaction and group

**Reads:** `COMPUTED_FEATURE_EVENTS` (`_change_type`, `TRANS_ID`, `AUTO_POLICY_ID`, `SRC_TRANS_TMSP`, `EFF_DT`),
`FEATURE_GROUP_REGISTRY` (`FEATURE_GROUP`, `ROUTING_TYPE`, `EFFECTIVE_DATE_COL`).

```python
def process_micro_batch(df, batch_id):
    df = df.filter(F.col("_change_type") == "insert")
    count = df.count()
    if count == 0:
        return

    registry    = spark.table(REGISTRY).collect()
    group_names = [reg["FEATURE_GROUP"] for reg in registry]

    group_routing = None
    for reg in registry:
        group               = reg["FEATURE_GROUP"]
        is_always_immediate = reg["ROUTING_TYPE"] == "immediate"
        eff_col             = reg["EFFECTIVE_DATE_COL"]          # SRC_TRANS_TMSP or EFF_DT

        g = (
            df.select("TRANS_ID", "AUTO_POLICY_ID", F.col(eff_col).alias("group_eff_dt"))
            .withColumn("FEATURE_GROUP", F.lit(group))
            .withColumn("IS_IMMEDIATE",
                        F.lit(is_always_immediate) | (F.col("group_eff_dt") <= F.lit(EFFECTIVE_TS)))
        )
        group_routing = g if group_routing is None else group_routing.unionByName(g)

    immediate_by_event = group_routing.filter("IS_IMMEDIATE")
    pending_by_event   = group_routing.filter("NOT IS_IMMEDIATE")
```
**Result:** one row per (transaction, group) with `IS_IMMEDIATE` true/false.
| TRANS_ID | FEATURE_GROUP | group_eff_dt | IS_IMMEDIATE |
|---|---|---|---|
| T4 | GROUP_A | 10-04 15:00 | true |
| T4 | GROUP_B | 10-04 | true |
| T5 | GROUP_A | 10-05 09:00 | true |
| T5 | GROUP_B | 10-17 | **false** → PENDING |

---

## 2. One closing date and group flags per transaction

**Uses:** `immediate_by_event` (from step 1). No table read.

```python
    closing_meta = (
        immediate_by_event.groupBy("TRANS_ID", "AUTO_POLICY_ID")
        .agg(F.min("group_eff_dt").alias("earliest_eff"),
             F.concat_ws(",", F.collect_set("FEATURE_GROUP")).alias("CHANGED_FEATURE_GROUP"))
    )

    immediate_flags = (
        immediate_by_event.groupBy("TRANS_ID", "AUTO_POLICY_ID")
        .pivot("FEATURE_GROUP", group_names)
        .agg(F.lit(True))
    )
    for group in group_names:
        immediate_flags = immediate_flags.withColumnRenamed(group, f"_is_immediate_{group}")
```
- `earliest_eff` → becomes HISTORY `VALID_TRANSACTION_FROM`.
- `CHANGED_FEATURE_GROUP` → stored in HISTORY.
- `_is_immediate_GROUP_A / _B` → true or null, used in step 3.

---

## 3. Keep only values that apply now

**Reads:** `COMPUTED_FEATURE_EVENTS` batch (`df`) — all 30 feature columns, `PLCY_CNTRCT_NUM`, `SRC_HH_NUM`;
`FEATURE_GROUP_REGISTRY` (`OWNED_COLUMNS`).
**Builds:** rows shaped like `AUTO_FEATURES_ACTIVE`.

```python
    closing_rows = (
        df.join(closing_meta, ["TRANS_ID", "AUTO_POLICY_ID"], "inner")
          .join(immediate_flags, ["TRANS_ID", "AUTO_POLICY_ID"], "left")
    )

    for reg in registry:
        group      = reg["FEATURE_GROUP"]
        owned_cols = reg["OWNED_COLUMNS"]
        flag_col   = F.col(f"_is_immediate_{group}").isNotNull()
        for c in owned_cols:
            closing_rows = closing_rows.withColumn(c, F.when(flag_col, F.col(c)).otherwise(F.lit(None)))
        closing_rows = closing_rows.withColumn(
            f"{group}_UPDATED_AT",
            F.when(flag_col, F.current_timestamp()).otherwise(F.lit(None).cast("timestamp"))
        )

    closing_rows = (
        closing_rows
        .withColumn("LAST_SOURCE_EVENT_ID", F.col("TRANS_ID"))
        .withColumn("FEATURE_UPDATED_AT", F.current_timestamp())
        .withColumn("IS_SENTINEL", F.lit(False))
    )

    active_cols = ["AUTO_POLICY_ID", "PLCY_CNTRCT_NUM", "SRC_HH_NUM", "TRANS_ID"]
    for reg in registry:
        active_cols += reg["OWNED_COLUMNS"] + [f"{reg['FEATURE_GROUP']}_UPDATED_AT"]
    active_cols += ["FEATURE_UPDATED_AT", "LAST_SOURCE_EVENT_ID"]

    closing_rows = closing_rows.select(*active_cols, "earliest_eff", "CHANGED_FEATURE_GROUP", "IS_SENTINEL")
```
**Columns set here (ACTIVE layout):**
| Column | Value |
|---|---|
| GROUP_A / GROUP_B feature columns | The value if the group applies now, else **null** |
| `GROUP_A_UPDATED_AT` / `GROUP_B_UPDATED_AT` | Now if the group applies, else null |
| `LAST_SOURCE_EVENT_ID` | `TRANS_ID` |
| `FEATURE_UPDATED_AT` | Now |
| `IS_SENTINEL` | false (real transaction) |

---

## 4. Add the current ACTIVE row and fill the blanks

**Reads:** `AUTO_FEATURES_ACTIVE` (all columns in `active_cols`) for the policies in this batch.

```python
    touched_with_immediate = closing_meta.select("AUTO_POLICY_ID").distinct()

    old_active_sentinel = (
        spark.table(ACTIVE)
        .join(F.broadcast(touched_with_immediate), "AUTO_POLICY_ID", "inner")
        .select(*active_cols)
        .withColumn("earliest_eff", F.lit(None).cast("timestamp"))
        .withColumn("CHANGED_FEATURE_GROUP", F.lit(None).cast("string"))
        .withColumn("IS_SENTINEL", F.lit(True))
    )

    combined = old_active_sentinel.unionByName(closing_rows)

    w_running = (
        Window.partitionBy("AUTO_POLICY_ID")
        .orderBy(F.col("IS_SENTINEL").desc(), F.col("earliest_eff").asc(), F.col("TRANS_ID").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    carry_forward_cols = []
    for reg in registry:
        carry_forward_cols += reg["OWNED_COLUMNS"] + [f"{reg['FEATURE_GROUP']}_UPDATED_AT"]

    for c in carry_forward_cols:
        combined = combined.withColumn(c, F.last(F.col(c), ignorenulls=True).over(w_running))
```
- Sentinel = the policy's ACTIVE row **before** this batch, placed first.
- `F.last(..., ignorenulls=True)` fills each null with the last known value above it.

---

## 5. History dates

**Uses:** `combined` (step 4). Sets `VALID_TRANSACTION_TO`, `IS_CURRENT`.

```python
    w_order = Window.partitionBy("AUTO_POLICY_ID").orderBy(
        F.col("IS_SENTINEL").desc(), F.col("earliest_eff").asc(), F.col("TRANS_ID").asc())
    combined = (
        combined
        .withColumn("next_eff", F.lead("earliest_eff").over(w_order))
        .withColumn("VALID_TRANSACTION_TO",
                    F.when(F.col("next_eff").isNotNull(),
                           F.col("next_eff") - F.expr("INTERVAL 1 MILLISECOND")))
        .withColumn("IS_CURRENT", F.col("next_eff").isNull())
    )
```
| Column | Value |
|---|---|
| `next_eff` | Next row's `earliest_eff` |
| `VALID_TRANSACTION_TO` | `next_eff − 1 ms` (null for the last row) |
| `IS_CURRENT` | true only for the last row of each policy |

---

## 6. Close the old current HISTORY row

**Writes:** `AUTO_FEATURES_HISTORY` — `IS_CURRENT`, `VALID_TRANSACTION_TO` (matched on `AUTO_POLICY_ID` and `IS_CURRENT = true`).

```python
    close_boundaries = (
        combined.filter(F.col("IS_SENTINEL") & F.col("next_eff").isNotNull())
        .select("AUTO_POLICY_ID", F.col("VALID_TRANSACTION_TO").alias("close_to"))
    )

    if close_boundaries.limit(1).count() > 0:
        close_boundaries.createOrReplaceTempView("_close_boundaries")
        spark.sql(f"""
            MERGE INTO {HISTORY} AS h
            USING _close_boundaries AS s
            ON h.AUTO_POLICY_ID = s.AUTO_POLICY_ID AND h.IS_CURRENT = true
            WHEN MATCHED THEN UPDATE SET
                h.IS_CURRENT           = false,
                h.VALID_TRANSACTION_TO = s.close_to
        """)
```
The sentinel's `VALID_TRANSACTION_TO` = just before the first new transaction → the end date of the old current row.

---

## 7. Insert new HISTORY rows

**Writes:** `AUTO_FEATURES_HISTORY` — all ACTIVE columns + `SNAPSHOT_AT`, `REASON`, `EVENT_ID`,
`CHANGED_FEATURE_GROUP`, `VALID_TRANSACTION_FROM`, `VALID_TRANSACTION_TO`, `IS_CURRENT`.

```python
    new_history_rows = (
        combined.filter(~F.col("IS_SENTINEL"))
        .withColumn("SNAPSHOT_AT", F.current_timestamp())
        .withColumn("REASON", F.lit("immediate_merge"))
        .withColumn("EVENT_ID", F.col("TRANS_ID"))
        .withColumn("VALID_TRANSACTION_FROM", F.col("earliest_eff"))
        .select(*active_cols, "SNAPSHOT_AT", "REASON", "EVENT_ID", "CHANGED_FEATURE_GROUP",
                "VALID_TRANSACTION_FROM", "VALID_TRANSACTION_TO", "IS_CURRENT")
    )
    new_history_rows.write.format("delta").mode("append").saveAsTable(HISTORY)
```
| Column | Value |
|---|---|
| `REASON` | `immediate_merge` |
| `EVENT_ID` | `TRANS_ID` |
| `VALID_TRANSACTION_FROM` | `earliest_eff` |
| `SNAPSHOT_AT` | Now |

---

## 8. MERGE each group into ACTIVE

**Reads:** `closing_rows` (step 3). **Writes:** `AUTO_FEATURES_ACTIVE` — the group's `OWNED_COLUMNS`,
`PLCY_CNTRCT_NUM`, `SRC_HH_NUM`, `TRANS_ID`, `<GROUP>_UPDATED_AT`, `FEATURE_UPDATED_AT`, `LAST_SOURCE_EVENT_ID`.

```python
    for reg in registry:
        group          = reg["FEATURE_GROUP"]
        owned_cols     = reg["OWNED_COLUMNS"]
        updated_at_col = f"{group}_UPDATED_AT"

        group_latest = (
            closing_rows.filter(F.col(f"{group}_UPDATED_AT").isNotNull())
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("AUTO_POLICY_ID")
                .orderBy(F.col("earliest_eff").desc(), F.col("TRANS_ID").desc())))
            .filter(F.col("_rn") == 1)
            .select("AUTO_POLICY_ID", "TRANS_ID", "PLCY_CNTRCT_NUM", "SRC_HH_NUM", *owned_cols)
        )

        if group_latest.limit(1).count() == 0:
            continue

        group_latest.createOrReplaceTempView("_group_latest")
        set_clause  = ", ".join(f"t.{c} = s.{c}" for c in owned_cols)
        insert_cols = ", ".join(owned_cols)
        insert_vals = ", ".join(f"s.{c}" for c in owned_cols)

        spark.sql(f"""
            MERGE INTO {ACTIVE} AS t
            USING _group_latest AS s
            ON t.AUTO_POLICY_ID = s.AUTO_POLICY_ID
            WHEN MATCHED THEN UPDATE SET
                {set_clause},
                t.PLCY_CNTRCT_NUM       = s.PLCY_CNTRCT_NUM,
                t.SRC_HH_NUM            = s.SRC_HH_NUM,
                t.TRANS_ID              = s.TRANS_ID,
                t.{updated_at_col}      = current_timestamp(),
                t.FEATURE_UPDATED_AT    = current_timestamp(),
                t.LAST_SOURCE_EVENT_ID  = s.TRANS_ID
            WHEN NOT MATCHED THEN INSERT (
                AUTO_POLICY_ID, TRANS_ID, PLCY_CNTRCT_NUM, SRC_HH_NUM, {insert_cols}, {updated_at_col},
                FEATURE_UPDATED_AT, LAST_SOURCE_EVENT_ID
            ) VALUES (
                s.AUTO_POLICY_ID, s.TRANS_ID, s.PLCY_CNTRCT_NUM, s.SRC_HH_NUM, {insert_vals}, current_timestamp(),
                current_timestamp(), s.TRANS_ID
            )
        """)
```
- Runs **once per group** (2 MERGEs per batch).
- Uses each policy's **newest** transaction where that group applies now.
- Only that group's columns are updated; the other group stays as it is.

---

## 9. MERGE future-dated groups into PENDING

**Reads:** `pending_by_event` (step 1), batch `df` (the group's `OWNED_COLUMNS`).
**Writes:** `AUTO_FEATURES_PENDING` — `AUTO_POLICY_ID`, `TRANS_ID`, GROUP_B columns, `EFFECTIVE_DATE`,
`CHANGED_FEATURE_GROUP`, `SOURCE_EVENT_ID`, `EVENT_RECEIVED_AT` (matched on `AUTO_POLICY_ID` + `SOURCE_EVENT_ID`).

```python
    for reg in registry:
        group      = reg["FEATURE_GROUP"]
        owned_cols = reg["OWNED_COLUMNS"]

        group_pending = (
            pending_by_event.filter(F.col("FEATURE_GROUP") == group)
            .join(df.select("TRANS_ID", "AUTO_POLICY_ID", *owned_cols), ["TRANS_ID", "AUTO_POLICY_ID"], "inner")
            .select("AUTO_POLICY_ID", "TRANS_ID", *owned_cols, F.col("group_eff_dt").alias("EFFECTIVE_DATE"))
        )

        if group_pending.limit(1).count() == 0:
            continue

        group_pending.createOrReplaceTempView("_group_pending")
        set_clause  = ", ".join(f"t.{c} = s.{c}" for c in owned_cols)
        insert_cols = ", ".join(owned_cols)
        insert_vals = ", ".join(f"s.{c}" for c in owned_cols)

        spark.sql(f"""
            MERGE INTO {PENDING} AS t
            USING _group_pending AS s
            ON  t.AUTO_POLICY_ID  = s.AUTO_POLICY_ID
            AND t.SOURCE_EVENT_ID = s.TRANS_ID
            WHEN MATCHED THEN UPDATE SET
                {set_clause},
                t.EVENT_RECEIVED_AT = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                AUTO_POLICY_ID, TRANS_ID, {insert_cols}, EFFECTIVE_DATE, CHANGED_FEATURE_GROUP,
                SOURCE_EVENT_ID, EVENT_RECEIVED_AT
            ) VALUES (
                s.AUTO_POLICY_ID, s.TRANS_ID, {insert_vals}, s.EFFECTIVE_DATE, '{group}',
                s.TRANS_ID, current_timestamp()
            )
        """)
```
In practice only GROUP_B ends up here. `06_job_daily_promotion` moves these rows to ACTIVE when `EFFECTIVE_DATE` arrives.

---

## 10. Start the stream

**Reads:** `COMPUTED_FEATURE_EVENTS` change feed. **Writes:** progress to `CHECKPOINT_PATH`.

```python
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
query.awaitTermination()
```
- `startingVersion = "0"` → on the first run, process all existing rows; later runs continue from the checkpoint.
- `availableNow` → process everything new, then stop.

---

## Quick check after running
```sql
-- one current history row per policy (must return 0 rows)
SELECT AUTO_POLICY_ID, COUNT(*) FROM ws_prd_analytics.featurestore_test.auto_features_history
WHERE IS_CURRENT GROUP BY AUTO_POLICY_ID HAVING COUNT(*) <> 1;

-- what is waiting for a future date
SELECT AUTO_POLICY_ID, TRANS_ID, EFFECTIVE_DATE FROM ws_prd_analytics.featurestore_test.auto_features_pending;
```
