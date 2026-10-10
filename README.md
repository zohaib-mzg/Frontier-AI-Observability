# Frontier AI Observatory – Raw to Bronze pipeline

A pipeline (scheduled Monday, Wednesday and Friday) that collects public AI-model data (LMArena leaderboard, CooperBench trajectories, BenchLM, Epoch AI) with **GitHub Actions**, lands it in a **Databricks Unity Catalog Volume**, and loads it with **PySpark + Delta** into the Bronze layer. Everything runs on Databricks Free Edition.

> **Status:** Staging (landing volume) and Raw → Bronze are implemented (this README). The `silver` and `gold` schemas are created but empty; Bronze → Silver and Gold are the next phases and will be documented here when they are built.

## 1. How it works

```
GitHub Actions (Mon/Wed/Fri 03:00 UTC)             Databricks
┌─────────────────────────────────┐     ┌───────────────────────────────────────────────┐
│ ingestion/frontier_collect.py   │     │ Volume  /Volumes/<catalog>/landing/staging/    │
│  smoke | full | incremental     │ ──▶ │   frontier/ new/  processed/  quarantine/      │
│  downloads, SHA-256, manifest   │     │             _state/  logs/                     │
└─────────────────────────────────┘     │                    │                           │
                                        │  spark/bronze/frontier_bronze_load.ipynb       │
                                        │                    ▼                           │
                                        │  <catalog>.bronze.*   (MERGE INTO, insert-only)│
                                        │  <catalog>.ops.*      (logs, drift, quarantine)│
                                        └───────────────────────────────────────────────┘
```

Size rules (checked in the collector **and again** in the Bronze notebook): a `full` load must land ≥ 400 MB, an `incremental` batch ≥ 1 MB. A batch that breaks the rule is refused and logged.

## 2. Repository layout

```
.github/workflows/frontier-collect.yml   GitHub Actions: smoke / full / incremental collection (Mon/Wed/Fri)
ingestion/frontier_collect.py            Collector (source -> Volume, manifest, state)
notebooks/00_setup_databricks.ipynb       One-time setup: landing, bronze, silver, gold, ops schemas + volume
spark/bronze/frontier_bronze_load.ipynb   Raw -> Bronze notebook (PySpark, parameterised, backfill, replay)
spark/bronze/frontier_bronze_proof.ipynb  Proof notebook: schemas, MERGE, backfill, drift, replay, key check, logs
docs/Frontier_Bronze_Run_Guide.md          Full step-by-step run guide, including the click-by-click Databricks setup
proof/                                   Executed notebooks with saved outputs (.ipynb/.html) or video link
sample_data/                             Small samples only
requirements.txt                         Collector dependencies
.gitignore
README.md
```

## 3. Rubric features and where to see them

| Feature | Where in the code | How to see it |
|---|---|---|
| Explicit schemas, no `inferSchema` | `ARENA_CONTRACT`, `EPOCH_CONTRACT`, `COOP_ROW_SCHEMA`, `BLM_ROW_SCHEMA` (Cell 2); Arena/Epoch are read with a `StructType` built from the contract | Proof 1 |
| `load_timestamp` on every row | every Bronze and `ops` table; set to `current_timestamp()` when the batch is written | `DESCRIBE TABLE` in Proof 1 |
| Provenance | `source_file`, `batch_id`, `collection_timestamp`, `load_type` on every Bronze row | any Bronze table |
| `MERGE INTO` idempotency | `merge_insert_only()` on `record_hash`; plus a file-level skip (same SHA-256) | Proof 2 |
| Parameters / backfill from any point to any point | widgets `source_dir`, `batch_id`, `from_batch`, `to_batch`, `start_date`, `end_date`, `source`, `load_type`, `force_reload`, `replay` | Proof 3 and 4B |
| Replay from the first batch, batch by batch (no full load) | `replay=true`: every earlier batch is loaded on its own, oldest first, each with its own log rows | Proof 4B |
| No primary key problems | insert-only `MERGE` on `record_hash`, de-duplication inside every chunk, and a key check after every load that logs duplicate or null keys per table | Proof 4B and 6 |
| Schema drift | new Arena column → `ALTER TABLE … ADD COLUMNS`; type change → file quarantined, batch continues; other sources → absorbed in `raw_payload` and logged | Proof 4 |
| Execution log for every stage | `ops.pipeline_execution_logs`: `Source-to-Landing` (collector), `Raw-to-Bronze` (file), `(batch)`, `(run)` and `Bronze data quality (primary key)` | Proof 5 and 6 |

## 4. Bronze data models

Primary key = the column `MERGE INTO` matches on. Delta does not enforce keys; the pipeline does. All four tables are Delta tables in `<catalog>.bronze`.

### 4.1 `bronze.arena_leaderboard_raw` – one Arena leaderboard row. PK `record_hash`

| Column | Type | Meaning |
|---|---|---|
| `subset` | STRING | `text`, `text_style_control`, `webdev`, `agent` |
| `load_type` | STRING | `full` or `incremental` |
| `model_name` | STRING | Model name as published |
| `organization` | STRING | Organization as published |
| `license` | STRING | License as published |
| `rating` | DOUBLE | Arena rating |
| `variance` | DOUBLE | Rating variance |
| `rating_q975` | DOUBLE | Upper confidence bound |
| `rating_q025` | DOUBLE | Lower confidence bound |
| `vote_count` | BIGINT | Number of votes |
| `rank` | BIGINT | Rank |
| `category` | STRING | Leaderboard category |
| `leaderboard_publish_date` | STRING | Publication date as text (cast in Silver) |
| *(new source columns)* | per source | Added automatically by schema evolution |
| `raw_payload` | STRING | Whole original row as JSON |
| `record_hash` | STRING | SHA-256 of `subset` + `raw_payload`. **PK** |
| `source_file` | STRING | Landing file the row came from |
| `batch_id` | STRING | Collector batch |
| `collection_timestamp` | TIMESTAMP | When the collector fetched the file |
| `load_timestamp` | TIMESTAMP | When the row was written to Bronze |

### 4.2 `bronze.cooperbench_member_raw` – one JSON/JSONL file inside an archive. PK `record_hash`

| Column | Type | Meaning |
|---|---|---|
| `study_setting` | STRING | `solo`, `coopgit`, `team`, `team_noproto` |
| `archive_file` | STRING | Archive name |
| `member_path` | STRING | Path inside the archive |
| `member_extension` | STRING | `.json` or `.jsonl` |
| `member_size_bytes` | BIGINT | Size inside the archive |
| `raw_content` | STRING | File text exactly as stored |
| `record_hash` | STRING | SHA-256 of setting + path + content hash. **PK** |
| `source_file` | STRING | Landing path of the archive |
| `batch_id` | STRING | Collector batch |
| `collection_timestamp` | TIMESTAMP | When the collector fetched the archive |
| `load_timestamp` | TIMESTAMP | When the row was written to Bronze |

### 4.3 `bronze.benchlm_raw` – one record of one export in one snapshot. PK `record_hash`

| Column | Type | Meaning |
|---|---|---|
| `dataset` | STRING | `models`, `benchmarks`, `pricing` |
| `record_path` | STRING | Where the record list sits in the JSON (`$` or `$.key`) |
| `record_key` | STRING | Record key when the export is an object of objects, else null |
| `record_index` | BIGINT | Position in the export |
| `raw_payload` | STRING | Record as compact JSON (keys sorted) |
| `payload_hash` | STRING | SHA-256 of `raw_payload` (compare snapshots) |
| `record_hash` | STRING | SHA-256 of snapshot + dataset + index. **PK** |
| `snapshot_id` | STRING | SHA-256 of the whole file |
| `source_file`, `batch_id` | STRING | Provenance |
| `collection_timestamp` | TIMESTAMP | When the collector fetched the file |
| `load_timestamp` | TIMESTAMP | When the row was written to Bronze |

### 4.4 `bronze.epoch_models_raw` – one CSV row in one snapshot. PK `record_hash`

| Column | Type | Meaning |
|---|---|---|
| `raw_payload` | STRING | Whole CSV row as JSON (all values kept as text) |
| `payload_hash` | STRING | SHA-256 of `raw_payload` |
| `record_hash` | STRING | SHA-256 of snapshot + `raw_payload`. **PK** |
| `snapshot_id` | STRING | SHA-256 of the whole file |
| `source_file`, `batch_id` | STRING | Provenance |
| `collection_timestamp` | TIMESTAMP | When the collector fetched the file |
| `load_timestamp` | TIMESTAMP | When the row was written to Bronze |

### 4.5 Operations tables (`<catalog>.ops`)

| Table | Purpose | Key columns |
|---|---|---|
| `pipeline_execution_logs` | One row per **file**, per **batch** and per **run** | `run_id`, `batch_id`, `layer`, `file_or_param`, `parameters_json`, `start_time`, `end_time`, `status`, `rows_read`, `rows_inserted`, `rows_updated`, `rows_skipped_duplicate`, `rows_quarantined`, `message`, `load_timestamp` |
| `error_logs` | Every error, from the collector and the notebook | `error_id`, `component`, `severity`, `error_type`, `error_message`, `stack_trace`, `occurred_at` |
| `quarantine_log` | Files/members that were set aside | `source_file`, `reason`, `detail`, `record_sample`, `quarantined_at` |
| `schema_drift_log` | Every schema change seen | `dataset`, `column_name`, `event_type` (`NEW_COLUMN`, `MISSING_COLUMN`, `TYPE_CHANGE`, `BASELINE`), `expected_type`, `actual_type`, `action_taken` |

Layers written to the log: `Source-to-Landing` (the GitHub collector, sent to Databricks), `Raw-to-Bronze` (one row per file), `Raw-to-Bronze (batch)`, `Raw-to-Bronze (run)` (one per notebook run, also empty or failed ones) and `Bronze data quality (primary key)` (one per Bronze table after a load). `status` values: `SUCCESS`, `FAILURE`, plus `SKIPPED` (same file already loaded), `QUARANTINED` (file set aside, batch continued) and `REJECTED` (batch broke the volume rule). `rows_updated` is always 0 in Bronze because Bronze is insert-only evidence; updates happen in Silver.

## 5. Schema handling and drift

- **Arena (Parquet):** the file footer is compared with the table (`ARENA_CONTRACT` plus evolved columns). New column → `ALTER TABLE ADD COLUMNS`, logged `NEW_COLUMN / TABLE_EVOLVED`. Missing column → null, logged. Type change that is not a safe widening → the **file is quarantined** (moved to `quarantine/`, one row in `ops.quarantine_log` with a sample of the file's columns and types) and the rest of the batch loads.
- **CooperBench:** members are stored as raw text. Members that are too large or not UTF-8 are quarantined individually, with a content sample.
- **BenchLM / Epoch:** everything is kept in `raw_payload`; new or missing fields never break the load and are written to `ops.schema_drift_log`. Epoch's core column names are fixed in `EPOCH_CONTRACT`; extend that list from the smoke-test output.

## 6. How to run

### One-time setup
1. Databricks: run `notebooks/00_setup_databricks` once. It creates the schemas `landing`, `bronze`, `silver`, `gold`, `ops` (silver and gold stay empty), the volume `/Volumes/<catalog>/landing/staging/frontier` and its folders.
2. GitHub: add secrets `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (optional `HF_TOKEN`).
3. Import the repo as a Databricks Git folder. The full click-by-click guide is `docs/Frontier_Bronze_Run_Guide.md`.

### Normal incremental load (Mon/Wed/Fri)
- **Collect:** automatic every Monday, Wednesday and Friday at 03:00 UTC (08:00 Pakistan time), or Actions → *Frontier data collector* → *Run workflow* → `incremental`. (First time only: run `smoke`, then `full` once.)
- **Load:** run `spark/bronze/frontier_bronze_load` with **no parameters** (this is what the Databricks Job does). It picks up every manifest in `new/`, loads it, and moves loaded files to `processed/`.

### Backfill from any point to any point
Run the same notebook with parameters (job parameters or widgets). Nothing in the code changes, and repeating a backfill is safe because `MERGE INTO` never creates duplicates. Batches are always loaded **one at a time, oldest first**, as they originally arrived.

Batch ids look like `20261005T030000Z_incremental`. `from_batch` and `to_batch` accept a full id or just the timestamp prefix, and both ends are inclusive.

| Goal | Parameters |
|---|---|
| From batch X to batch Y | `source_dir=processed`, `from_batch=20261001`, `to_batch=20261008T030000Z`, `force_reload=true` |
| Everything from batch X onward | `source_dir=processed`, `from_batch=20261005T030000Z`, `force_reload=true` |
| Everything up to batch Y | `source_dir=processed`, `to_batch=20261005T030000Z`, `force_reload=true` |
| One batch | `source_dir=processed`, `batch_id=<batch id>`, `force_reload=true` |
| A date range (day the batch was collected) | `source_dir=processed`, `start_date=2026-09-01`, `end_date=2026-09-30`, `force_reload=true` |
| Only Arena, all history | `source_dir=processed`, `source=arena`, `force_reload=true` |
| Only incremental batches of October | `source_dir=processed`, `load_type=incremental`, `start_date=2026-10-01`, `end_date=2026-10-31`, `force_reload=true` |
| Another folder of files | `source_dir=/Volumes/<catalog>/landing/staging/frontier/<folder>` (needs its own `_manifests` folder) |

When `source_dir` is not `new`, files are not moved. `force_reload=false` skips a file whose SHA-256 was already loaded successfully.

### Replay from the start (rebuild Bronze without a full load)
Use this when Bronze was emptied, damaged, or you want to see the data build up again: set `replay=true` (plus `bronze_schema` if you rebuild a different schema). The notebook then reads `processed/`, takes **every batch in the order it arrived**, and loads each one separately, so the first batch is the original full load and the later ones are the incremental batches. Nothing is downloaded again. Each batch writes its own log rows. `replay` always runs with `force_reload`, so it also works after `TRUNCATE TABLE`. Add `from_batch` / `to_batch` to replay only part of the history. After a replay, run the notebook once normally to pick up batches still waiting in `new/`.

### Primary keys
Each Bronze table has the key `record_hash`. Inside a chunk, duplicates are removed before the insert-only `MERGE`; across chunks and runs the `MERGE` skips keys that already exist. After every load that inserted rows, or any backfill or replay, the notebook counts duplicate and null keys in all four Bronze tables, writes one row per table to the log (`Bronze data quality (primary key)`) and fails the run if it finds a problem. Set the Databricks job to **one concurrent run** so two loads never write at the same time.

### Proof that it works
Run `spark/bronze/frontier_bronze_proof` after the first full load and keep the saved outputs (or record them). It shows, in order: explicit schemas, a MERGE re-run with identical row counts, a backfill by date and by batch, a batch-range backfill and a replay from the first batch (including a rebuild after `TRUNCATE`), a schema change (new column evolves the table, type change is quarantined, batch finishes), an empty run and a bad-parameter run both logged, and the log tables. Its drift test writes only to a separate `bronze_test` schema.

## 7. Notes and limits
- Bronze keeps original values; casting and cleaning happen in Silver.
- **Schedule:** Mon/Wed/Fri was chosen after checking the sources: Epoch is updated daily, BenchLM 1-3 times a week, Arena and CooperBench irregularly. Three runs a week catch Epoch quickly and match BenchLM.
- **Scheduled runs and the 1 MB rule:** on a run where the changes are under 1 MB, the collector refuses the batch (exit code 2), logs it, leaves the state untouched and removes the staged files. The workflow shows this as a **warning**, not a failure, and the changes are included in the next scheduled batch.
- A day with no changes at all ends with "No changes since the last run" and uploads nothing.
- BenchLM and Epoch are stored as full snapshots, but only when the file content changed (SHA-256), so unchanged days add no rows.
- GitHub disables scheduled workflows after 60 days without repository activity; re-enable it on the Actions page if that happens.
- The collector pins its CLI action to a release tag (`databricks/setup-cli@v0.299.0`).
