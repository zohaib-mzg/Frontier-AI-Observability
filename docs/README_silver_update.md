# README update for the Silver layer

Two parts. **Part A** lists small edits to the existing README text (find the old text, replace it). **Part B** is new text: paste it after section 7 as sections 8 to 12. All numbers are from the runs of 10 October 2026.

---

## Part A - edits to the existing README

**A1. Title and status.** Replace the title with
`# Frontier AI Observatory – Raw → Bronze → Silver pipeline`
and replace the status quote with:

> **Status:** Staging (landing volume), Raw → Bronze and Bronze → Silver are implemented. The `gold` schema is created but empty; Gold is the next phase and will be documented here when it is built.

In the first paragraph, after "loads it with **PySpark + Delta** into the Bronze layer", add: ", then cleans, types and de-duplicates it into the Silver layer".

**A2. Diagram in section 1.** Replace the diagram with:

```
GitHub Actions (Mon/Wed/Fri 03:00 UTC)             Databricks
┌─────────────────────────────────┐     ┌───────────────────────────────────────────────┐
│ ingestion/frontier_collect.py   │     │ Volume  /Volumes/<catalog>/landing/staging/    │
│  smoke | full | incremental     │ ──▶ │   frontier/ new/  processed/  quarantine/      │
│  downloads, SHA-256, manifest   │     │             _state/  logs/                     │
└─────────────────────────────────┘     │                    │                           │
                                        │  spark/bronze/frontier_bronze_load.ipynb       │
                                        │                    ▼                           │
                                        │  <catalog>.bronze.*   (text only, insert-only) │
                                        │                    │                           │
                                        │  spark/silver/frontier_silver_load.ipynb       │
                                        │                    ▼                           │
                                        │  <catalog>.silver.*   (typed, MERGE upsert)    │
                                        │  <catalog>.ops.*      (logs, drift, quarantine)│
                                        └───────────────────────────────────────────────┘
```

**A3. Size rule (section 1).** Replace "a `full` load must land ≥ 400 MB" with "a `full` load must land ≥ 150 MB". The notebook and the Silver Guide use 150 MB (the first full load was about 192 MB).

**A4. Repository layout (section 2).** Add these lines, and change the setup line to "One-time setup: landing, bronze, silver, gold, ops schemas + volume (Silver tables are created by the Silver notebook)":

```
spark/silver/frontier_silver_load.ipynb   Bronze -> Silver notebook (PySpark, parameterised, backfill, replay, quarantine)
docs/Frontier_Silver_Guide.md              Silver design, decisions and plan
proof/frontier_silver_proof_*.ipynb       Executed Silver notebooks with saved outputs
```

**A5. Rubric table (section 3).** Add these rows:

| Feature | Where in the code | How to see it |
|---|---|---|
| Typed Silver tables, explicit casts, no `inferSchema` | column maps `ARENA_SILVER`, `EPOCH_MAP`, `BLM_*_MAP`, `COOP_*_MAP` (Cell 2); casting functions (Cell 5) | `DESCRIBE TABLE` in Silver proof 01 |
| Cleaning, validation and bad records | `clean()`, `arena_build()`, `epoch_build()`, `make_json_builder()`, `coop_members_build()` (Cell 5); `ops.silver_quarantine_log` | Silver proofs 01 and 02, demo cell |
| `MERGE INTO` upsert, same input twice changes nothing | `merge_upsert()` (Cell 6): inserts new keys, updates a key only when the content changed and the batch is not older | Silver proofs 03, 04 and 05 |
| Backfill and replay (same widgets as Bronze) | `pending_batches()` (Cell 7): `batch_id`, `from_batch`, `to_batch`, `start_date`, `end_date`, `source`, `force_reload`, `replay` | Silver proof 06 |
| No primary key problems | `key_check()` (Cell 6) after every load that changed rows, and after every backfill or replay | log layer `Silver data quality (primary key)` |
| Schema and type drift | `check_bronze_structure()`: `NEW_COLUMN_NOT_PROMOTED`, `MISSING_COLUMN`; casts that fail: `CAST_FAILURE`; bad records to quarantine | Cell 8 demo, `ops.schema_drift_log` |
| Execution log for the Silver step | `log_run()`: `Bronze-to-Silver` (table and batch), `(batch)`, `(run)` | layer query in Silver proof 05 |

**A6. Bronze types (section 4).** Bronze is text only. If your section 4 still shows `DOUBLE` or `BIGINT` for source columns (Arena `rating`, `variance`, `rating_q975`, `rating_q025`, `vote_count`, `rank`; CooperBench `member_size_bytes`), change them all to `STRING`. Only `collection_timestamp` and `load_timestamp` are `TIMESTAMP`.

**A7. Operations tables (section 4.5).** Add this row to the table:

| `silver_quarantine_log` | Records that Silver set aside | `quarantine_id`, `run_id`, `batch_id`, `silver_table`, `bronze_record_hash`, `column_name`, `bad_value`, `reason`, `raw_payload`, `quarantined_at`, `load_timestamp` |

Add to the list of layers: `Bronze-to-Silver` (one row per Bronze table and batch), `Bronze-to-Silver (batch)`, `Bronze-to-Silver (run)` and `Silver data quality (primary key)` (one per Silver table). Add to the drift event types: `NEW_COLUMN_NOT_PROMOTED` and `CAST_FAILURE`. Keep the sentence "`rows_updated` is always 0 in Bronze ... updates happen in Silver" and add after it: "In Silver, `rows_updated` counts rows whose content changed in a newer batch."

---

## Part B - new sections (paste after section 7)

## 8. Silver data models

Silver tables are Delta tables in `<catalog>.silver`. They hold **typed, validated, de-duplicated, current** data: one row per business key. Delta does not enforce keys; the pipeline does (`MERGE INTO` on the key, plus a key check after each load). Every table carries lineage (`bronze_record_hash`, `bronze_batch_id`, `source_file`, `collection_timestamp`) and `load_timestamp`. Bronze keeps the original text; Silver changes only through the column maps in Cell 2 of `spark/silver/frontier_silver_load.ipynb`.

Tables, keys and row counts after the first load (10 October 2026):

| Table | Primary key | Rows | Set aside (quarantine) |
|---|---|---|---|
| `silver.arena_leaderboard` | `subset`, `category`, `model_name`, `leaderboard_publish_date` | 2,200,576 | 224 (198 duplicate keys, 26 without rating) |
| `silver.epoch_models` | `model`, `publication_date` | 3,614 | 12 (no publication date) |
| `silver.benchlm_models` | `slug` | 889 | 0 |
| `silver.benchlm_benchmarks` | `benchmark_key`, `category` | 667 | 0 |
| `silver.benchlm_pricing` | `canonical_model_key` | 827 | 0 |
| `silver.cooperbench_members` | `study_setting`, `member_path` | 17,327 | 1 (content is not valid JSON) |
| `silver.cooperbench_evals` | `study_setting`, `member_path` | 2,584 | 0 |
| `silver.cooperbench_runs` | `study_setting`, `member_path` | 2,606 | 0 |

### 8.1 `silver.arena_leaderboard` - one model's rating in one subset and category on one publish date

| Column | Type | Source and rule |
|---|---|---|
| `subset` | STRING | Bronze `subset`. **Key.** One of `text`, `text_style_control`, `webdev`, `agent`, otherwise quarantined |
| `category` | STRING | Bronze `category`. **Key.** Missing: quarantined |
| `model_name` | STRING | Bronze `model_name`, cleaned. **Key.** Missing: quarantined |
| `model_key` | STRING | Join key for Gold: lower-case `model`/`model_name`, runs of non-alphanumeric characters replaced by `-`, leading and trailing `-` removed |
| `organization` | STRING | Bronze `organization`, cleaned |
| `license` | STRING | Bronze `license`, cleaned |
| `leaderboard_publish_date` | DATE | `yyyy-MM-dd` text cast to DATE. **Key.** Must be between 2023-01-01 and tomorrow, otherwise quarantined |
| `rating` | DOUBLE | `coalesce(rating, score)` (`agent` publishes `score`). Missing: quarantined. Must be above 0, except in `agent` where scores can be negative |
| `rating_variance` | DOUBLE | Bronze `variance`. Negative: set to null, logged |
| `rating_ci_lower` | DOUBLE | `coalesce(rating_q025, rating_lower, score_ci_lower)`. If lower > rating, upper < rating or lower > upper, both bounds are set to null |
| `rating_ci_upper` | DOUBLE | `coalesce(rating_q975, rating_upper, score_ci_upper)`. Same check as the lower bound |
| `vote_count` | BIGINT | Whole-number cast. Negative: null, logged |
| `observation_count` | BIGINT | Whole-number cast (`'29.0'` becomes 29). Negative: null, logged |
| `session_count` | BIGINT | Whole-number cast. Negative: null, logged |
| `rank` | BIGINT | Whole-number cast. Below 1: null, logged |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.2 `silver.epoch_models` - current state of one model in Epoch's database

Epoch publishes full snapshots. Silver keeps the latest version of each model. Models that disappear from the source are not deleted.

| Column | Type | Source and rule |
|---|---|---|
| `model` | STRING | Bronze `Model`. **Key.** Missing: quarantined |
| `model_key` | STRING | Join key for Gold: lower-case `model`/`model_name`, runs of non-alphanumeric characters replaced by `-`, leading and trailing `-` removed |
| `domain` | STRING | Bronze `Domain` |
| `task` | STRING | Bronze `Task` |
| `organization` | STRING | Bronze `Organization` |
| `authors` | STRING | Bronze `Authors` |
| `publication_date` | DATE | Bronze `Publication date` as a date. **Key.** Missing, invalid or after tomorrow: quarantined (12 rows today) |
| `reference` | STRING | Bronze `Reference` |
| `link` | STRING | Bronze `Link`. Not starting with `http`: null, logged |
| `citations` | BIGINT | Bronze `Citations` as a whole number. Negative: null, logged |
| `notability_criteria` | STRING | Bronze `Notability criteria` |
| `notability_criteria_notes` | STRING | Bronze `Notability criteria notes` |
| `parameters` | DOUBLE | Bronze `Parameters` as a number. Negative: null, logged |
| `training_compute_flop` | DOUBLE | Bronze `Training compute (FLOP)` as a number. Negative: null, logged |
| `training_dataset_size_total` | DOUBLE | Bronze `Training dataset size (total)` as a number. Negative: null, logged |
| `training_time_hours` | DOUBLE | Bronze `Training time (hours)` as a number. Negative: null, logged |
| `training_hardware` | STRING | Bronze `Training hardware` |
| `approach` | STRING | Bronze `Approach` |
| `confidence` | STRING | Bronze `Confidence` |
| `epochs` | DOUBLE | Bronze `Epochs` as a number. Negative: null, logged |
| `model_accessibility` | STRING | Bronze `Model accessibility` |
| `country_of_organization` | STRING | Bronze `Country (of organization)` |
| `base_model` | STRING | Bronze `Base model` |
| `finetune_compute_flop` | DOUBLE | Bronze `Finetune compute (FLOP)` as a number. Negative: null, logged |
| `hardware_quantity` | DOUBLE | Bronze `Hardware quantity` as a number. Negative: null, logged |
| `hardware_utilization_mfu` | DOUBLE | Bronze `Hardware utilization (MFU)` as a number |
| `last_modified` | TIMESTAMP | Bronze `Last modified` as a timestamp |
| `organization_categorization` | STRING | Bronze `Organization categorization` |
| `foundation_model` | STRING | Bronze `Foundation model` |
| `frontier_model` | STRING | Bronze `Frontier model` |
| `training_compute_cost_usd_2023` | DOUBLE | Bronze `Training compute cost (2023 USD)` as a number. Negative: null, logged |
| `training_power_draw_w` | DOUBLE | Bronze `Training power draw (W)` as a number. Negative: null, logged |
| `open_model_weights` | BOOLEAN | Bronze `Open model weights?`: `Yes`/`No` as a boolean |
| `hugging_face_developer_id` | STRING | Bronze `Hugging Face developer id` |
| `snapshot_id` | STRING | Bronze `snapshot_id` (lineage) |
| `payload_hash` | STRING | Bronze `payload_hash`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.3 `silver.benchlm_models` - one model in BenchLM's ranking (latest snapshot)

The nested objects (`scores`, `benchmarks`, `ranking`, `coverage`, `scoreInterval90`) are kept as JSON text in the `*_json` columns. `sourceResultTables` and `benchmarkSources` are not carried (logged as `NEW_COLUMN_NOT_PROMOTED`).

| Column | Type | Source and rule |
|---|---|---|
| `slug` | STRING | `slug`. **Key.** Missing: quarantined |
| `id` | STRING | `id` |
| `model` | STRING | `model` |
| `model_key` | STRING | Join key for Gold: lower-case `model`/`model_name`, runs of non-alphanumeric characters replaced by `-`, leading and trailing `-` removed |
| `canonical_model_key` | STRING | `canonicalModelKey` |
| `creator` | STRING | `creator` |
| `family` | STRING | `family` |
| `release_date` | DATE | `releaseDate` as a date (`yyyy-MM-dd`) |
| `reasoning_type` | STRING | `reasoningType` |
| `evidence_status` | STRING | `evidenceStatus` |
| `ranking_eligible` | BOOLEAN | `rankingEligible` as a boolean (`true`/`false`, also `yes`/`no`) |
| `market` | STRING | `market` |
| `is_regional` | BOOLEAN | `isRegional` as a boolean (`true`/`false`, also `yes`/`no`) |
| `source_type` | STRING | `sourceType` |
| `ordering_meaning` | STRING | `orderingMeaning` |
| `public_ranking_mode` | STRING | `publicRankingMode` |
| `overall_rank` | BIGINT | `overallRank` as a number |
| `display_score` | DOUBLE | `displayScore` as a number |
| `provisional_display_score` | DOUBLE | `provisionalDisplayScore` as a number |
| `context_window` | STRING | `contextWindow` |
| `context_window_tokens` | BIGINT | `contextWindowTokens` as a number. Negative: null, logged |
| `ranking_json` | STRING | nested value `ranking` kept as JSON text |
| `coverage_json` | STRING | nested value `coverage` kept as JSON text |
| `score_interval90_json` | STRING | nested value `scoreInterval90` kept as JSON text |
| `scores_json` | STRING | nested value `scores` kept as JSON text |
| `benchmarks_json` | STRING | nested value `benchmarks` kept as JSON text |
| `url` | STRING | `url` |
| `markdown_url` | STRING | `markdownUrl` |
| `snapshot_id` | STRING | Bronze `snapshot_id` (lineage) |
| `payload_hash` | STRING | Bronze `payload_hash`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.4 `silver.benchlm_benchmarks` - one benchmark in one category

| Column | Type | Source and rule |
|---|---|---|
| `benchmark_key` | STRING | `benchmarkKey`. **Key** (with `category`) |
| `category` | STRING | `category`. **Key** (with `benchmark_key`). The same benchmark can sit in two categories |
| `name` | STRING | `name` |
| `full_name` | STRING | `fullName` |
| `capability_category` | STRING | `capabilityCategory` |
| `category_label` | STRING | `categoryLabel` |
| `scored_category` | STRING | `scoredCategory` |
| `scoring_basis` | STRING | `scoringBasis` |
| `weight` | DOUBLE | `weight` as a number. Negative: null, logged |
| `decimals` | BIGINT | `decimals` as a number. Negative: null, logged |
| `year` | BIGINT | `year` as a number |
| `difficulty` | STRING | `difficulty` |
| `format` | STRING | `format` |
| `tasks` | STRING | `tasks` |
| `description` | STRING | `description` |
| `authors` | STRING | `authors` |
| `paper_title` | STRING | `paperTitle` |
| `paper_url` | STRING | `paperUrl` |
| `url` | STRING | `url` |
| `markdown_url` | STRING | `markdownUrl` |
| `successor_key` | STRING | `successorKey` |
| `successor_url` | STRING | `successorUrl` |
| `displayable_score_count` | BIGINT | `displayableScoreCount` as a number. Negative: null, logged |
| `evaluated_configuration_count` | BIGINT | `evaluatedConfigurationCount` as a number |
| `source_result_note` | STRING | `sourceResultNote` |
| `source_result_tables` | STRING | nested value `sourceResultTables` kept as JSON text |
| `snapshot_id` | STRING | Bronze `snapshot_id` (lineage) |
| `payload_hash` | STRING | Bronze `payload_hash`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.5 `silver.benchlm_pricing` - one model's price record

| Column | Type | Source and rule |
|---|---|---|
| `canonical_model_key` | STRING | `canonicalModelKey`. **Key.** (`slug` is empty for some models) |
| `slug` | STRING | `slug` |
| `model` | STRING | `model` |
| `model_key` | STRING | Join key for Gold: lower-case `model`/`model_name`, runs of non-alphanumeric characters replaced by `-`, leading and trailing `-` removed |
| `creator` | STRING | `creator` |
| `source_type` | STRING | `sourceType` |
| `context_window` | STRING | `contextWindow` |
| `context_window_tokens` | BIGINT | `contextWindowTokens` as a number. Negative: null, logged |
| `input_price` | DOUBLE | `inputPrice` as a number. Negative: null, logged |
| `output_price` | DOUBLE | `outputPrice` as a number. Negative: null, logged |
| `cached_input_price` | DOUBLE | `cachedInputPrice` as a number. Negative: null, logged |
| `training_price` | DOUBLE | `trainingPrice` as a number. Negative: null, logged |
| `score_per_output_dollar` | DOUBLE | `scorePerOutputDollar` as a number |
| `display_score` | DOUBLE | `displayScore` as a number |
| `overall_rank` | BIGINT | `overallRank` as a number |
| `has_numeric_pricing` | BOOLEAN | `hasNumericPricing` as a boolean (`true`/`false`, also `yes`/`no`) |
| `is_free_pricing` | BOOLEAN | `isFreePricing` as a boolean (`true`/`false`, also `yes`/`no`) |
| `note` | STRING | `note` |
| `url` | STRING | `url` |
| `markdown_url` | STRING | `markdownUrl` |
| `snapshot_id` | STRING | Bronze `snapshot_id` (lineage) |
| `payload_hash` | STRING | Bronze `payload_hash`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.6 `silver.cooperbench_members` - one file inside a CooperBench archive (metadata and validity)

The file content stays in Bronze (`raw_content`); Silver keeps the path parts, validity and a content hash. The outcome fields are in the two tables below.

| Column | Type | Source and rule |
|---|---|---|
| `study_setting` | STRING | Bronze `study_setting`. **Key.** One of `solo`, `coopgit`, `team`, `team_noproto`, otherwise quarantined |
| `member_path` | STRING | Bronze `member_path`. **Key.** Missing: quarantined |
| `member_extension` | STRING | Bronze `member_extension` (`.json`, `.jsonl`) |
| `member_size_bytes` | BIGINT | Whole-number cast. 0 or less: null, logged |
| `file_name` | STRING | Last part of the path |
| `file_type` | STRING | File name without extension; `agent1_traj.json` becomes `agent_traj`, `agent2_sent.jsonl` becomes `agent_sent` |
| `repo` | STRING | Path part `<repo>_task` (null for files above the task level, such as `summary.json`) |
| `task_id` | BIGINT | Path part with the task number |
| `feature_pair` | STRING | Feature pair taken from the path (`f3_f7`) |
| `is_valid_json` | BOOLEAN | JSON parses (every non-empty line for `.jsonl`). Not valid: quarantined |
| `line_count` | BIGINT | Number of non-empty lines, `.jsonl` only |
| `content_hash` | STRING | SHA-256 of `raw_content`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.7 `silver.cooperbench_evals` - the result of one task run (from `eval.json`)

| Column | Type | Source and rule |
|---|---|---|
| `repo` | STRING | `repo` |
| `task_id` | BIGINT | `task_id` as a number |
| `features` | STRING | nested value `features` kept as JSON text |
| `setting_label` | STRING | `setting` |
| `both_passed` | BOOLEAN | `both_passed` as a boolean (`true`/`false`, also `yes`/`no`) |
| `error` | STRING | `error` |
| `evaluated_at` | TIMESTAMP | `evaluated_at` as a timestamp |
| `apply_status_agent1` | STRING | `apply_status.agent1` |
| `apply_status_agent2` | STRING | `apply_status.agent2` |
| `merge_status` | STRING | `merge.status` |
| `merge_strategy` | STRING | `merge.strategy` |
| `feature1_passed` | BOOLEAN | `feature1.passed` as a boolean (`true`/`false`, also `yes`/`no`) |
| `feature1_feature_id` | BIGINT | `feature1.feature_id` as a number |
| `feature1_tests_passed` | BIGINT | `feature1.tests_passed` as a number. Negative: null, logged |
| `feature1_tests_failed` | BIGINT | `feature1.tests_failed` as a number. Negative: null, logged |
| `feature1_exit_code` | BIGINT | `feature1.exit_code` as a number |
| `feature2_passed` | BOOLEAN | `feature2.passed` as a boolean (`true`/`false`, also `yes`/`no`) |
| `feature2_feature_id` | BIGINT | `feature2.feature_id` as a number |
| `feature2_tests_passed` | BIGINT | `feature2.tests_passed` as a number. Negative: null, logged |
| `feature2_tests_failed` | BIGINT | `feature2.tests_failed` as a number. Negative: null, logged |
| `feature2_exit_code` | BIGINT | `feature2.exit_code` as a number |
| `study_setting` | STRING | Bronze `study_setting`. **Key** (with `member_path`) |
| `member_path` | STRING | Bronze `member_path`. **Key** (with `study_setting`) |
| `feature_pair` | STRING | Feature pair taken from the path (`f3_f7`) |
| `content_hash` | STRING | SHA-256 of `raw_content`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

### 8.8 `silver.cooperbench_runs` - the settings and costs of one task run (from `result.json`)

| Column | Type | Source and rule |
|---|---|---|
| `repo` | STRING | `repo` |
| `task_id` | BIGINT | `task_id` as a number |
| `features` | STRING | nested value `features` kept as JSON text |
| `setting_label` | STRING | `setting` |
| `run_id` | STRING | `run_id` |
| `run_name` | STRING | `run_name` |
| `agent_framework` | STRING | `agent_framework` |
| `model` | STRING | `model` |
| `started_at` | TIMESTAMP | `started_at` as a timestamp |
| `ended_at` | TIMESTAMP | `ended_at` as a timestamp |
| `duration_seconds` | DOUBLE | `duration_seconds` as a number. Negative: null, logged |
| `lead_agent` | STRING | `lead_agent` |
| `total_cost` | DOUBLE | `total_cost` as a number. Negative: null, logged |
| `total_steps` | BIGINT | `total_steps` as a number. Negative: null, logged |
| `messages_sent` | BIGINT | `messages_sent` as a number. Negative: null, logged |
| `log_dir` | STRING | `log_dir` |
| `agents_json` | STRING | nested value `agents` kept as JSON text |
| `metrics_json` | STRING | nested value `metrics` kept as JSON text |
| `study_setting` | STRING | Bronze `study_setting`. **Key** (with `member_path`) |
| `member_path` | STRING | Bronze `member_path`. **Key** (with `study_setting`) |
| `feature_pair` | STRING | Feature pair taken from the path (`f3_f7`) |
| `content_hash` | STRING | SHA-256 of `raw_content`. The MERGE updates a row only when this value changes |
| `bronze_record_hash` | STRING | Bronze primary key of the source row (lineage) |
| `bronze_batch_id` | STRING | Bronze `batch_id` (lineage) |
| `source_file` | STRING | Bronze `source_file` (lineage) |
| `collection_timestamp` | TIMESTAMP | Bronze `collection_timestamp`: when the collector fetched the source (lineage; decides which version is newer) |
| `load_timestamp` | TIMESTAMP | When this Silver row was inserted or last updated (`current_timestamp()`) |

## 9. Cleaning, validation and quarantine

Silver processes one Bronze table and one batch at a time, in this order. Each step is a rule in code and its counts are written to the log message.

1. **Select** the Bronze rows of the batch. Batches are taken in arrival order (`collection_timestamp`, then `batch_id`), never by `load_timestamp`, because a Bronze rebuild changes that.
2. **Clean text:** trim, collapse repeated spaces, turn empty strings and the markers `NaN`, `null`, `None`, `N/A`, `n/a`, `-` into null.
3. **Cast** with `try_cast`, so a bad value becomes null and never stops the batch:

| Type | Rule |
|---|---|
| DOUBLE | `try_cast(x AS DOUBLE)`; `NaN` and infinity become null. Epoch values like `3.55e+24` work |
| BIGINT | accepted only if the value is a whole number (`29.0` becomes 29, `29.5` fails) |
| DATE | `yyyy-MM-dd` only |
| TIMESTAMP | ISO text with or without offset (`2026-10-05 13:44:28+00:00`) |
| BOOLEAN | `true`, `false`, `yes`, `no` |

4. **Validate.** A failure on a **key or required** field sends the *record* to `ops.silver_quarantine_log`; a failure on an **optional** field sets that field to null, counts it in the log message and goes on.

| Table | Required (record quarantined) | Optional (field set to null) |
|---|---|---|
| arena | `subset` is `text`, `text_style_control`, `webdev` or `agent`; `category`, `model_name` present; publish date valid and between 2023-01-01 and tomorrow; `rating` present and above 0 (the `agent` subset may be negative) | CI bounds consistent with `rating` (both set to null together); counts ≥ 0; `rank` ≥ 1; variance ≥ 0 |
| epoch | `model` present; `publication_date` valid and not later than tomorrow | `citations` and the numeric fields ≥ 0; `link` starts with `http` |
| benchlm (3 tables) | payload is valid JSON; key fields present (`slug`; `benchmark_key` and `category`; `canonical_model_key`) | `context_window_tokens`, prices, `weight`, `decimals`, `displayable_score_count` ≥ 0 |
| cooperbench members | `study_setting` is one of the four settings; `member_path` present; content is valid JSON (every non-empty line for `.jsonl`) | `member_size_bytes` > 0 |
| cooperbench evals and runs | content is valid JSON; `study_setting` and `member_path` present | test counts, duration, cost, steps and messages ≥ 0 |

5. **Derive** helper columns (`model_key`, path parts, hashes).
6. **De-duplicate** inside the batch, then **MERGE**.

**Duplicates.** If several rows in one batch share a key, the newest wins (`collection_timestamp`, then for Arena the higher `vote_count`, then `batch_id`); the others are **not dropped silently**, they go to quarantine with reason `DUPLICATE_BUSINESS_KEY`. Today this affects 198 Arena rows: for the model `gpt-5.3-codex (codex-harness)` in `webdev`, Arena publishes two different entries (different ratings, votes and ranks) under the same name on each date.

**MERGE.**

```sql
MERGE INTO silver.<table> AS t USING <validated batch> AS s ON <key columns equal>
WHEN MATCHED AND s.collection_timestamp >= t.collection_timestamp
              AND NOT (t.<change column> <=> s.<change column>) THEN UPDATE SET <all non-key columns>
WHEN NOT MATCHED THEN INSERT (<all columns>) VALUES (<all columns>)
```

The change column is `bronze_record_hash` for Arena, `payload_hash` for Epoch and BenchLM, and `content_hash` for CooperBench. The same batch twice gives 0 inserted and 0 updated. An old batch (backfill) cannot overwrite a newer value because of the `collection_timestamp` condition. `rows_skipped_duplicate` counts unchanged rows.

**Quarantine.** `ops.silver_quarantine_log` has one row per record and reason. Its `quarantine_id` is a hash of table, record, column and reason, and it is written with an insert-only `MERGE`, so repeating a run never adds the same record twice. After a load, records that now exist in the Silver table are removed from the quarantine log (`released_from_quarantine` in the log message). This keeps the log correct after a rule is corrected.

**Schema and type drift** (all rows in `ops.schema_drift_log`, dataset `silver/<table>`):

| Event | What happens |
|---|---|
| Bronze or JSON gets a column Silver does not read | `NEW_COLUMN_NOT_PROMOTED`, the load continues. To promote it, add one line to the column map; the notebook then runs `ALTER TABLE ... ADD COLUMNS` and logs `NEW_COLUMN` |
| A mapped column is missing | Silver column is null, `MISSING_COLUMN` |
| A value does not cast | optional field: null and `CAST_FAILURE` with the row count; required field: record quarantined |

Known case: 24 Epoch rows have two numbers in `Training dataset size (total)` (for example `929000,929000`); the column is set to null and logged as `CAST_FAILURE`.

## 10. How to run Silver

**Setup:** nothing extra. Run `notebooks/00_setup_databricks` once as for Bronze; the Silver tables are created from the column maps the first time a source runs.

**Normal load (Mon/Wed/Fri, after the Bronze task):** run `spark/silver/frontier_silver_load` with **no parameters**. It processes every Bronze batch that has no `SUCCESS` row for it in `ops.pipeline_execution_logs` (layer `Bronze-to-Silver`). The Databricks job has two tasks: `raw_to_bronze`, then `bronze_to_silver` (depends on the first), with one concurrent run.

**Widgets** (same names as Bronze):

| Widget | Meaning |
|---|---|
| `catalog`, `bronze_schema`, `silver_schema` | default `workspace`, `bronze`, `silver` |
| `source` | `all`, `arena`, `epoch`, `benchlm`, `cooperbench` |
| `batch_id` | one batch |
| `from_batch`, `to_batch` | batch id or prefix, both ends included |
| `start_date`, `end_date` | by collection day |
| `force_reload` | `true` = also process batches Silver already logged (safe because of the `MERGE`) |
| `replay` | `true` = every batch from the first, one at a time, oldest first (always with `force_reload`) |

| Goal | Widgets |
|---|---|
| Backfill one period | `from_batch=20261005`, `to_batch=20261008`, `force_reload=true` |
| One batch | `batch_id=<id>`, `force_reload=true` |
| Only one source | `source=arena` (add `force_reload=true` to re-run it) |
| Rebuild Silver from the first batch | `replay=true` (after `TRUNCATE TABLE` on the Silver tables if needed) |
| Re-check that nothing changes | `force_reload=true`: every table must show 0 inserted and 0 updated |

Each table and batch writes a log row; a failed table is logged and the other tables still run, then the run row is `FAILURE` and the job fails.

## 11. Silver proof

Executed notebooks with saved outputs are in `proof/`:

| File | Shows |
|---|---|
| `frontier_silver_proof_01_first_load.ipynb` | explicit schemas (`DESCRIBE TABLE`), first load of BenchLM and CooperBench, quarantine, drift events, log rows |
| `frontier_silver_proof_02_arena_agent_rating_fix.ipynb` | a corrected rule: 666 agent rows loaded, the old quarantine entries released |
| `frontier_silver_proof_03_rerun_benchlm_idempotent.ipynb` | same input again: 0 inserted, 0 updated |
| `frontier_silver_proof_04_rerun_cooperbench_idempotent.ipynb` | same input again: 0 inserted, 0 updated |
| `frontier_silver_proof_05_rerun_all_idempotent.ipynb` | all 8 tables again: 0 inserted, 0 updated, 0 duplicate keys; log layers |
| `frontier_silver_proof_06_backfill_batch_range.ipynb` | run with `from_batch` and `to_batch` |

## 12. Silver notes and limits

- Rows that disappear from a source are not deleted from Silver.
- Epoch rows without a publication date cannot be keyed and are quarantined (12 today).
- `Organization` in Epoch is a comma-joined text; some organization names contain commas, so it is not split.
- Arena keeps the entry with more votes when a name appears twice on one date; the other is in the quarantine log.
- Bronze stays the place for the full original content (CooperBench `raw_content`, all Epoch columns Silver does not carry).
- The Free Edition compute limits can stop a long run. Re-running is always safe.
