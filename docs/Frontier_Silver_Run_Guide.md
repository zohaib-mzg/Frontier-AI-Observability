> **Edits to apply to `docs/Frontier_Silver_Guide.md`. This file is not a standalone guide.** It lists what to change in that guide after Silver was profiled, built and run.

# Silver Guide update

Apply these edits to `docs/Frontier_Silver_Guide.md`. Section numbers refer to the current guide. The guide was written before the Silver notebook existed; below is what changed after profiling and building.

## 1. Header and status

Replace the "Status of this document" paragraph with:

**Status of this document:** sections 1–3 describe the staging and Bronze work. Sections 5–9 describe the Silver layer, which is **built and has run** (notebook `spark/silver/frontier_silver_load.ipynb`, proof in `proof/frontier_silver_proof_*.ipynb`). Where the build differs from the original plan, section 5.5 below lists the final decisions.

In section 2 (data flow), remove "(to build)" after "Bronze → Silver".

## 2. Section 4, Rubric checklist - replace the Silver column

| Requirement | Silver |
|---|---|
| Databricks + GitHub structure | Done: `spark/silver/` |
| Data models and README | Done: README sections 8 and 9 |
| Explicit schemas, no `inferSchema` | Done: Silver reads Bronze tables; casting is explicit in the column maps (Cell 2) |
| Casting to proper types | Done: `try_cast`-based casting (Cell 5) |
| `load_timestamp` on every row | Done: all 8 Silver tables |
| Raw data with provenance | Done: lineage columns carried into every Silver table |
| Data quality | Done: cleaning, validation, duplicate rule, `ops.silver_quarantine_log` |
| `MERGE INTO`, same input twice → no change | Done: proofs 03, 04, 05 (0 inserted, 0 updated) |
| Parameters and backfills | Done: same widgets as Bronze; proof 06 |
| Schema drift without crashing | Done: `NEW_COLUMN_NOT_PROMOTED`, `MISSING_COLUMN`, `CAST_FAILURE`; demo cell |
| Execution logs, both steps | Done: layer `Bronze-to-Silver` |
| README with run instructions | Done (after the README update is merged) |
| Proof | Done: proofs 01 to 06; proof 07 (update path) after the first incremental batch |

## 3. Section 10, Plan - replace the step table and "Current position"

**Current position:** Bronze and Silver are built and have run. Remaining: the Silver task in the job (S6), the incremental-update proof, and the README and guide updates (S7).

| Step | Work | Status |
|---|---|---|
| S1 | Bronze finished | Done |
| S2 | Profile Bronze, confirm keys and limits | Done (decisions in 5.5) |
| S3 | Create the Silver tables and quarantine log | Done (tables are created from the column maps) |
| S4 | Build the Silver notebook | Done |
| S5 | Run and prove | Done: proofs 01 to 06 |
| S6 | Silver task after the Bronze task in the job | Add `bronze_to_silver` (depends on `raw_to_bronze`, one concurrent run), then do a **Run now** |
| S7 | README and guide, final proof export | In progress; add proof 07 after the first incremental batch (Monday 12 October) |
| S8 | Gold | Next phase |

## 4. New section 5.5, Final decisions after profiling and the build

| Topic | Original plan | Final decision |
|---|---|---|
| Arena key | (`subset`, `category`, `model_name`, `leaderboard_publish_date`), to confirm | Confirmed with exceptions: 198 groups have two different entries for one model in `webdev`. `rank` was **not** added to the key (it can change when a day is republished). The newest row wins (higher `vote_count` on a tie) and the other is quarantined as `DUPLICATE_BUSINESS_KEY` |
| Arena rating rule | `rating` above 0 | Above 0 except in the `agent` subset, whose scores are negative (checked: about 666 agent rows). 26 `text/korean` rows have no rating and stay quarantined |
| Epoch key | (`model`, `publication_date`), to confirm | Confirmed: no duplicates in the snapshot. 12 rows have no date and are quarantined |
| Epoch columns | "promote numeric fields after profiling" | 33 columns promoted (numbers, last-modified timestamp, yes/no flag); the rest stay in Bronze and are logged `NEW_COLUMN_NOT_PROMOTED`. `Training dataset size (total)` has 24 two-number values that become null (`CAST_FAILURE`) |
| BenchLM keys | chosen after profiling | `models`: `slug`; `benchmarks`: `benchmarkKey` **and** `category` (one benchmark can sit in two categories); `pricing`: `canonicalModelKey` (`slug` is empty for some models). No duplicates inside a snapshot |
| BenchLM parsing | explicit `StructType` and `from_json` | explicit column map with a fixed JSON path per column (`get_json_object`); nested objects kept as JSON text |
| CooperBench | members table, runs table "after sampling" | three tables: `members` (metadata, validity, hash), `evals` (from `eval.json`), `runs` (from `result.json`). File content stays in Bronze |
| Change detection | `bronze_record_hash` | Arena: `bronze_record_hash`. Epoch and BenchLM: `payload_hash` (the Bronze record hash changes with every snapshot). CooperBench: `content_hash` |
| Quarantine log | insert on every run | insert-only `MERGE` on a deterministic id, so a re-run adds nothing; records that later load into Silver are released from it |
| Booleans (6.1) | `true`/`false` only | also `yes`/`no` (Epoch `Open model weights?`) |
| MERGE (7) | `UPDATE SET *`, `category <=> ` | explicit column lists; `category` is a required key so `=` is used |
| Log label | `<bronze table>\|batch_id=<id>` | same, with `#models`, `#benchmarks`, `#pricing`, `#eval`, `#result` after the table name for the sources that share a Bronze table |

## 5. Section 6.2 and 6.3 - replace the arena `rating` row and the duplicate rule

- arena | `rating` not null and above 0 (any value for `agent`) | required | quarantine record
- **6.3** Several rows for the same business key in one input: the newest wins (`collection_timestamp`, then `vote_count` for Arena, then `batch_id`). The others are quarantined as `DUPLICATE_BUSINESS_KEY`, not dropped silently.

## 6. Section 11, Proof - replace with

1. **Explicit schemas:** Cell 2 of both notebooks; `DESCRIBE TABLE` of all 8 Silver tables in proof 01.
2. **MERGE re-run, no duplicates:** proofs 03, 04 and 05 (0 inserted, 0 updated) and the key check with 0 duplicates.
3. **Backfill:** proof 06 (`from_batch` and `to_batch`).
4. **Schema change:** the `NEW_COLUMN_NOT_PROMOTED` and `CAST_FAILURE` rows in `ops.schema_drift_log`, plus the demo cell (Cell 8) where `rating = 'n/a'`, `rating = 'abc'` and a bad date are quarantined and a negative `vote_count` becomes null.
5. **Log table:** `ops.pipeline_execution_logs` with the layers `Source-to-Landing`, `Raw-to-Bronze` and `Bronze-to-Silver`.
6. **Update path (proof 07):** after the first incremental batch, a normal Silver run with `rows_updated` above 0.

## 7. Section 12 - replace the open points and limits

- Records that disappear from a source are not deleted from Silver.
- Epoch rows without a publication date are quarantined (12).
- The update path (`WHEN MATCHED`) has not run on real data yet, because only one batch exists; it is covered by proof 07.
- Free Edition compute limits can stop long runs. A replay or a rerun is always safe because of the `MERGE`.
