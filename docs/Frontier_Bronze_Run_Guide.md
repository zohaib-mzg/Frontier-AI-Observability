# Frontier AI Observatory – GitHub Structure, Full Run Flow, Backfill, Replay and Bronze Proof (Final)

**Scope of this stage:** staging area + Raw → Bronze, fully implemented. The `silver` and `gold` schemas exist but are **empty on purpose**; they are built in later phases.
**Schedule:** Monday, Wednesday and Friday at 03:00 UTC (08:00 Pakistan time).
**Notebooks:** Databricks notebooks are delivered as `.ipynb` files.

---

## 1. GitHub repository structure

```
Frontier-AI-Observability/                 (your GitHub repo)
├── .github/
│   └── workflows/
│       └── frontier-collect.yml           GitHub Actions robot: smoke / full / incremental (Mon/Wed/Fri)
├── ingestion/
│   └── frontier_collect.py                Collector: sources -> Databricks staging volume, manifest, state
├── notebooks/
│   └── 00_setup_databricks.ipynb          One-time setup: 5 schemas + staging volume + folders
├── spark/
│   └── bronze/
│       ├── frontier_bronze_load.ipynb     Raw -> Bronze (parameters, backfill, replay, key check, logs)
│       └── frontier_bronze_proof.ipynb    Proof notebook (section 4)
├── docs/
│   └── Frontier_Bronze_Run_Guide.md       this guide
├── proof/                                 Executed notebooks with saved outputs (section 5)
├── sample_data/                           small samples only (GitHub limit: 100 MB per file)
├── requirements.txt                       requests, pyarrow
├── .gitignore                             never commit tokens or output/ files
└── README.md                              data models + run instructions
```

Inside Databricks:

```
workspace (catalog)
├── landing.staging (volume)   STAGING AREA
│       frontier/ new/ (waiting)  processed/ (loaded, replayable)  quarantine/ (set aside)  _state/  logs/
├── bronze   arena_leaderboard_raw, cooperbench_member_raw, benchlm_raw, epoch_models_raw
├── silver   (empty, reserved for the next phase)
├── gold     (empty, reserved for the next phase)
├── ops      pipeline_execution_logs, error_logs, quarantine_log, schema_drift_log
├── bronze_test / bronze_replay_test   (created only by the proof notebook, can be dropped)
```

### Stage map: where data is at each stage and where it is logged

| Stage | What happens | Where you see it | Logged in |
|---|---|---|---|
| 1. Source → staging | GitHub robot downloads, checks size rules, uploads data, then manifest, then state | `landing.staging/frontier/new/` | Collector run + error files, sent to Databricks: `ops.pipeline_execution_logs` (layer `Source-to-Landing`) and `ops.error_logs` |
| 2. Staging area | `new/` = waiting, `processed/` = loaded and kept for replay, `quarantine/` = set aside | folder counts shown by the Bronze health check cell | per file in `ops.pipeline_execution_logs` and `ops.quarantine_log` |
| 3. Raw → Bronze | Explicit schemas, insert-only `MERGE` on `record_hash`, drift handling, primary key check | `bronze.*` tables | `Raw-to-Bronze` (file), `Raw-to-Bronze (batch)`, `Raw-to-Bronze (run)`, `Bronze data quality (primary key)`; `ops.schema_drift_log`; `ops.quarantine_log`; `ops.error_logs` |
| 4. Silver, 5. Gold | not built in this stage | empty schemas | – |

---

## 2. Full flow: from zero to a working Bronze layer

### Step 1 – One-time setup (click-by-click instructions in section 2A)
1. **Databricks:** run `notebooks/00_setup_databricks` once. It creates the schemas `landing`, `bronze`, `silver`, `gold`, `ops`, the volume `/Volumes/workspace/landing/staging/frontier` and its folders. (If your catalog is not called `workspace`, change the `catalog` widget in every notebook and `VOLUME` in the workflow.)
2. **GitHub secrets** (repo → Settings → Secrets and variables → Actions): `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, optional `HF_TOKEN`.
3. **Push the code** to GitHub using the structure above.
4. **Connect the repo to Databricks:** Workspace → Create → Git folder → paste your repo URL. The `.ipynb` files open as notebooks.

### Step 2 – Smoke test (checks the sources, uploads nothing)
GitHub → Actions → **Frontier data collector** → Run workflow → `mode = smoke`.
Read the printed file structures and compare them with `ARENA_CONTRACT` and `EPOCH_CONTRACT` in the Bronze notebook (Cell 2). Edit the lists if they differ, then commit (see "Columns to verify", section 7).

### Step 3 – Full load (once only, minimum 400 MB)
Actions → Run workflow → `mode = full`.
This is the **first batch**. The robot uploads the data, then the manifest, and saves the state last. A second full load is blocked on purpose. Rebuilding Bronze later never needs another full download: use the replay in Step 9.

### Step 4 – Raw to Bronze
Open `spark/bronze/frontier_bronze_load` and press **Run all** (default widgets).
The last cells show the staging area counts, row counts per Bronze table, the latest log rows, quarantined files, drift events and open errors. Loaded files move from `new/` to `processed/`. At the end of a load, the notebook checks every Bronze table for duplicate or empty `record_hash` and writes one log row per table.

### Step 5 – Run the proof notebook (section 4) and save its outputs (section 5)

### Step 6 – Automatic schedule (Monday, Wednesday, Friday)
- **GitHub robot:** cron `0 3 * * 1,3,5`, incremental mode, minimum 1 MB.
- **Databricks Job:** Workflows → Create job → Notebook task → `spark/bronze/frontier_bronze_load` → no parameters → Mon/Wed/Fri 04:00 UTC (cron `0 0 4 ? * MON,WED,FRI`, timezone UTC). **Set "Maximum concurrent runs" to 1**, so two loads never write at the same time.

### Step 7 – Normal incremental load (manual)
1. Actions → Run workflow → `mode = incremental`.
2. In Databricks run `frontier_bronze_load` with no parameters.

If the changes are under 1 MB the collector refuses the batch (exit code 2), logs it, deletes the staged files and does not move the state. GitHub shows a **warning**, not a failure, and the changes go into the next batch.

### Step 8 – Backfill from any point to any point
Run `frontier_bronze_load` with parameters. No code edit is needed. Batches are always loaded **one at a time, oldest first, as they originally arrived**, each with its own log rows. Repeating a backfill is safe (`MERGE INTO`, no duplicates).

Batch ids look like `20261005T030000Z_incremental`. `from_batch` and `to_batch` accept a full id or only the timestamp prefix; both ends are inclusive.

| Goal | Parameters |
|---|---|
| From batch X to batch Y | `source_dir=processed`, `from_batch=20261001`, `to_batch=20261008T030000Z`, `force_reload=true` |
| Everything from batch X onward | `source_dir=processed`, `from_batch=20261005T030000Z`, `force_reload=true` |
| Everything up to batch Y | `source_dir=processed`, `to_batch=20261005T030000Z`, `force_reload=true` |
| One batch | `source_dir=processed`, `batch_id=<batch id>`, `force_reload=true` |
| A date range (day the batch was collected) | `source_dir=processed`, `start_date=2026-09-01`, `end_date=2026-09-30`, `force_reload=true` |
| The batch of one day | `source_dir=processed`, `start_date=2026-10-12`, `end_date=2026-10-12`, `force_reload=true` |
| Only Arena, all history | `source_dir=processed`, `source=arena`, `force_reload=true` |
| Only incremental batches of October | `source_dir=processed`, `load_type=incremental`, `start_date=2026-10-01`, `end_date=2026-10-31`, `force_reload=true` |
| Another folder of files | `source_dir=/Volumes/workspace/landing/staging/frontier/<folder>` (needs its own `_manifests` folder) |

All parameters: `catalog`, `bronze_schema`, `source`, `load_type`, `source_dir`, `batch_id`, `from_batch`, `to_batch`, `start_date`, `end_date`, `force_reload`, `replay`. Ranges can be combined (for example a batch range plus `source=arena`). A wrong parameter (bad date, `from_batch` later than `to_batch`) is written to the log as a FAILURE row.

When `source_dir` is not `new`, files are not moved. With `force_reload=false` a file whose SHA-256 was already loaded is skipped.

### Step 9 – Replay from the start (rebuild without a full load)
Use it when Bronze was emptied or damaged, or when you want to see the data build up again as it originally did.

1. Run `frontier_bronze_load` with `replay=true` (add `from_batch` / `to_batch` for only part of the history).
2. The notebook reads `processed/`, takes **every batch in arrival order** (first the original full batch, then each incremental one) and loads them **one by one**. Nothing is downloaded again.
3. Each batch has its own log rows, so you can see how many rows each batch added. Rows already present are skipped by the `MERGE`, so there are no duplicates.
4. Then run the notebook once normally to pick up anything still waiting in `new/`.

`replay` always runs with `force_reload`, so it also works right after `TRUNCATE TABLE`. Only if the files in `processed/` are really gone would you need a new full collection (then delete `_state/state.json` first; do not do this otherwise).

### Step 10 – "I am at some stage and something went wrong: what now?"

| Situation | What to do |
|---|---|
| Collector failed (GitHub run is red) | Nothing was uploaded and the state did not move. Fix the cause, run the workflow again. |
| Data is in `new/` but Bronze has not run | Run `frontier_bronze_load` normally. |
| Bronze loaded wrongly or incompletely for a period | Backfill that period (Step 8) with `force_reload=true`. |
| Bronze tables emptied or dropped | `replay=true` (Step 9). |
| A file landed in `quarantine/` | Read `ops.quarantine_log` and `ops.schema_drift_log`, fix the cause (for example the contract), put the file path in `RELEASE_FILES` (last cell of the notebook), run that cell, then run the notebook again. |
| Run failed halfway | Run it again. Finished files are skipped; an unfinished file is repeated and the `MERGE` adds only what is missing. |
| You want to be sure there are no key problems | Look at the `Bronze data quality (primary key)` rows in `ops.pipeline_execution_logs`. |

### Where to look when something goes wrong
| Question | Table |
|---|---|
| What ran, when, how many rows? | `ops.pipeline_execution_logs` |
| What failed and why? | `ops.error_logs` |
| Which files were set aside? | `ops.quarantine_log` (with `record_sample`) |
| Did a source change shape? | `ops.schema_drift_log` |

---

## 2A. How to run the setup in Databricks (click by click)

Menu names can differ slightly between Databricks versions; the idea is the same. Free Edition has **serverless compute only**, which is what these notebooks use.

### A. Find your catalog name and workspace URL
1. Open your Databricks workspace. The browser address up to `.com` is your **host**, for example `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`.
2. Left sidebar → **Catalog**. Note the catalog name (normally `workspace`). If it is different, use that name in the `catalog` widget of every notebook and in `VOLUME` inside `.github/workflows/frontier-collect.yml`.

### B. Bring the GitHub repo into Databricks
1. Left sidebar → **Workspace** → **Create** → **Git folder**.
2. Paste the repo URL, choose **GitHub**, click **Create Git folder**. A private repo needs your GitHub account linked first (profile → Settings → Linked accounts).
3. The `.ipynb` files appear as notebooks. After you push changes to GitHub, open the Git folder, click the branch name and press **Pull**.
4. If you prefer not to use a Git folder: Workspace → **Import** → upload the `.ipynb` files. Keep `frontier_bronze_load` and `frontier_bronze_proof` in the same folder, because the proof notebook calls `./frontier_bronze_load`.

### C. Run the one-time setup notebook
1. Open `notebooks/00_setup_databricks`, choose compute **Serverless**, keep the `catalog` widget at `workspace` (or your catalog name).
2. Click **Run all**.
3. Success: `Volume ready: /Volumes/workspace/landing/staging/frontier`, a list of folders, and five schemas (`landing`, `bronze`, `silver`, `gold`, `ops`).
4. Check: **Catalog** → `workspace` → `landing` → `staging` → `frontier`.

**Do this before the first GitHub run.** The workflow's first step lists that volume and stops with an error if it does not exist.

### D. Create the Databricks token for GitHub
1. Profile (top right) → **Settings** → **Developer** → **Access tokens** → **Manage** → **Generate new token**.
2. Name it `github-actions`, choose a lifetime (for example 90 days), generate it and copy it now (shown once).
3. GitHub: repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**: `DATABRICKS_HOST` = the address from step A, `DATABRICKS_TOKEN` = the token.
4. Never paste the token in code, README, chat or a screenshot. When it expires, create a new one and update the secret.

### E. Collect the data from GitHub
1. Actions → **Frontier data collector** → Run workflow → `smoke`; read the log and compare the columns with the contracts.
2. Run workflow → `full` (once only).
3. In Databricks: **Catalog** → `workspace` → `landing` → `staging` → `frontier` → `new`. You should see `arena/`, `cooperbench/`, `benchlm/`, `epoch/`, `_manifests/` and `_state/state.json`.

### F. Run Raw to Bronze
1. Open `spark/bronze/frontier_bronze_load`, compute **Serverless**. Leave the widgets at their defaults (they appear after the first cell runs).
2. **Run all**. The full load takes a while.
3. Check: **Catalog** → `workspace` → `bronze` (four tables) and `ops` (four log tables), or run `SELECT * FROM workspace.ops.pipeline_execution_logs ORDER BY load_timestamp DESC` in a new cell.

### G. Run the proof notebook
1. Open `spark/bronze/frontier_bronze_proof`, compute **Serverless**, **Run all**. It writes only to the test schemas `bronze_test` and `bronze_replay_test`.
2. Save the executed notebook (section 5).

### H. Schedule the Bronze job (Mon/Wed/Fri)
1. **Jobs & Pipelines** → **Create** → **Job**.
2. Task `raw_to_bronze`, type **Notebook**, source **Workspace**, path = `frontier_bronze_load` inside your Git folder, compute **Serverless**, no parameters.
3. **Schedules & Triggers** → **Add trigger** → **Scheduled** → **Advanced** (cron): `0 0 4 ? * MON,WED,FRI`, timezone **UTC**.
4. Set **Maximum concurrent runs** to **1**. Save, then test with **Run now**.

### Common problems
| Message or symptom | Fix |
|---|---|
| `databricks fs ls` fails in GitHub Actions | Setup notebook not run, wrong catalog in `VOLUME`, or wrong `DATABRICKS_HOST` / `DATABRICKS_TOKEN` secret |
| Invalid access token / 401 | Token expired or copied with spaces; generate a new one and update the secret |
| "Batches to process: 0" | The collector has not uploaded a batch yet, or `source_dir` / the batch range is wrong |
| Notebook changes missing in Databricks | Push to GitHub, then **Pull** in the Git folder |
| Proof notebook cannot find the Bronze notebook | Keep both in `spark/bronze/`, or set the `bronze_notebook` widget to the full path |
| Run fails with "Primary key problem" | Read the `Bronze data quality (primary key)` log row for the table, then fix the cause and run a backfill with `force_reload=true` |

---

## 3. What the pipeline does for each rubric item (Bronze stage)

| Rubric item | What happens |
|---|---|
| Infrastructure and GitHub | Databricks Free Edition; all code in GitHub with the structure in section 1 |
| Data models and README | `README.md` documents every Bronze table (columns, types, primary key) and the `ops` tables. Silver is added with the Silver layer |
| Explicit schemas | `StructType` contracts, no `inferSchema` anywhere |
| `load_timestamp` | on every Bronze and `ops` row, set when the batch is written |
| Bronze provenance | `source_file`, `batch_id`, `collection_timestamp`, `load_type` |
| MERGE and idempotency | insert-only `MERGE INTO` on `record_hash`; same input twice adds nothing |
| Parameters and backfill | any batch range, date range, source, load type or folder (Step 8) |
| Replay from the start | `replay=true`, batch by batch in arrival order, no full load (Step 9) |
| No primary key problems | `MERGE` + de-duplication + a key check logged after every load |
| Schema drift | new Arena column evolves the table; type change quarantines the file (with a sample) and the batch finishes |
| Execution logs for every stage | collector, file, batch, run and key-check rows (stage map, section 1) |
| Code and run guide | `README.md` and this guide |

---

## 4. What is the Bronze proof, and why do we need it?

`frontier_bronze_proof` is a second notebook that **runs the real Bronze notebook with different parameters** and prints evidence of every feature. It fakes nothing; it only calls `frontier_bronze_load` and queries the results.

| Proof | What it shows | Evidence you get |
|---|---|---|
| 1. Explicit schemas | Column names and types of the four Bronze tables | `DESCRIBE TABLE` output |
| 2. MERGE idempotency | Same batch loaded twice more with `force_reload=true` | Identical row counts before / after 1 / after 2, zero duplicate `record_hash`, an `assert` that stops if they differ |
| 3. Backfill | An older period loaded by date, and by batch + source | Log rows with the parameters used |
| 4. Schema drift | Two generated Arena files: one adds a column, one changes `rating` to text | New column appears, the second file is quarantined with a sample, the batch still ends normally |
| 4B. Batch range and replay | Three small test batches with overlapping rows: only batch 2 loaded alone (60,000 rows); replay from the first batch (120,000 rows); a second replay (unchanged); `TRUNCATE` then replay (120,000 rows again) | Row-count asserts, per-batch log rows in arrival order, primary key check rows |
| 5. Every run is logged | A run that matches no batch, and a run with an invalid date | Log rows for both (SUCCESS with 0 files, FAILURE with the reason) |
| 6. Log table | The latest 30 rows of `pipeline_execution_logs` and the error table | Layer, file/parameter, start/end, status, rows inserted/updated |

**Why we need it**
1. **The rubric asks for it.** "What to submit" requires proof of explicit schemas, a MERGE re-run with no duplicates, a backfill, how a schema change is handled, and the log table.
2. **Grading is on what the pipeline does, not what the README says.** A feature that cannot be seen running gets no marks.
3. **It is repeatable.** Anyone can re-run it and get fresh evidence.
4. **It is safe.** Tests write only to `bronze_test` and `bronze_replay_test`, never to your real Bronze tables. Set the `cleanup` widget to `true` to drop them afterwards.

---

## 5. Notebooks as `.ipynb` and how to keep the proof

You asked for `.ipynb`, so all Databricks notebooks are `.ipynb` files. Databricks opens them directly (from a Git folder or via Import).

| Good | Watch out for |
|---|---|
| Native notebook format, outputs can be saved with the notebook, easy to export as HTML | An `.ipynb` is a large JSON file, so GitHub diffs are hard to read |
| Works the same in Databricks, VS Code and Jupyter | Saved outputs can contain data, paths or even secrets. Check before committing |

**Keeping the proof**
1. Run `frontier_bronze_load` and then `frontier_bronze_proof` in Databricks (all cells finished, no errors).
2. In each notebook: **File → Export → IPython Notebook (.ipynb)** or **HTML**. The export contains the outputs. (A Git folder commit may not include outputs, depending on the workspace setting, so export manually to be sure.)
3. Save the exports in `proof/` (for example `proof/frontier_bronze_proof.ipynb`), check that no output shows a token or secret, then commit and push.

Or record the ~5 minute video showing the proofs and link it in the README.

---

## 6. Why Monday, Wednesday, Friday?

I checked how often each source really updates (10 Oct 2026):

| Source | What the source says / what I saw | Daily? |
|---|---|---|
| **Epoch AI** `all_ai_models.csv` | Documentation: the CSV is "updated daily"; entries are added "near-daily". | **Yes** |
| **BenchLM** | "Several times per week" (data page) and "1-3 times per week" (embed/API page). | No |
| **LMArena** `leaderboard-dataset` | No frequency stated. The `latest` split showed publish date 2026-10-02, eight days earlier. | No (irregular) |
| **CooperBench** `team-trajectories` | A research dataset, not a feed; "updated 4 days ago" at one look and "9 days ago" at another. | No (occasional) |

Three runs a week match BenchLM and catch Epoch within one or two days. Runs that find nothing new only compare fingerprints and stop.

| Situation | Result |
|---|---|
| Nothing changed at the sources | "No changes since the last run", nothing uploaded, logged as SUCCESS |
| Changes of 1 MB or more | Batch uploaded, state saved last, Bronze loads it at 04:00 UTC |
| Changes under 1 MB | Batch refused and logged (`VolumeRuleNotMet`), staged files removed, GitHub warning, state not moved; included in the next batch |
| Collector or download fails | Run fails, nothing uploaded, state not moved; the next scheduled run retries |
| Two batches before the Bronze job runs | Both manifests sit in `new/` and load in order |

---

## 7. Things to know

1. **Epoch storage growth.** Epoch rows are keyed on `snapshot + row`, so each time the Epoch file changes (up to three times a week) the whole file is stored again. If this becomes a problem, change the Epoch key to `payload_hash`.
2. **Columns to verify.** The Arena dataset card lists `rating_lower` / `rating_upper` for the leaderboard subsets and `score`, `score_ci_lower`, `score_ci_upper`, `observation_count`, `session_count` for the `agent` subsets. `ARENA_CONTRACT` uses `rating_q975`, `rating_q025`, `vote_count`. Compare with your smoke-test output. A mismatch does not crash the pipeline (new columns evolve the table, missing ones are logged and left null) but the contract columns would be empty for those files. Also extend `EPOCH_CONTRACT` (10 core column names) from the real Epoch header.
3. **Replay order.** Replay uses the arrival order recorded in the manifests (`created_at`, then batch id). Bronze never overwrites rows, so the order only affects `load_timestamp`; it does not change which rows exist.
4. **Free Edition limits.** If you reach the compute quota, run the Bronze job once or twice a week. A replay of the full history includes the large CooperBench archives and can take long; use `from_batch` / `to_batch` or `source` to replay in parts.
5. **GitHub disables scheduled workflows after 60 days without repository activity.** Re-enable it on the Actions page if the schedule stops.
6. **Not tested from here.** I could not run Databricks, GitHub Actions or the real downloads. The notebook code compiles, and the batch-range logic was tested on its own, but the first real run of `MERGE INTO`, the replay and the key check is the proof notebook. Do one manual `incremental` run and read the Actions log before relying on the schedule.
7. **Silver and Gold are not built.** Their schemas exist and are empty. The rubric still requires Silver (casts, `MERGE` with updates, its own parameters and logs) for the final submission.

---

## 8. Final submission checklist (Bronze stage)

- [ ] GitHub repo link, structure as in section 1
- [ ] `spark/bronze/frontier_bronze_load.ipynb` and `ingestion/frontier_collect.py`
- [ ] `README.md` with Bronze models, incremental run, backfill and replay instructions
- [ ] `ARENA_CONTRACT` / `EPOCH_CONTRACT` checked against the smoke-test output
- [ ] Full load done once, Bronze notebook run, no open errors in `ops.error_logs`
- [ ] Proof notebook run; executed `.ipynb`/HTML in `proof/` (or video link in the README)
- [ ] Proof shows: schemas, MERGE re-run with equal counts, backfill, batch range, replay and rebuild, schema change, key check rows, log table
- [ ] Mon/Wed/Fri schedule active (GitHub workflow and the Databricks Job, one concurrent run)
- [ ] Silver and Gold (later phases; still required by the rubric for the final submission)
