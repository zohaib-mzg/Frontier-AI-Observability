"""
Frontier AI Observatory - source collector

Runs on GitHub Actions (or on your own computer for testing).

Modes
  smoke        Checks every source URL, downloads a few small files and prints their
               structure. Uploads nothing.
  full         One-time history load. Downloads every source, checks that the landed
               volume is at least 200 MB, writes a manifest.
  incremental  Downloads only sources that changed since the last run. The batch must
               be at least 1 MB, otherwise it is refused (and logged).

Every problem (HTTP failure, bad archive, volume rule broken, ...) is written to
  <out-dir>/logs/collector_errors_<stamp>.jsonl
and every run writes
  <out-dir>/logs/collector_run_<stamp>.json
Both files are uploaded to Databricks by the workflow, even when the run fails.

Exit codes: 0 = ok (or nothing new) | 1 = failure | 2 = volume rule not met
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import tarfile
import time
import traceback
import uuid
from datetime import datetime, timezone

import requests

MB = 1_000_000                       # decimal megabytes, as in the Phase 1 document
MIN_FULL_BYTES = 200 * MB            # full load must land at least this much
MIN_INCREMENTAL_BYTES = 1 * MB       # an incremental batch must land at least this much
MAX_RETRIES = 3
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 120
CHUNK = 1024 * 1024
USER_AGENT = "FrontierAIObservatory/1.0 (university data-engineering project)"

# Archive members that Bronze will load. Must match MEMBER_EXTENSIONS in the Bronze notebook.
MEMBER_EXTENSIONS = {".json", ".jsonl"}

HF = "https://huggingface.co/datasets"
ARENA_REPO = "lmarena-ai/leaderboard-dataset"
ARENA_FULL_REV = "7d2d9009b2ba84383deb6f1b70e66f85b7cda4e7"
ARENA_SUBSETS = ["text", "text_style_control", "webdev", "agent"]
ARENA_OPTIONAL_LATEST = {"webdev"}   # a missing "latest" file for these is only a warning

COOPER_REPO = "CooperBench/team-trajectories"
COOPER_REV = "dd371629174a226ecd251672af315699b77de05d"
COOPER_ARCHIVES = {
    "solo": "cmp-full-solo.tar.gz",
    "coopgit": "cmp-full-coopgit.tar.gz",
    "team": "cmp-full-team.tar.gz",
    "team_noproto": "cmp-full-team-noproto.tar.gz",
}

BENCHLM_URLS = {
    "models": "https://benchlm.ai/data/models.json",
    "benchmarks": "https://benchlm.ai/data/benchmarks.json",
    "pricing": "https://benchlm.ai/data/pricing.json",
}
EPOCH_URL = "https://epoch.ai/data/all_ai_models.csv"


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def arena_url(subset, split, revision):
    return f"{HF}/{ARENA_REPO}/resolve/{revision}/{subset}/{split}-00000-of-00001.parquet"


def cooper_url(filename, revision):
    return f"{HF}/{COOPER_REPO}/resolve/{revision}/{filename}"


# ---------------------------------------------------------------------------
# Error log (a JSON-lines file you can open later) + run summary
# ---------------------------------------------------------------------------

class ErrorLog:
    def __init__(self, path, batch_id):
        self.path = path
        self.batch_id = batch_id
        self.count = 0
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "a", encoding="utf-8").close()

    def add(self, stage, source, target, message, severity="ERROR", error_type="",
            http_status=None, attempt=None, exc=None):
        entry = {
            "error_id": uuid.uuid4().hex,
            "occurred_at": now_iso(),
            "batch_id": self.batch_id,
            "stage": stage,
            "source": source,
            "file_or_param": target,
            "severity": severity,
            "error_type": error_type or (type(exc).__name__ if exc else ""),
            "message": str(message)[:2000],
            "http_status": http_status,
            "attempt": attempt,
            "traceback": "".join(traceback.format_exception(exc))[-3000:] if exc else "",
        }
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
        if severity == "ERROR":
            self.count += 1
        print(f"{severity} | {stage} | {source} | {target} | {message}", flush=True)


# ---------------------------------------------------------------------------
# HTTP helpers (bounded retries, every failure is logged)
# ---------------------------------------------------------------------------

def make_session():
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    token = os.environ.get("HF_TOKEN", "").strip()      # optional, only if Hugging Face rate-limits you
    if token:
        session.headers["Authorization"] = "Bearer " + token
    return session


def fingerprint(session, url, errors, source):
    """Cheap change detector. Returns (fingerprint, size, status). Never raises."""
    current = url
    try:
        for _ in range(5):
            resp = session.head(current, allow_redirects=False, timeout=(CONNECT_TIMEOUT, 30))
            linked = resp.headers.get("X-Linked-Etag")
            if resp.status_code in (301, 302, 303, 307, 308) and not linked:
                location = resp.headers.get("Location", "")
                if location.startswith("/"):
                    current = "https://huggingface.co" + location
                    continue
                if location.startswith("https://huggingface.co"):
                    current = location
                    continue
            etag = linked or resp.headers.get("ETag")
            size = resp.headers.get("X-Linked-Size") or resp.headers.get("Content-Length")
            if resp.status_code >= 400:
                return None, None, resp.status_code
            return (etag.strip('"').replace("W/", "").strip('"') if etag else None,
                    int(size) if size and size.isdigit() else None,
                    resp.status_code)
    except Exception as exc:
        errors.add("fingerprint", source, url, exc, "WARNING", exc=exc)
    return None, None, None


def download(session, url, dest, errors, source, target):
    """Download to dest with retries. Returns {"size","sha256"} or None (error already logged)."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    part = dest + ".part"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as resp:
                if resp.status_code >= 400:
                    final = resp.status_code in (400, 401, 403, 404) or attempt == MAX_RETRIES
                    errors.add("download", source, target, f"HTTP {resp.status_code} for {url}",
                               "ERROR" if final else "WARNING", "HTTPError", resp.status_code, attempt)
                    if final:
                        return None
                    time.sleep(2 ** attempt)
                    continue
                digest = hashlib.sha256()
                size = 0
                with open(part, "wb") as out:
                    for chunk in resp.iter_content(CHUNK):
                        out.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
            expected = resp.headers.get("Content-Length")
            if expected and expected.isdigit() and int(expected) != size and not resp.headers.get("Content-Encoding"):
                raise IOError(f"Incomplete download: got {size} bytes, expected {expected}")
            os.replace(part, dest)
            return {"size": size, "sha256": digest.hexdigest()}
        except Exception as exc:
            last = attempt == MAX_RETRIES
            errors.add("download", source, target, f"{exc} ({url})", "ERROR" if last else "WARNING",
                       attempt=attempt, exc=exc)
            if not last:
                time.sleep(2 ** attempt)
    if os.path.exists(part):
        os.remove(part)
    return None


# ---------------------------------------------------------------------------
# Inspection helpers
# ---------------------------------------------------------------------------

def inspect_archive(path):
    """Streams through a .tar.gz. Returns (members, loaded_members, loaded_bytes, by_extension)."""
    members = loaded = loaded_bytes = 0
    by_ext = {}
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            if not member.isfile():
                continue
            members += 1
            ext = os.path.splitext(member.name)[1].lower() or "(none)"
            slot = by_ext.setdefault(ext, {"files": 0, "bytes": 0})
            slot["files"] += 1
            slot["bytes"] += member.size
            if ext in MEMBER_EXTENSIONS:
                loaded += 1
                loaded_bytes += member.size
    return members, loaded, loaded_bytes, by_ext


def first_members(path, count=5):
    names = []
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            if member.isfile():
                names.append(member.name)
                if len(names) >= count:
                    break
    return names


def parquet_schema_lines(path):
    import pyarrow.parquet as pq
    parquet = pq.ParquetFile(path)
    lines = [f"    {field.name}: {field.type}" for field in parquet.schema_arrow]
    return parquet.metadata.num_rows, lines


def csv_shape(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = sum(1 for _ in reader)
    return header, rows


# ---------------------------------------------------------------------------
# State = "what we already have" (kept in the Databricks Volume)
# ---------------------------------------------------------------------------

def read_state(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


# ---------------------------------------------------------------------------
# Work lists
# ---------------------------------------------------------------------------

def build_items(mode):
    """Every item the collector may download. Each item is a dict."""
    items = []
    for subset in ARENA_SUBSETS:
        if mode == "full":
            items.append({"key": f"arena/{subset}/full", "source": "arena", "dataset": subset,
                          "ext": "parquet", "url": arena_url(subset, "full", ARENA_FULL_REV),
                          "check_url": arena_url(subset, "full", ARENA_FULL_REV), "optional": False})
        else:
            items.append({"key": f"arena/{subset}/latest", "source": "arena", "dataset": subset,
                          "ext": "parquet", "url": arena_url(subset, "latest", "main"),
                          "check_url": arena_url(subset, "latest", "main"),
                          "optional": subset in ARENA_OPTIONAL_LATEST})
    for setting, filename in COOPER_ARCHIVES.items():
        revision = COOPER_REV if mode == "full" else "main"
        items.append({"key": f"cooperbench/{setting}", "source": "cooperbench", "dataset": setting,
                      "ext": "tar.gz", "url": cooper_url(filename, revision),
                      "check_url": cooper_url(filename, revision), "optional": False})
    for dataset, url in BENCHLM_URLS.items():
        items.append({"key": f"benchlm/{dataset}", "source": "benchlm", "dataset": dataset,
                      "ext": "json", "url": url, "check_url": None, "optional": False})
    items.append({"key": "epoch/all_ai_models", "source": "epoch", "dataset": "all_ai_models",
                  "ext": "csv", "url": EPOCH_URL, "check_url": None, "optional": False})
    return items


def stage_items(mode, state, session, out_dir, stamp, errors):
    """Downloads what is needed. Returns (staged_entries, checked_count, failed_count)."""
    previous = (state or {}).get("items", {})
    staged, failed, checked = [], 0, 0
    for item in build_items(mode):
        checked += 1
        old = previous.get(item["key"], {})
        remote_fp = None
        if item["check_url"]:
            remote_fp, _, status = fingerprint(session, item["check_url"], errors, item["source"])
            if status in (403, 404) and item["optional"]:
                errors.add("fingerprint", item["source"], item["key"],
                           f"Optional file not available (HTTP {status}); skipped", "WARNING")
                continue
            if mode == "incremental" and remote_fp and remote_fp == old.get("fingerprint"):
                print(f"Unchanged (fingerprint) | {item['key']}", flush=True)
                continue

        rel = f"{item['source']}/{item['dataset']}__{mode}__{stamp}.{item['ext']}"
        dest = os.path.join(out_dir, "upload", "new", rel)
        result = download(session, item["url"], dest, errors, item["source"], item["key"])
        if result is None:
            if item["optional"]:
                continue
            failed += 1
            continue
        if mode == "incremental" and result["sha256"] == old.get("sha256"):
            os.remove(dest)
            print(f"Unchanged (content) | {item['key']}", flush=True)
            continue

        entry = {"key": item["key"], "source": item["source"], "dataset": item["dataset"],
                 "path": rel, "size_bytes": result["size"], "sha256": result["sha256"],
                 "fingerprint": remote_fp, "landed_bytes": result["size"]}
        if item["ext"] == "tar.gz":
            try:
                members, loaded, loaded_bytes, by_ext = inspect_archive(dest)
                entry.update({"members": members, "loaded_members": loaded,
                              "landed_bytes": loaded_bytes, "by_extension": by_ext})
                print(f"Archive {item['key']} | members: {members} | JSON members: {loaded} | "
                      f"JSON bytes: {loaded_bytes / MB:.1f} MB", flush=True)
                if loaded == 0:
                    errors.add("inspect", item["source"], item["key"],
                               "Archive has no .json/.jsonl members; check MEMBER_EXTENSIONS", "ERROR")
                    failed += 1
                    continue
            except Exception as exc:
                errors.add("inspect", item["source"], item["key"], f"Cannot read archive: {exc}", exc=exc)
                failed += 1
                continue
        staged.append(entry)
        print(f"Staged | {item['key']} | {result['size'] / MB:.2f} MB", flush=True)
    return staged, checked, failed


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def run_smoke(session, out_dir, errors):
    problems = 0
    print("== 1. Source URLs ==", flush=True)
    for mode in ("full", "incremental"):
        for item in build_items(mode):
            if item["check_url"]:
                fp, size, status = fingerprint(session, item["check_url"], errors, item["source"])
                note = "ok" if status and status < 400 else "NOT REACHABLE"
                print(f"  {mode:11} {item['key']:34} HTTP {status} | size {size} | fp {str(fp)[:12]} | {note}")
                if note != "ok" and not item["optional"]:
                    problems += 1

    print("== 2. Arena parquet structure (copy into ARENA_CONTRACT if different) ==", flush=True)
    probes = [(s, "latest", "main") for s in ARENA_SUBSETS] + [("webdev", "full", ARENA_FULL_REV),
                                                               ("agent", "full", ARENA_FULL_REV)]
    for subset, split, revision in probes:
        dest = os.path.join(out_dir, "smoke", f"{subset}_{split}.parquet")
        result = download(session, arena_url(subset, split, revision), dest, errors, "arena", f"{subset}/{split}")
        if result is None:
            if not (split == "latest" and subset in ARENA_OPTIONAL_LATEST):
                problems += 1
            continue
        rows, lines = parquet_schema_lines(dest)
        print(f"  {subset}/{split} | {result['size'] / MB:.2f} MB | rows: {rows}")
        print("\n".join(lines))

    print("== 3. CooperBench archive (solo) ==", flush=True)
    dest = os.path.join(out_dir, "smoke", "solo.tar.gz")
    result = download(session, cooper_url(COOPER_ARCHIVES["solo"], COOPER_REV), dest, errors, "cooperbench", "solo")
    if result is None:
        problems += 1
    else:
        members, loaded, loaded_bytes, by_ext = inspect_archive(dest)
        print(f"  size {result['size'] / MB:.1f} MB | members {members} | JSON members {loaded} | "
              f"JSON bytes {loaded_bytes / MB:.1f} MB")
        for ext, slot in sorted(by_ext.items(), key=lambda kv: -kv[1]["bytes"])[:8]:
            print(f"    {ext:10} files {slot['files']:7} bytes {slot['bytes'] / MB:9.1f} MB")
        print("  first members:", first_members(dest))
        if loaded == 0:
            problems += 1

    print("== 4. BenchLM and Epoch ==", flush=True)
    for dataset, url in BENCHLM_URLS.items():
        dest = os.path.join(out_dir, "smoke", f"benchlm_{dataset}.json")
        result = download(session, url, dest, errors, "benchlm", dataset)
        if result is None:
            problems += 1
            continue
        with open(dest, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        shape = f"list of {len(doc)}" if isinstance(doc, list) else f"object with keys {list(doc)[:8]}"
        print(f"  benchlm/{dataset} | {result['size'] / MB:.2f} MB | {shape}")
    dest = os.path.join(out_dir, "smoke", "epoch.csv")
    result = download(session, EPOCH_URL, dest, errors, "epoch", "all_ai_models")
    if result is None:
        problems += 1
    else:
        header, rows = csv_shape(dest)
        print(f"  epoch | {result['size'] / MB:.2f} MB | columns {len(header)} | rows {rows}")
        print("  first columns:", header[:10])

    if problems:
        print(f"Smoke test FAILED | problems: {problems} (see the error log)", flush=True)
        return 1
    print("Smoke test passed | all sources reachable", flush=True)
    return 0


def run_load(mode, state_in, out_dir, stamp, errors, summary):
    state = read_state(state_in)
    if mode == "incremental" and state is None:
        errors.add("start", "all", state_in, "No state file. Run the FULL load once before incremental loads.")
        return 1
    batch_id = f"{stamp}_{mode}"
    session = make_session()
    staged, checked, failed = stage_items(mode, state, session, out_dir, stamp, errors)
    summary.update({"files_checked": checked, "files_failed": failed, "files_staged": len(staged)})

    if failed:
        errors.add("batch", "all", batch_id, f"{failed} download(s) failed. Nothing is uploaded; "
                   "the next run will try again.")
        summary["message"] = f"{failed} download(s) failed"
        return 1
    if not staged:
        print("No changes since the last run. Nothing to upload.", flush=True)
        summary["message"] = "No changes"
        return 0

    downloaded = sum(e["size_bytes"] for e in staged)
    landed = sum(e["landed_bytes"] for e in staged)
    needed = MIN_FULL_BYTES if mode == "full" else MIN_INCREMENTAL_BYTES
    summary.update({"downloaded_bytes": downloaded, "landed_bytes": landed, "required_bytes": needed})
    print(f"Volume check | downloaded {downloaded / MB:.1f} MB | landed {landed / MB:.1f} MB | "
          f"required {needed / MB:.0f} MB", flush=True)
    if landed < needed:
        errors.add("volume_rule", "all", batch_id,
                   f"{mode} load landed {landed / MB:.2f} MB but the rule requires at least {needed / MB:.0f} MB. "
                   "Batch refused; nothing uploaded and the state did not move.", error_type="VolumeRuleNotMet")
        summary["message"] = "Volume rule not met"
        shutil.rmtree(os.path.join(out_dir, "upload"), ignore_errors=True)   # nothing half-staged is left
        return 2

    manifest = {"batch_id": batch_id, "load_type": mode, "created_at": now_iso(),
                "required_bytes": needed, "downloaded_bytes": downloaded, "landed_bytes": landed,
                "gate_passed": True, "files": staged}
    manifest_dir = os.path.join(out_dir, "upload", "new", "_manifests")
    os.makedirs(manifest_dir, exist_ok=True)
    with open(os.path.join(manifest_dir, f"manifest__{batch_id}.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    new_state = state or {"version": 1, "items": {}}
    for entry in staged:
        new_state["items"][entry["key"]] = {"fingerprint": entry["fingerprint"], "sha256": entry["sha256"],
                                            "size_bytes": entry["size_bytes"], "last_batch": batch_id}
    if mode == "full":
        new_state["full_load_completed_at"] = now_iso()
    new_state["updated_at"] = now_iso()
    state_dir = os.path.join(out_dir, "upload", "_state")
    os.makedirs(state_dir, exist_ok=True)
    with open(os.path.join(state_dir, "state.json"), "w", encoding="utf-8") as handle:
        json.dump(new_state, handle, indent=2)
    summary["message"] = f"Batch {batch_id} ready"
    return 0


def main():
    parser = argparse.ArgumentParser(description="Frontier AI Observatory collector")
    parser.add_argument("--mode", choices=["smoke", "full", "incremental"], required=True)
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--state-in", default="state/state.json")
    args = parser.parse_args()

    started = now_iso()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_dir = os.path.join(args.out_dir, "logs")
    errors = ErrorLog(os.path.join(log_dir, f"collector_errors_{stamp}.jsonl"), f"{stamp}_{args.mode}")
    summary = {"run_id": f"collector_{stamp}", "batch_id": f"{stamp}_{args.mode}", "mode": args.mode,
               "started_at": started, "downloaded_bytes": 0, "landed_bytes": 0, "files_staged": 0,
               "files_checked": 0, "files_failed": 0, "message": ""}
    code = 1
    try:
        if args.mode == "smoke":
            code = run_smoke(make_session(), args.out_dir, errors)
        else:
            code = run_load(args.mode, args.state_in, args.out_dir, stamp, errors, summary)
    except Exception as exc:
        errors.add("main", "all", args.mode, f"Unexpected failure: {exc}", exc=exc)
        code = 1
    finally:
        summary.update({"finished_at": now_iso(), "exit_code": code, "error_count": errors.count,
                        "status": "SUCCESS" if code == 0 else "FAILURE"})
        with open(os.path.join(log_dir, f"collector_run_{stamp}.json"), "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
        print("Run summary:", json.dumps(summary), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
