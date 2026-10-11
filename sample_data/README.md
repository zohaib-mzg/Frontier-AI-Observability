# Public source samples

`full/arena/` contains four 100-row representative historical Parquet samples. `full/cooperbench/` contains an inspected, unchanged result JSON member for each coordination setting. `full/benchlm/` contains representative attributed catalog and pricing subsets.

`incremental/arena/` contains three unchanged native source Parquet files following the pinned historical baseline. They contain 21,896 records and total **1,219,849 bytes (1.22 MB)**. Standard and style-controlled text are related evidence and must not be pooled as independent experiments.

The complete eight-file source inventory totals **216,373,763 bytes (216.37 MB)** in original formats. These full payloads are not stored in Git. Supplemental BenchLM and Epoch exports add approximately 10.18 MB, making initial acquisition approximately 227 MB.

`manifest.json` records original source sizes, source revisions and checksums, sample selections and sample checksums. Paths are relative to the repository root. Native incremental sizes count the original Parquet files, not the outer transport ZIP used to deliver this repository package.

Use `python tools/verify_samples.py` from the repository root after installing the dependencies. Full-study traces and Epoch raw exports remain restricted. Zero-filled CooperBench token and cost fields do not establish free execution or measured usage.
