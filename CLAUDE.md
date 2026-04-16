# spark_lineage — PySpark DataFrame Lineage Library

## What this is
A Python library for runtime column-level lineage tracking of PySpark DataFrames.
The user calls `spl.track_df(df, "orders")` on a source DataFrame, runs their pipeline normally,
then calls `spl.save_report(df, "./report")` to get a full lineage report — which source columns
contributed to which target columns, through what transformations, at what file:line.

## Core design principle
No static analysis. No hardcoded function names. Runtime monkey-patching of
`DataFrame.__getattribute__` and `GroupedData.__getattribute__` — every method call on a
tracked DataFrame is intercepted transparently. Column references are extracted from Spark's
own analyzed query plan (`queryExecution().analyzed()`) — not string matching.

## What is implemented

### Runtime interception (`spark_lineage/core/`)
- `tracked_df.py` — patches `DataFrame.__getattribute__` and `GroupedData.__getattribute__` at import.
  Every transformation on a tracked DataFrame registers a child node with UUID identity.
  Uses Spark's analyzed plan (`projectList()` / `aggregateExpressions()`) to extract
  `{output_col: [referenced_input_cols]}` — no string matching.
- `tracked_writer.py` — patches `DataFrameWriter.__getattribute__`. Terminal writes register
  a `write.*` node and save `(run_id, dest) → (node_id, cols)` to the LineageStore.
- `tracked_reader.py` — patches `DataFrameReader.__getattribute__`. Reads look up the store
  and link back to the prior write node, bridging cross-process lineage.
- `registry.py` — in-memory graph of `LineageNode` objects. UUID-keyed. Forward/backward
  traversal, leaf descendants, path between source and target.
- `node.py` — `LineageNode` dataclass with `id`, `operation`, `args_repr`, `parent_ids`,
  `output_cols`, `column_refs` (plan-based), `caller` (CallerInfo with file:line:qualname).
- `store.py` — `LineageStore` abstract base. `MemoryStore` (in-process) and `FileStore`
  (cross-process, one JSON per run_id). Used for write→read bridges across Spark jobs.

### Public API (`spark_lineage/__init__.py`)
- `track_df(df, name)` — explicit root registration with output_cols from df.columns.
- `session(run_id, store)` — context manager, auto-tracks all DataFrames. Optionally cross-process.
- `tracked_task(func)` — decorator, auto-tracks DataFrame function arguments.
- `save_report(source_df, path, name)` — generates .json + .html report.
- `build_ol_event(source_df, run_id, job_name, job_namespace)` — builds OpenLineage RunEvent dict.
- `emit_openlineage(source_df, url, job_name, run_id, job_namespace)` — POSTs to collector.

### Lineage report (`spark_lineage/report.py`)
- Source-centric: one source DataFrame, N target (leaf) DataFrames.
- `_build_col_source_map(path_nodes, source_id)` — walks all nodes topologically, resolves
  column refs through intermediate columns to find ultimate source column attribution.
  Multi-hop: `total → net_amount → {amount, discount}` works correctly.
- `_build_col_trace(col, path_nodes, source_id, col_src_map)` — per-column vertical trace,
  role classification: source / created / modified / passthrough.
  Uses `node.column_refs` (plan-based) for role classification, NOT string matching.
- `_compute_source_influence(source_cols, targets, traces)` — builds
  `{source_col: {target_id: [influenced_target_cols]}}` for click-through filtering.

### HTML report viewer
- Two-pane: left sidebar (source columns, targets list, columns panel), right trace area.
- Source column chips are **clickable** — clicking a source column dims non-influenced targets,
  highlights influenced targets with badge, and marks influenced columns in blue in the column panel.
- Target overview shown on target select (before column selected): qualname, file:line, column
  count by role (created/modified/passthrough), source column attribution, clickable column chips.
- Column trace: top = most recent step, bottom = creation/source (reverse chronological — "where
  did this come from" mental model).
- Passthrough steps shown by default, collapsible via toggle.
- Passthrough step args shown (muted) so user can see what that step was doing.

### OpenLineage emitter (`spark_lineage/openlineage.py`)
- `build_event(source_df, run_id, job_name, job_namespace)` — builds OpenLineage COMPLETE RunEvent.
- `emit(source_df, url, job_name, ...)` — POSTs to `/api/v1/lineage` (Marquez/DataHub compatible).
- Emits `columnLineage` facet per output column with `inputFields` (namespace/name/field) and
  `transformations` (DIRECT/INDIRECT with subtype and description).
- Emits `schema` facet for inputs and outputs.

## What is NOT yet implemented (priority order)

### P0 — Spark SQL support
`spark.sql("SELECT ...")` is completely invisible. Large corporate pipelines use SQL heavily.
**Plan:** intercept `SparkSession.sql()`, parse SQL via `spark._jsparkSession.sessionState().sqlParser()`,
extract table/column refs, link back to tracked DataFrames via registered temp views.

### P1 — Better target labels
Targets currently labeled as `.orderBy() · function_name()` (last operation + caller).
Ideally the target label should be the variable name or dataset name the result is assigned to.
Variable names are not available at runtime; best option is qualname (already showing in overview now)
or explicit naming via `spl.track_df(result, "customer_summary")`.

### P2 — Join column attribution
When `df1.join(df2, "key")`, columns from `df2` appear in the result. Currently these show no
source attribution from the original source because `df2` is a separate lineage chain.
Need: multi-root column source resolution.

### P3 — OpenLineage transport for S3/HDFS
`FileStore` writes to local disk. Cross-job lineage on cloud infra needs `S3Store` / `HDFSStore`.
Short-term: use a shared NFS mount or serialize to object store outside the library.

### P4 — Kedro integration
Kedro injects DataFrames via catalog hooks, not explicit reads. Best entry point:
`after_dataset_loaded` hook calls `spl.track_df(data, dataset_name)`.
No code changes needed — just documentation.

### P5 — `modified` role completeness
`modified` role only fires when `node.column_refs` has the column AND refs differ from `{col}`.
Ops that don't produce a Project/Aggregate plan (complex window functions, etc.) fall back to
passthrough even if they modify the column. Acceptable false-negative (safer than false-positive).

## Key files
```
spark_lineage/
  __init__.py           public API
  report.py             JSON builder + HTML template
  openlineage.py        OpenLineage emitter
  core/
    tracked_df.py       DataFrame + GroupedData patches
    tracked_writer.py   DataFrameWriter patch
    tracked_reader.py   DataFrameReader patch
    registry.py         in-memory lineage graph
    node.py             LineageNode + CallerInfo dataclasses
    store.py            MemoryStore + FileStore

test_comprehensive.py   large test pipeline (SalesAnalyticsPipeline, 4 branches)
sales_lineage.html      generated report (gitignored)
sales_lineage.json      generated data (gitignored)
```
