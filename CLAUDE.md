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

### JSON data model (`spark_lineage/report.py` — `_build_json`)
Self-contained hierarchical structure. Everything is precomputed — no JS traversal needed.
```
{
  meta:    { title, generated_at, source_count, target_count }
  sources: [
    {
      id, name, primary,           # primary=true for the source_df passed to save_report
      caller,                      # { fqn: "module.Class.method:line", file_short, line, ... }
      columns,                     # schema-order column list
      influence: {                 # for each source column → which target cols it reaches
        col: { target_id: [influenced_target_cols] }
      }
    }
  ]
  targets: [
    {
      id, label, name,
      caller,                      # fqn of the function/line that produced this target
      source_dfs,                  # [ {id, name, caller} ] — ALL ancestor source DFs
      columns,                     # schema-order column list
      col_roles: { col: "created"|"modified"|"passthrough" },
      traces: {
        col: {
          sources: [ {src_id, col, src_name} ],   # ultimate source column attribution
          steps:   [ {operation, role, args, caller, context_key} ]
        }
      }
    }
  ]
}
```
Key design decisions:
- `sources[].influence` uses `(src_id, col)` tuples — same-named columns in different source
  DFs are never confused. `count(*)` and join-key-only sources have empty influence entries.
- `targets[].source_dfs` is derived from actual ancestor nodes (not influence map), so it
  correctly includes sources that contribute only via `count(*)` or join keys.
- `_build_col_source_map` seeds with `{(node.id, col)}` tuples, propagates through transforms.
- `_compute_source_influence` matches by exact `(src_id, col)` pair.

### HTML report viewer (`spark_lineage/report.py` — `_VIEWER_TEMPLATE`)
Two-pane layout: left sidebar (sources accordion + targets list + columns panel), right main area.

#### Left sidebar
- **Sources accordion**: each source DF is a collapsible entry showing its column chips.
  - Click header → toggle select (multi-select, `STATE.activeSrcIds` Set).
  - Click column chip → toggle source column select (multi-select, `STATE.activeSrcCols` Map keyed `"srcId|col"`).
  - FQN (`module.Class.method:line`) shown as sub-label under the name.
- **Targets list**: always shows ALL source DF chips per target as `sel` (blue) / `unsel` (grey).
  Selecting `orders`+`campaigns` and then deselecting `orders` → target keeps both chips,
  `orders` chip turns grey. Never loses source info.
  - Target dim/highlight uses AND logic: target must use ALL selected source DFs.
- **Columns panel**: columns list at bottom, highlighted when fed by selected source.

#### Interaction model (STATE)
```javascript
STATE = {
  activeSrcIds:  Set,   // source DF multi-select — AND filter on targets
  activeSrcCols: Map,   // source column multi-select: key="srcId|col", value={srcId,col}
  activeTarget:  null,
  activeCol:     null,
}
```

**Startup**: nothing is auto-selected. Both panels are built but STATE is all-null/empty.

**Selection cascade — complete model**

| Level | Element | Action | Result |
|-------|---------|--------|--------|
| 1 | Source DF header | click | `toggleSrcSelect`: toggle in `activeSrcIds`; deselecting also clears that DF's source columns from `activeSrcCols` |
| 2 | Source column chip | click | `selectSrcCol`: toggle in `activeSrcCols`; selecting also adds parent DF to `activeSrcIds` and clears `activeCol` |
| 3 | Target item | click | `selectTarget`: toggle `activeTarget`; deselecting also clears `activeCol` and hides cols-panel |
| 4 | Target column | click | `selectColumn`: toggle `activeCol`; deselecting returns to target overview. Back button does the same. |

**Key cascade rules**:
1. Deselect source DF → also clears its source columns (but NOT target/column selections)
2. Select source column → ensures parent DF expands (adds to `activeSrcIds`), clears `activeCol`
3. Deselect target → clears column too (col has no context without a target)
4. Deselect column → returns to target overview (target stays selected)
5. Selecting a target column does **NOT** clear `activeSrcCols` — source column context
   is preserved alongside the trace view

**Mode switching (automatic, no toggle)**:
- **DF mode**: `activeSrcCols.size === 0` — AND intersection over `activeSrcIds` determines
  which targets highlight. Target highlighted only if ALL selected DFs feed it.
- **Column mode**: `activeSrcCols.size > 0` — OR union over selected source columns.
  Badge shows `N cols` count per target.
- Exits column mode by deselecting all source columns.

**Source accordion expansion logic** (`updateSrcPanelUI`):
- Selected DF (`activeSrcIds.has(id)`): always expanded, `selected` + `expanded` classes.
- Unselected DF: collapsed, **unless** a trace is active and that DF contributes to the
  selected column's trace → auto-expands via `traceContribSrcIds()` without modifying STATE.

#### Right main area
- **Target overview** (target selected, no column): stats bar, Contributing Sources chips
  (always shown, sel/unsel), per-source-column influence rows, Created/Modified/Passthrough chips.
  - `not-infl-banner` shown when user opens a target not fed by selected source(s).
  - Column chips only glow when the target IS in the influence map.
- **Column trace** (column selected): "derived from" green chips at top (`col ← src_name`),
  then reverse-chronological step timeline grouped by calling context.
  - `count(*)` fallback: shows "created from [df] (whole-dataset aggregate — no single column input)".
  - Source DFs auto-expand and contributing chips highlight when trace is shown.

### OpenLineage emitter (`spark_lineage/openlineage.py`)
- Uses `data["sources"]` (primary = one with `primary: True`).
- `_build_outputs` reads `t["traces"][col]` for column lineage facet.
- Emits `columnLineage` facet: `DIRECT`/`INDIRECT` with subtype and description.
- Emits `schema` facet for inputs and outputs.

## Known limitations / honest behavior
- **`count(*)`**: no column-level source attribution (Spark plan has no column refs for `*`).
  Displayed as "whole-dataset aggregate" with the source DF shown as context.
- **Dead columns**: source columns not referenced in any downstream expression show
  "no influence" with a "may be unused (dead column)" note — this is a real lineage finding.
- **Join column attribution (P2)**: columns that flow through a join from a secondary source
  are tracked, but `count(customer_id)` style aggs from a joined DF may not always attribute
  back to the join source perfectly.

## What is NOT yet implemented (priority order)

### P0 — Spark SQL support
`spark.sql("SELECT ...")` is completely invisible. Large corporate pipelines use SQL heavily.
**Plan:** intercept `SparkSession.sql()`, parse SQL via `spark._jsparkSession.sessionState().sqlParser()`,
extract table/column refs, link back to tracked DataFrames via registered temp views.

### P1 — Better target labels
Targets labeled by `qualname()` of the function that produced them. Variable name is unavailable
at runtime. Best alternative: explicit `spl.track_df(result, "customer_summary")`.

### P2 — Join column attribution (partial)
Multi-source column resolution works but `count(*)` and aggregates that don't reference specific
columns from a joined DF won't show that DF in influence. `source_dfs` (ancestor-based) is always
correct; `influence` map may undercount for aggregate-heavy joins.

### P3 — OpenLineage transport for S3/HDFS
`FileStore` writes to local disk. Cross-job lineage on cloud infra needs `S3Store` / `HDFSStore`.

### P4 — Kedro integration
Hook documented in `spark_lineage/integrations/kedro_hook.py`. Works as-is.

### P5 — `modified` role completeness
`modified` only fires when `node.column_refs` has the col AND refs differ from `{col}`.
Complex window functions fall back to passthrough. Acceptable false-negative.

### P6 — Per-source breakdown in target overview ✓ DONE
Right pane overview groups columns by contributing source DF (`buildSrcSections`), with role
subgroups (Created/Modified/Passthrough) and source column attribution arrows per group.
Columns with no column-level attribution (count(*) etc.) appear in "Whole-dataset aggregates" section.

## Key files
```
spark_lineage/
  __init__.py           public API
  report.py             JSON builder + HTML template (~1300 lines)
  openlineage.py        OpenLineage emitter
  core/
    tracked_df.py       DataFrame + GroupedData patches
    tracked_writer.py   DataFrameWriter patch
    tracked_reader.py   DataFrameReader patch
    registry.py         in-memory lineage graph
    node.py             LineageNode + CallerInfo dataclasses
    store.py            MemoryStore + FileStore
  integrations/
    kedro_hook.py       Kedro after_dataset_loaded + after_pipeline_run hooks

test_comprehensive.py   SalesAnalyticsPipeline — 4 sources (orders/campaigns/customers/products),
                        4 targets (monthly_channel_trends, product_performance,
                        channel_campaign_efficiency, customer_lifetime_value)
sales_lineage.html      generated report (gitignored)
sales_lineage.json      generated data (gitignored)
```

## Running the test
```bash
python test_comprehensive.py
# produces sales_lineage.html + sales_lineage.json
open sales_lineage.html
```
