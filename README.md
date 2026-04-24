<div align="center">

# spark-lineage

**Column-level lineage tracking for PySpark DataFrames. No decorators. No schema declarations. No code changes.**

[![PyPI](https://img.shields.io/pypi/v/spark-lineage?color=blue&label=PyPI)](https://pypi.org/project/spark-lineage/)
[![Python](https://img.shields.io/badge/python-%3E%3D3.11-blue)](https://pypi.org/project/spark-lineage/)
[![PySpark](https://img.shields.io/badge/pyspark-%3E%3D3.3-orange)](https://pypi.org/project/spark-lineage/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![CI](https://github.com/nitrajen/spark-lineage/actions/workflows/release.yml/badge.svg)](https://github.com/nitrajen/spark-lineage/actions)

</div>

---

**[Install](#install)** | **[Quickstart](#quickstart)** | **[API](#api)** | **[Design](#design)** | **[HTML Viewer](#html-viewer)** | **[JSON Structure](#json-structure)** | **[Limitations](#limitations)** | **[In Progress](#in-progress)**

---

The first obstacle developers often stumble upon when studying or updating large data pipelines is the source to target lineage. Dataset-level lineage is useless because the granularity of change is often at a column-level. spark-lineage implements column-level lineage for PySpark DataFrames by monkey-patching `DataFrame.__getattribute__` at import time — every method call on a tracked DataFrame is intercepted transparently, and column attribution is extracted directly from Spark's analyzed query plan.

Register a source DataFrame and get two outputs from the same lineage graph: an interactive HTML report and a queryable JSON file.

```python
import spark_lineage as spl

orders = spl.track_df(spark.read.parquet("orders"), "orders")
result = orders.filter(...).groupBy(...).agg(...)
spl.save_report(result, "./report")
# → report.html  (interactive viewer)
# → report.json  (queryable lineage data)
```

## Compatibility

| PySpark | Python |
|---------|--------|
| >= 3.3  | >= 3.11 |

## Install

```bash
pip install spark-lineage
```

---

## Quickstart

### Explicit tracking

```python
import spark_lineage as spl
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Mark the source DataFrame. Everything downstream is tracked from here.
orders = spl.track_df(spark.read.parquet("/data/orders"), "orders")

enriched = (
    orders
    .filter(F.col("status") == "completed")
    .withColumn("revenue", F.col("amount") * F.col("quantity"))
    .withColumn("margin", F.col("revenue") - F.col("cost"))
)

spl.save_report(enriched, "./lineage/orders_report")
# Writes orders_report.html and orders_report.json
```

### Session tracking (automatic)

Within a `session()` block every DataFrame is registered automatically. There is no need to call `track_df` manually.

```python
with spl.session() as run_id:
    orders   = spark.read.parquet("/data/orders")
    products = spark.read.parquet("/data/products")

    result = orders.join(products, "product_id").groupBy("category").agg(
        F.sum("revenue").alias("total_revenue")
    )
    result.write.mode("overwrite").saveAsTable("reporting.category_summary")
    spl.save_report(result, "./report")
```

### Cross-process lineage

Two Airflow tasks in separate processes can share lineage via a `FileStore`. The write node in Task A is linked to the read in Task B automatically.

```python
store = spl.FileStore("/shared/.spark_lineage")

# Task A
with spl.session(run_id=context["run_id"], store=store):
    df = spark.read.parquet("fact_orders")
    df.write.saveAsTable("enriched_orders")

# Task B (separate process, same run_id)
with spl.session(run_id=context["run_id"], store=store):
    df = spark.read.table("enriched_orders")   # linked to Task A's write
    spl.save_report(df, "./report")
```

---

## API

### `spl.track_df(df, name)`

Registers a DataFrame as a lineage root. All transformations applied to it (and DataFrames derived from it) are tracked from this point forward.

```python
orders = spl.track_df(spark.read.parquet("/data/orders"), "orders")
```

Returns the same DataFrame. The name is used as the source label in reports.

### `spl.session(run_id=None, store=None)`

Context manager. Inside the block, every DataFrame is tracked automatically — no `track_df` calls needed. Write operations (`df.write.parquet(...)`, `df.write.saveAsTable(...)`) are captured as target nodes in the lineage graph.

```python
with spl.session() as run_id:
    df = spark.read.parquet(...)
    df.write.mode("overwrite").parquet("/data/output")
```

- `run_id`: opaque string scoping this run. Defaults to a random UUID.
- `store`: `MemoryStore()` (default, in-process) or `FileStore(path)` (cross-process).

### `spl.save_report(df, path, name=None)`

Generates an HTML report and a JSON data file from the current lineage state.

```python
spl.save_report(result, "./report/lineage")
# → ./report/lineage.html
# → ./report/lineage.json
```

### `spl.tracked_task(func)`

Decorator. Any DataFrame argument entering the function that is not already tracked is registered as a lineage root automatically.

```python
@spl.tracked_task
def build_features(orders: DataFrame, products: DataFrame) -> DataFrame:
    return orders.join(products, "product_id").withColumn(...)
```

### `spl.build_ol_event(source_df, run_id, job_name, job_namespace)`

Returns an OpenLineage `RunEvent` dict without sending it. Useful for inspection or custom transport.

### `spl.emit_openlineage(source_df, url, job_name, run_id=None, job_namespace="default")`

Builds and POSTs an OpenLineage COMPLETE event to a Marquez or DataHub collector.

```python
spl.emit_openlineage(result, "http://marquez:5000", job_name="preprocess_orders")
```

---

## Design

spark-lineage patches four PySpark entry points at import time: `DataFrame.__getattribute__`, `DataFrameWriter.__getattribute__`, `DataFrameReader.__getattribute__`, and `SparkSession.sql`. This means every method call on a tracked DataFrame — and every SQL query that references a temp view of one — is intercepted without a hardcoded list of method names. Terminal writes vs chaining calls are distinguished purely by return type at runtime.

Column attribution does not use string matching or static analysis. After each transformation, spark-lineage walks `df.queryExecution().analyzed()` via the JVM bridge and reads `projectList()` or `aggregateExpressions()` directly from Spark's own analyzed plan. The result is an exact `{output_column: [referenced_input_columns]}` map that handles aliases, renames, and multi-source joins correctly — because it comes from the same plan Spark uses to execute the query.

Each node in the lineage graph carries the operation name, a truncated repr of its arguments, its parent node UUIDs, the column list at that stage, the column reference map from the analyzed plan, and the call site (file, line, qualified function name).

Report targets are write nodes, not intermediate DataFrames. When `df.write.parquet(...)` or `df.write.saveAsTable(...)` is called anywhere in the pipeline, those become the targets in the report. For `saveAsTable` and `insertInto`, the destination is resolved to a fully qualified name (`catalog.schema.table`) using the active Spark session's catalog API, with support for both Unity Catalog (Spark 3.4+) and the legacy metastore.

---

## HTML Viewer

The generated `.html` file is a self-contained single-page app. No server required. Open it in any browser.

### Layout

Two-pane layout: source panel on the left, detail panel on the right.

**Source panel**

- Lists all source DataFrames as collapsible accordion entries.
- Each entry shows the source name, the qualified function name and line where it was registered, and its column list.
- Clicking a column chip selects that source column and filters targets to those influenced by it.
- Clicking the header expands or collapses the entry without selecting it.

**Target list**

- Shows all write targets (or leaf DataFrames if no writes exist).
- Each target shows chips for all source DataFrames that feed it.
- When a source DF is selected, targets not fed by that source are dimmed. When multiple sources are selected, only targets fed by ALL selected sources are highlighted (AND logic).
- Clicking a target opens the target overview in the right panel.

### Interaction

**Source DF selection**: click a source header chip in the target list (or a column chip in the source accordion) to filter targets. Multiple sources can be selected simultaneously. AND logic: a target must use all selected sources to be highlighted.

**Column mode**: click any column chip in the source accordion. The view switches to column mode, showing per-target influence counts. Selecting columns from multiple sources uses OR logic: a target is highlighted if it uses any of the selected source columns.

**Target overview**: click any target to see its column breakdown. Columns are grouped by role:

- **Created** — did not exist in the source; computed by this pipeline.
- **Modified** — existed in the source but was transformed (renamed, cast, computed from other columns).
- **Passthrough** — copied from source unchanged.

Each column in the overview shows which source columns contributed to it.

**Column trace**: click any column in the target overview to drill into the full derivation path, shown as a top-to-bottom flow:

```
FROM (source columns)
  |
VIA (intermediate columns computed along the way)
  |
CREATED / MODIFIED (the step that produced this column)
  |
carried through N steps
  |
OUTPUT (final column in the write target)
```

Intermediate columns are resolved recursively: if `margin_pct` was derived from `total_gross_profit`, which was derived from `gross_profit`, all three appear in the VIA block in chronological order.

### Tooltips

Hovering over any role term (Created, Modified, Passthrough, Intermediate) shows a tooltip with a short definition and a code example.

---

## JSON Structure

The `.json` file produced by `save_report` contains the full lineage data. It is self-contained and can be used to build custom visualizations or feed downstream tools.

```json
{
  "meta": {
    "title": "string",
    "generated_at": "ISO 8601 timestamp",
    "source_count": 2,
    "target_count": 3
  },
  "sources": [
    {
      "id": "hex UUID",
      "name": "orders",
      "primary": true,
      "caller": {
        "fqn": "module.function:42",
        "file_short": "pipeline.py",
        "line": 42
      },
      "columns": ["order_id", "amount", "quantity"],
      "influence": {
        "amount": {
          "<target_id>": ["revenue", "margin"]
        }
      }
    }
  ],
  "targets": [
    {
      "id": "hex UUID",
      "label": "write.parquet",
      "name": "/data/output/summary",
      "caller": {
        "fqn": "pipeline.run:87",
        "file_short": "pipeline.py",
        "line": 87
      },
      "source_dfs": [
        { "id": "hex UUID", "name": "orders", "caller": { "..." : "..." } }
      ],
      "columns": ["category", "total_revenue", "margin_pct"],
      "col_roles": {
        "category": "passthrough",
        "total_revenue": "created",
        "margin_pct": "created"
      },
      "traces": {
        "total_revenue": {
          "sources": [
            { "src_id": "hex UUID", "col": "amount", "src_name": "orders" }
          ],
          "steps": [
            {
              "operation": "groupBy.agg",
              "role": "created",
              "args": ["sum(amount)"],
              "caller": {
                "fqn": "pipeline.run:72",
                "file_short": "pipeline.py",
                "line": 72
              }
            }
          ]
        }
      }
    }
  ]
}
```

**Key fields:**

- `sources[].influence` — for each source column, which target columns it reaches. Keyed by `(source_id, column_name)` so same-named columns in different source DataFrames are never confused.
- `targets[].source_dfs` — all ancestor source DataFrames, derived from the actual lineage graph. Always includes sources that contribute only via `count(*)` or join keys, even when column-level attribution is not possible.
- `targets[].col_roles` — role classification for each output column.
- `targets[].traces[col].steps` — ordered list of operations from source to output, each with the operation name, role, arguments, and call site.

---

## Limitations

**Single Python process (default)**

By default, lineage is in-memory and does not cross process boundaries. Use `FileStore` with a shared path to bridge across Airflow tasks or other multi-process setups. Cloud storage (`S3Store`, `HDFSStore`) is not yet implemented.

**No row-level tracking**

Filters, joins, and limit operations change which rows appear in the output, but spark-lineage only tracks column derivation, not row provenance. A filter that removes 90% of rows is not reflected in the lineage graph.

**`count(*)` and whole-dataset aggregates**

When a column is produced by `count(*)`, Spark's analyzed plan has no column references. These columns appear in the report with a "whole-dataset aggregate" note and show the source DataFrame as context, but no specific source column attribution is possible.

**Target labels**

Targets produced by `df.write.*` calls are labeled with the write path or fully qualified table name. Targets that are intermediate DataFrames (no write) are labeled with the qualified function name and line. Variable names are not available at runtime.

**Join column attribution**

For columns that flow through a join from a secondary source, column-level attribution works in most cases. Aggregations that do not reference a specific column from the joined DataFrame (e.g. `count(*)` applied after a join) will not show that joined DataFrame in the column trace, though it will still appear in `source_dfs`.

---

## In Progress

### P0 — Spark SQL support ✓ Done

`spark.sql("SELECT ...")` is fully tracked. Register a temp view from any tracked DataFrame via `df.createOrReplaceTempView("name")`, then query it with `spark.sql(...)` — the result is linked back to its source DataFrames automatically. Column attribution is extracted from Spark's analyzed plan, same as the DataFrame API.

### P1 — Better target labels

Intermediate DataFrames are currently labeled by the qualified function name and line that produced them. The variable name (e.g. `enriched_orders`) is not accessible at runtime. Best workaround today: call `spl.track_df(result, "enriched_orders")` explicitly.

### P2 — Cloud stores

`FileStore` writes JSON to local disk. Cross-job lineage on cloud infrastructure needs `S3Store` and `HDFSStore` implementations.

### P3 — Join condition in trace

The column trace view shows which columns were involved in a join but does not surface the join condition itself (e.g. `orders.customer_id == customers.id`). This would make the row-filtering effect of joins more visible in the lineage report.

### P4 — OpenLineage transport improvements

The OpenLineage emitter currently uses a simple HTTP POST. Batching, retry logic, and async transport are not implemented.

---

## License

[Apache 2.0](LICENSE)
