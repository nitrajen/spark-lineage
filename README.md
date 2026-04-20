# spark_lineage

`spark_lineage` is a Python library for runtime, column-level lineage tracking of PySpark DataFrames. You call `spl.track_df(df, "orders")` on a source DataFrame, run your pipeline normally, then call `spl.save_report(df, "./report")` to get a full lineage report — which source columns contributed to which target columns, through what transformations, and at exactly which file and line number.

## How it works

`spark_lineage` works entirely at runtime through monkey-patching: `DataFrame.__getattribute__` (and `GroupedData.__getattribute__`) is replaced at import time, so every method call on a tracked DataFrame is intercepted transparently — no hardcoded list of method names, no static analysis, no bytecode inspection. When a transformation is intercepted, column attribution is extracted directly from Spark's analyzed query plan (`queryExecution().analyzed()`) — specifically `projectList()` and `aggregateExpressions()` — giving exact `{output_col: [referenced_input_cols]}` mappings from the same plan Spark uses to execute the query.

## What is tracked

- Source DataFrames registered via `track_df`, `session()`, or `@tracked_task`
- Target DataFrames produced by terminal writes or captured via `save_report`
- All transformation steps between source and target
- Column-level attribution: which source columns each target column was derived from
- Intermediate columns (e.g. `gross_profit = quantity * unit_price` before aggregation)
- Caller identity (file:line, fully-qualified function name) for every step

## When lineage is complete vs. partial

**Complete lineage** (all columns traced to root sources):

- Any chain of `withColumn`, `select`, `filter`, `join`, `groupBy().agg()` on tracked DataFrames
- Multi-source joins: columns from both DataFrames are attributed to their respective sources
- Intermediate computed columns are traced through to their root origins

**Partial lineage** (known gaps):

- `count(*)` and `count(1)`: Spark's analyzed plan has no column-level references for wildcard aggregates. These show as "whole-dataset aggregate" with source DataFrame context but no column attribution.
- Complex window functions: may fall back to passthrough when Spark's plan does not clearly attribute the output to specific inputs.
- `spark.sql("SELECT ...")`: Not yet intercepted. SQL queries are invisible to the lineage tracker. (P0 — planned)

**Not tracked at all**:

- DataFrames created outside a `session()` context or `track_df()` call
- DataFrames read/written in a separate process without a shared `FileStore`
- Pure Python transformations that do not use Spark's DataFrame API

## Identifying DataFrames by FQN

The name shown for a source DataFrame (e.g. `"orders"`) is what was passed to `track_df(df, "orders")`. This is a user-provided alias.

The fully-qualified identifier shown below the name (e.g. `pipelines.OrdersLoader.load:42`) is the actual code location where `track_df` was called — the module, class, method, and line number. **This is the stable identifier** regardless of variable names in the pipeline. In large pipelines where variables are named `df`, `df2`, etc., the FQN is how you locate the DataFrame in the codebase.

To get a meaningful FQN, call `track_df` inside a named function or class method, not at module level.

## Quick start

```python
import spark_lineage as spl

# Mark your source DataFrames
orders = spl.track_df(spark.read.parquet("orders"), "orders")
products = spl.track_df(spark.read.parquet("products"), "products")

# Run your pipeline normally
result = orders.join(products, "product_id").groupBy("category").agg(...)

# Generate the report
spl.save_report(result, path="./lineage_report", name="product_summary")
```

## Running the test pipeline

```bash
python test_comprehensive.py
open sales_lineage.html
```
