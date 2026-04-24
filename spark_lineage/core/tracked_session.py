"""
Patches SparkSession.sql() once at import time.

When spark.sql("SELECT ...") is called:
  - The analyzed plan is walked iteratively to collect all SubqueryAlias names,
    which is how Spark represents temp views and catalog tables in the plan.
  - Each name is looked up in the registry's view map.  Views registered via
    createOrReplaceTempView() on tracked DataFrames are linked as parents.
  - Column attribution is extracted from the analyzed plan — the same mechanism
    used for DataFrame API operations.
  - If no tracked views are referenced but a session is active, the SQL result
    is registered as a named root so it still enters the lineage graph.

No SQL parsing is done in Python.  Everything comes from Spark's own analyzed
plan, so attribution is exact and handles aliases, subqueries, and CTEs.
"""
from __future__ import annotations
from uuid import uuid4
from pyspark.sql import SparkSession, DataFrame

from .registry import _registry
from .node import CallerInfo
from .tracked_df import _extract_col_refs_from_plan

# Mirrors tracked_df._session_active — set by the session() context manager.
_session_active: bool = False


def _set_session(active: bool) -> None:
    global _session_active
    _session_active = active


def _view_names_from_plan(jdf) -> list[str]:
    """
    Iteratively walk the analyzed logical plan and collect all SubqueryAlias
    names.  SubqueryAlias is the plan node Spark uses for temp views, CTEs,
    and named subqueries — its identifier/alias is the name the SQL references.

    Returns a deduplicated list in discovery order.
    """
    try:
        plan = jdf.queryExecution().analyzed()
        names: list[str] = []
        stack = [plan]
        seen: set[int] = set()

        while stack:
            node = stack.pop()
            try:
                nid = node.hashCode()
                if nid in seen:
                    continue
                seen.add(nid)
            except Exception:
                continue

            try:
                if node.getClass().getSimpleName() == "SubqueryAlias":
                    try:
                        names.append(node.identifier().name())
                    except Exception:
                        try:
                            names.append(str(node.alias()))
                        except Exception:
                            pass
            except Exception:
                pass

            try:
                children = node.children()
                for i in range(children.size()):
                    stack.append(children.apply(i))
            except Exception:
                pass

        return list(dict.fromkeys(names))  # deduplicate, preserve order
    except Exception:
        return []


_original_sql = SparkSession.sql


def _patched_sql(self, sqlQuery: str, *args, **kwargs):
    result = _original_sql(self, sqlQuery, *args, **kwargs)

    if not isinstance(result, DataFrame):
        return result

    # Find tracked parent views referenced in the SQL plan.
    view_names = _view_names_from_plan(result._jdf)
    parent_ids: list[str] = []
    for name in view_names:
        pid = _registry.get_by_view(name)
        if pid is not None:
            parent_ids.append(pid)

    if not parent_ids and not _session_active:
        return result  # nothing to track

    caller = CallerInfo.capture(depth=2)
    child_id = uuid4().hex
    result._dfi_id = child_id

    try:
        out_cols = list(result.columns)
    except Exception:
        out_cols = []

    col_refs = _extract_col_refs_from_plan(result)

    # Flatten whitespace for a readable single-line repr.
    flat = " ".join(sqlQuery.split())
    sql_repr = flat[:200] + ("..." if len(flat) > 200 else "")

    if parent_ids:
        _registry.register_child(
            child_id,
            parent_ids=parent_ids,
            operation="sql",
            args_repr=[sql_repr],
            caller=caller,
            output_cols=out_cols,
            column_refs=col_refs,
        )
    else:
        # Session active but no tracked views — named root.
        short = flat[:60] + ("..." if len(flat) > 60 else "")
        _registry.register_root(
            child_id,
            name=f"sql: {short}",
            output_cols=out_cols,
            caller=caller,
        )

    return result


SparkSession.sql = _patched_sql
