"""
Patches pyspark.sql.DataFrame.__getattribute__ once at import time.
Only tracked DataFrames (those with _dfi_id set) pay any interception cost.

Session mode: when _session_active is True, any DataFrame that hasn't been
explicitly tracked is auto-registered as a root on its first method call.
"""

import functools
from uuid import uuid4
from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.group import GroupedData

from .registry import _registry
from .node import CallerInfo

# Set to True inside a dfi.session() context to auto-track all DataFrames.
_session_active: bool = False

# Methods that register a view as a side effect (return None).
# Small, stable PySpark Catalog API — not business logic.
_VIEW_METHODS = {"createOrReplaceTempView", "createTempView", "createGlobalTempView"}

_BASE = object.__getattribute__   # bypass all overrides safely


# ── column reference extraction via analyzed plan ─────────────────────────────

def _extract_join_condition(result_df) -> str | None:
    """
    Walk the analyzed plan of a join result to find the Join node and return
    its condition as a SQL string.  Returns None if no condition is found
    (e.g. cross join) or if the plan cannot be inspected.
    """
    try:
        plan = result_df._jdf.queryExecution().analyzed()
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
                if node.getClass().getSimpleName() == "Join":
                    cond = node.condition()
                    if cond.isDefined():
                        return str(cond.get().sql())
                    return None
            except Exception:
                pass
            try:
                children = node.children()
                for i in range(children.size()):
                    stack.append(children.apply(i))
            except Exception:
                pass
    except Exception:
        pass
    return None


def _extract_col_refs_from_plan(result_df) -> dict:
    """
    After a transformation executes, extract {output_col: [referenced_input_cols]}
    from the analyzed logical plan.

    Works for Project nodes (withColumn, select, withColumnRenamed, …) and
    Aggregate nodes (groupBy.agg).  Returns {} for Filter, Sort, Join, etc.
    — those don't transform column values so callers treat all cols as passthrough.
    """
    try:
        plan       = result_df._jdf.queryExecution().analyzed()
        plan_class = plan.getClass().getSimpleName()
        col_map: dict[str, list[str]] = {}

        if plan_class == "Project":
            named_exprs = plan.projectList()
        elif plan_class == "Aggregate":
            named_exprs = plan.aggregateExpressions()
        else:
            return {}

        for i in range(named_exprs.size()):
            ne   = named_exprs.apply(i)
            name = ne.name()
            refs = set()
            ref_seq = ne.references().toSeq()
            for j in range(ref_seq.size()):
                refs.add(ref_seq.apply(j).name())
            if refs:
                col_map[name] = sorted(refs)

        return col_map
    except Exception:
        return {}


# ── interception wrappers ─────────────────────────────────────────────────────

def _wrap(source_id: str, op_name: str, method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        caller = CallerInfo.capture(depth=2)
        result = method(*args, **kwargs)

        if isinstance(result, DataFrame):
            child_id = uuid4().hex
            result._dfi_id = child_id
            try:
                out_cols = list(result.columns)
            except Exception:
                out_cols = []
            # Collect any tracked DataFrames passed as arguments (e.g. join's right side)
            extra_parents = [
                _BASE(a, "__dict__").get("_dfi_id")
                for a in args
                if isinstance(a, DataFrame)
            ]
            parent_ids = [source_id] + [p for p in extra_parents if p and p != source_id]
            col_refs   = _extract_col_refs_from_plan(result)

            # For joins, replace the raw Column/DataFrame reprs with the SQL
            # condition extracted from the analyzed plan — much more readable.
            if op_name == "join":
                join_cond = _extract_join_condition(result)
                how = kwargs.get("how", "inner")
                if join_cond:
                    args_repr = [f"on: {join_cond}", f"how: {how}"]
                else:
                    args_repr = [f"how: {how}"]
            else:
                args_repr = [_safe_repr(a) for a in args]

            _registry.register_child(
                child_id,
                parent_ids=parent_ids,
                operation=op_name,
                args_repr=args_repr,
                caller=caller,
                output_cols=out_cols,
                column_refs=col_refs,
            )

        elif isinstance(result, GroupedData):
            # Propagate tracking through groupBy so .agg() result is also tracked
            result._dfi_id = source_id
            result._dfi_groupby_op = op_name
            result._dfi_groupby_args = [_safe_repr(a) for a in args]
            result._dfi_groupby_caller = caller

        elif op_name in _VIEW_METHODS and args:
            _registry.register_view(str(args[0]), source_id)

        return result

    return wrapper


def _wrap_grouped(grouped_obj, op_name: str, method):
    """Wrapper for GroupedData methods that produce a DataFrame."""
    source_id      = _BASE(grouped_obj, "__dict__").get("_dfi_id")
    groupby_op     = _BASE(grouped_obj, "__dict__").get("_dfi_groupby_op", "groupBy")
    groupby_args   = _BASE(grouped_obj, "__dict__").get("_dfi_groupby_args", [])
    groupby_caller = _BASE(grouped_obj, "__dict__").get("_dfi_groupby_caller")

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        result = method(*args, **kwargs)
        if isinstance(result, DataFrame) and source_id:
            child_id = uuid4().hex
            result._dfi_id = child_id
            combined_op  = f"{groupby_op}({', '.join(groupby_args)}).{op_name}"
            try:
                out_cols = list(result.columns)
            except Exception:
                out_cols = []
            col_refs = _extract_col_refs_from_plan(result)
            _registry.register_child(
                child_id,
                parent_ids=[source_id],
                operation=combined_op,
                args_repr=[_safe_repr(a) for a in args],
                caller=groupby_caller,
                output_cols=out_cols,
                column_refs=col_refs,
            )
        return result

    return wrapper


def _safe_repr(val) -> str:
    try:
        r = repr(val)
        return r if len(r) <= 120 else r[:117] + "..."
    except Exception:
        return "<unrepresentable>"


def _patched_getattribute(self, name: str):
    value = _BASE(self, name)
    dfi_id = _BASE(self, "__dict__").get("_dfi_id")
    if dfi_id is None:
        if _session_active and not name.startswith("_") and callable(value):
            # Auto-register this DataFrame as a root on first method call.
            dfi_id = uuid4().hex
            _BASE(self, "__dict__")["_dfi_id"] = dfi_id
            try:
                out_cols = list(_BASE(self, "columns"))
            except Exception:
                out_cols = []
            _registry.register_root(dfi_id, name=None, output_cols=out_cols,
                                    caller=CallerInfo.capture(depth=3))
        else:
            return value
    # Tag DataFrameWriter with the source id so tracked_writer.py can intercept writes.
    if not name.startswith("_") and isinstance(value, DataFrameWriter):
        _BASE(value, "__dict__")["_dfi_source_id"] = dfi_id
    if name.startswith("_") or not callable(value):
        return value
    return _wrap(dfi_id, name, value)


def _grouped_patched_getattribute(self, name: str):
    value = _BASE(self, name)
    dfi_id = _BASE(self, "__dict__").get("_dfi_id")
    if dfi_id is None or name.startswith("_") or not callable(value):
        return value
    return _wrap_grouped(self, name, value)


# Install both patches once at import time
DataFrame.__getattribute__ = _patched_getattribute
GroupedData.__getattribute__ = _grouped_patched_getattribute
