"""
Patches DataFrameWriter.__getattribute__ once at import time.

When a tracked DataFrame's write.X() method fires:
  - Methods that return None are terminal writes → register a write node +
    store the materialization so downstream reads can re-link the lineage.
  - Methods that return DataFrameWriter are chaining calls (format, option, mode, …)
    → propagate _dfi_source_id so the chain isn't lost.

No write method names are hardcoded: terminal vs chaining is determined purely
by return type.
"""
from __future__ import annotations
import functools
from uuid import uuid4
from pyspark.sql import DataFrameWriter, DataFrame
from .registry import _registry
from .node import CallerInfo

_BASE = object.__getattribute__

# Set by the session() context manager; None outside a session.
_session_run_id: str | None = None
_session_store = None  # LineageStore | None


def _set_session(run_id, store) -> None:
    global _session_run_id, _session_store
    _session_run_id = run_id
    _session_store = store


def _dest_from(args, kwargs) -> str | None:
    """
    Extract the write destination generically: first string positional arg,
    or a 'path' / 'table' / 'name' keyword arg.
    """
    for a in args:
        if isinstance(a, str):
            return a
    for key in ("path", "table", "name"):
        if isinstance(kwargs.get(key), str):
            return kwargs[key]
    return None


def _resolve_fqn(op_name: str, dest: str) -> str:
    """
    Resolve dest to a fully-qualified identifier.
    - Table writes (saveAsTable, insertInto): prepend current catalog + database
      if not already qualified.  Falls back to dest unchanged on any error.
    - File writes (parquet, csv, json, …): the path IS the identifier — returned as-is.
    """
    if op_name not in ("saveAsTable", "insertInto"):
        return dest  # file path — no resolution needed
    if not dest:
        return dest
    parts = dest.split(".")
    if len(parts) >= 3:
        return dest  # already catalog.schema.table
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if not spark:
            return dest
        db = spark.catalog.currentDatabase()
        try:
            cat = spark.catalog.currentCatalog()   # Spark 3.4+ / Unity Catalog
            if len(parts) == 1:
                return f"{cat}.{db}.{dest}"
            else:                                  # schema.table
                return f"{cat}.{dest}"
        except Exception:
            # Pre-3.4 Spark — no currentCatalog
            return f"{db}.{dest}" if len(parts) == 1 else dest
    except Exception:
        return dest


def _safe_repr(val) -> str:
    try:
        r = repr(val)
        return r if len(r) <= 120 else r[:117] + "..."
    except Exception:
        return "<unrepresentable>"


def _wrap_write(source_id: str, op_name: str, method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        caller = CallerInfo.capture(depth=2)
        result = method(*args, **kwargs)

        if result is None:
            # Terminal write — register a lineage node and the materialization.
            node = _registry.get(source_id)
            out_cols = list(node.output_cols) if node else []

            dest = _dest_from(args, kwargs)
            fqn  = _resolve_fqn(op_name, dest) if dest else None

            write_id = uuid4().hex
            _registry.register_child(
                write_id,
                parent_ids=[source_id],
                operation=f"write.{op_name}",
                args_repr=[_safe_repr(a) for a in args],
                caller=caller,
                output_cols=out_cols,
                name=fqn or dest,   # FQN for tables, path for files
            )

            if dest and _session_store is not None and _session_run_id is not None:
                _session_store.save(_session_run_id, dest, write_id, out_cols)

        elif isinstance(result, DataFrameWriter):
            # Chaining call — propagate the source id to the next writer.
            _BASE(result, "__dict__")["_dfi_source_id"] = source_id

        return result
    return wrapper


def _patched_writer_getattribute(self, name: str):
    value = _BASE(self, name)
    source_id = _BASE(self, "__dict__").get("_dfi_source_id")
    if source_id is None or name.startswith("_") or not callable(value):
        return value
    return _wrap_write(source_id, name, value)


DataFrameWriter.__getattribute__ = _patched_writer_getattribute
