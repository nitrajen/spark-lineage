"""
Patches DataFrameReader.__getattribute__ once at import time.

When a terminal read method returns a DataFrame:
  1. If the destination matches a previously-written tracked DataFrame (via the
     store): register a read node linked as a child → lineage chain continues
     through the persistence boundary.
  2. If no match but a session is active: register the DataFrame as a named
     root (destination as name) so it enters the lineage graph.
  3. Outside a session: no-op.

No read method names are hardcoded: all reader methods are wrapped and only
act when they return a DataFrame.
"""
from __future__ import annotations
import functools
from uuid import uuid4
from pyspark.sql import DataFrameReader, DataFrame
from .registry import _registry
from .node import CallerInfo

_BASE = object.__getattribute__

# Set by the session() context manager.
_session_run_id: str | None = None
_session_store = None   # LineageStore | None
_session_active: bool = False  # mirrors tracked_df._session_active


def _set_session(run_id, store, active: bool) -> None:
    global _session_run_id, _session_store, _session_active
    _session_run_id = run_id
    _session_store = store
    _session_active = active


def _dest_from(args, kwargs) -> str | None:
    for a in args:
        if isinstance(a, str):
            return a
    for key in ("path", "table", "name"):
        if isinstance(kwargs.get(key), str):
            return kwargs[key]
    return None


def _safe_repr(val) -> str:
    try:
        r = repr(val)
        return r if len(r) <= 120 else r[:117] + "..."
    except Exception:
        return "<unrepresentable>"


def _wrap_read(op_name: str, method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        caller = CallerInfo.capture(depth=2)
        result = method(*args, **kwargs)

        if isinstance(result, DataFrame) and _session_active:
            dest = _dest_from(args, kwargs)

            if dest and _session_store is not None and _session_run_id is not None:
                entry = _session_store.load(_session_run_id, dest)
                if entry:
                    # Bridge: this read continues the lineage of a prior write.
                    parent_id, parent_cols = entry
                    child_id = uuid4().hex
                    _BASE(result, "__dict__")["_dfi_id"] = child_id
                    try:
                        out_cols = list(result.columns)
                    except Exception:
                        out_cols = parent_cols
                    _registry.register_child(
                        child_id,
                        parent_ids=[parent_id],
                        operation=f"read.{op_name}",
                        args_repr=[_safe_repr(a) for a in args],
                        caller=caller,
                        output_cols=out_cols,
                    )
                    return result

            # No materialization match — register as a named root.
            if dest and not _BASE(result, "__dict__").get("_dfi_id"):
                dfi_id = uuid4().hex
                _BASE(result, "__dict__")["_dfi_id"] = dfi_id
                _registry.register_root(dfi_id, name=dest)

        return result
    return wrapper


def _patched_reader_getattribute(self, name: str):
    value = _BASE(self, name)
    if name.startswith("_") or not callable(value):
        return value
    if not _session_active:
        return value
    return _wrap_read(name, value)


DataFrameReader.__getattribute__ = _patched_reader_getattribute
