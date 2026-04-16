from . import core as _core  # installs DataFrame / DataFrameWriter / DataFrameReader patches
from .core.registry import _registry
from .core import tracked_df as _tdf
from .core import tracked_writer as _twr
from .core import tracked_reader as _trd
from .core.store import LineageStore, MemoryStore, FileStore
from .report import print_lineage, get_lineage, save_report
from .openlineage import build_event as build_ol_event, emit as emit_openlineage
from .core.column_lineage import get_col_lineage
from uuid import uuid4
import functools
from contextlib import contextmanager


def track_df(df, name: str = None):
    """Explicitly mark a DataFrame as a lineage root."""
    df._dfi_id = uuid4().hex
    try:
        cols = list(df.columns)
    except Exception:
        cols = []
    _registry.register_root(df._dfi_id, name=name, output_cols=cols)
    return df


def column_lineage(df, col_name: str) -> dict:
    """Trace col_name back to its source columns via Spark's query plan."""
    return get_col_lineage(df, col_name)


@contextmanager
def session(run_id: str = None, store: LineageStore = None):
    """
    Context manager that auto-tracks all DataFrames within the block.

    run_id : opaque string scoping this run's materializations.
             Use whatever your orchestrator provides (Airflow run_id, Flyte
             execution id, Kedro session id, or any string you choose).
             Defaults to a random UUID (ephemeral, in-process only).

    store  : where materializations are persisted.
             MemoryStore() — default, in-process only.
             FileStore(path) — JSON files on disk, safe for cross-process use.

    Within the block:
      - Any DataFrame is automatically registered as a root on first use.
      - Writes (df.write.X()) register the destination in the store.
      - Reads (spark.read.X()) that match a prior write are linked as children
        → lineage flows through the persistence boundary transparently.

    Example (same process):
        with spl.session():
            df = spark.read.parquet("fact_orders")
            df = df.filter(...).withColumn(...)
            df.write.saveAsTable("enriched_orders")

    Example (cross-process, e.g. two Airflow tasks sharing a FileStore):
        store = spl.FileStore("/shared/.dfi")

        # Task A
        with spl.session(run_id=context["run_id"], store=store):
            df = spark.read.parquet("fact_orders")
            df = df.filter(...).withColumn(...)
            df.write.saveAsTable("enriched_orders")

        # Task B (different process, same run_id + store)
        with spl.session(run_id=context["run_id"], store=store):
            df = spark.read.table("enriched_orders")  # ← linked to Task A's write
            df = df.groupBy(...).agg(...)
            spl.save_report(df, "report")
    """
    _store  = store or MemoryStore()
    _rid    = run_id or uuid4().hex

    _tdf._session_active = True
    _twr._set_session(_rid, _store)
    _trd._set_session(_rid, _store, active=True)

    try:
        yield _rid
    finally:
        _tdf._session_active = False
        _twr._set_session(None, None)
        _trd._set_session(None, None, active=False)


def tracked_task(func):
    """
    Decorator that auto-tracks DataFrame arguments entering a task function.

    Any DataFrame argument without a _dfi_id is registered as a lineage root
    before the function body runs. Works with any callable — no framework
    coupling.

        @spl.tracked_task
        def build_features(orders: DataFrame, products: DataFrame) -> DataFrame:
            return orders.join(products, "product_id").withColumn(...)
    """
    from pyspark.sql import DataFrame as _DF

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for arg in args:
            if isinstance(arg, _DF) and not _tdf._BASE(arg, "__dict__").get("_dfi_id"):
                track_df(arg, name=f"{func.__qualname__}:arg")
        for val in kwargs.values():
            if isinstance(val, _DF) and not _tdf._BASE(val, "__dict__").get("_dfi_id"):
                track_df(val, name=f"{func.__qualname__}:arg")
        return func(*args, **kwargs)

    return wrapper
