"""
Kedro hook for automatic spark_lineage tracking.

Registers every PySpark DataFrame loaded from the Kedro catalog as a tracked
source, and saves one lineage report per source DataFrame after the pipeline run.

Usage — add to your project's settings.py:

    from spark_lineage.integrations.kedro_hook import SparkLineageHook

    HOOKS = (SparkLineageHook(report_dir="./lineage_reports"),)

This module has NO hard dependency on Kedro. If Kedro is not installed, importing
it is safe — the hook methods become plain Python methods that do nothing unless
called explicitly.
"""
from __future__ import annotations

import os
from typing import Any

# Lazy Kedro import: fall back to a no-op decorator when Kedro is absent so
# this file can be imported in any environment without errors.
try:
    from kedro.framework.hooks import hook_impl  # type: ignore
except ImportError:
    def hook_impl(fn):  # type: ignore
        """No-op hook_impl when Kedro is not installed."""
        return fn


class SparkLineageHook:
    """
    Kedro pipeline hook that auto-tracks PySpark DataFrames.

    Args:
        report_dir:
            Directory where .json + .html report files are written.
            Defaults to "./lineage_reports".
        primary_datasets:
            Optional list of dataset names to use as report roots (the
            ``source_df`` argument to ``save_report``).  If ``None``, every
            loaded DataFrame becomes a root and gets its own report — useful
            for inspecting all inputs.  If specified, only those datasets
            produce reports (e.g. ``["raw_orders", "raw_customers"]``).
    """

    def __init__(
        self,
        report_dir: str = "./lineage_reports",
        primary_datasets: list[str] | None = None,
    ) -> None:
        self._report_dir       = report_dir
        self._primary_datasets = primary_datasets
        self._tracked: dict[str, Any] = {}   # dataset_name → DataFrame

    @hook_impl
    def after_dataset_loaded(self, dataset_name: str, data: Any) -> None:
        """Called by Kedro after each catalog dataset is loaded."""
        try:
            from pyspark.sql import DataFrame
        except ImportError:
            return
        if not isinstance(data, DataFrame):
            return

        import spark_lineage as spl
        spl.track_df(data, name=dataset_name)
        self._tracked[dataset_name] = data

    @hook_impl
    def after_pipeline_run(
        self,
        run_params: dict | None = None,
        pipeline=None,
        catalog=None,
    ) -> None:
        """Called by Kedro after the full pipeline completes."""
        import spark_lineage as spl

        os.makedirs(self._report_dir, exist_ok=True)

        roots = (
            [n for n in self._primary_datasets if n in self._tracked]
            if self._primary_datasets
            else list(self._tracked.keys())
        )

        for name in roots:
            df   = self._tracked[name]
            safe = name.replace("/", "_").replace("\\", "_")
            path = os.path.join(self._report_dir, safe)
            try:
                spl.save_report(df, path=path, name=name)
            except Exception as exc:
                print(f"[spark_lineage] Warning: could not save report for '{name}': {exc}")
