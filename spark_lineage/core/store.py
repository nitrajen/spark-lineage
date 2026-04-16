"""
Lineage materialization store — bridges write→read boundaries across persistence.

Two implementations:
  MemoryStore  — in-process only (same Python session, e.g. Kedro default runner)
  FileStore    — cross-process safe (one JSON file per run_id, e.g. Airflow tasks)

Both share the same two-method interface so callers are fully decoupled from storage.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import json
import os


class LineageStore(ABC):
    @abstractmethod
    def save(self, run_id: str, dest: str, df_id: str, output_cols: list[str]) -> None:
        """Record that df_id was written to dest under run_id."""

    @abstractmethod
    def load(self, run_id: str, dest: str) -> Optional[tuple[str, list[str]]]:
        """Return (df_id, output_cols) if dest was written under run_id, else None."""


class MemoryStore(LineageStore):
    def __init__(self):
        self._data: dict[tuple[str, str], tuple[str, list[str]]] = {}

    def save(self, run_id: str, dest: str, df_id: str, output_cols: list[str]) -> None:
        self._data[(run_id, _norm(dest))] = (df_id, output_cols)

    def load(self, run_id: str, dest: str) -> Optional[tuple[str, list[str]]]:
        return self._data.get((run_id, _norm(dest)))


class FileStore(LineageStore):
    """
    Persists materializations as JSON files under `directory/<run_id>.json`.
    One file per run_id means concurrent runs never contend.

    Usage:
        store = FileStore("/shared/nfs/.dfi")            # local/NFS path
        store = FileStore("/dbfs/.dfi")                  # Databricks
        # s3 paths not directly writable; mount first or use a local path
    """

    def __init__(self, directory: str):
        self._dir = directory
        os.makedirs(directory, exist_ok=True)

    def save(self, run_id: str, dest: str, df_id: str, output_cols: list[str]) -> None:
        path = self._run_path(run_id)
        data = self._read(path)
        data[_norm(dest)] = {"df_id": df_id, "output_cols": output_cols}
        with open(path, "w") as f:
            json.dump(data, f)

    def load(self, run_id: str, dest: str) -> Optional[tuple[str, list[str]]]:
        entry = self._read(self._run_path(run_id)).get(_norm(dest))
        if entry:
            return entry["df_id"], entry["output_cols"]
        return None

    def _run_path(self, run_id: str) -> str:
        safe = run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
        return os.path.join(self._dir, f"{safe}.json")

    def _read(self, path: str) -> dict:
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


def _norm(dest: str) -> str:
    """Normalize path/table name for consistent matching across write and read."""
    return dest.rstrip("/").lower()
