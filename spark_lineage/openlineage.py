"""
OpenLineage emitter for spark_lineage.

Converts the in-memory lineage registry into a standard OpenLineage COMPLETE
RunEvent and POSTs it to any compliant collector (Marquez, DataHub, etc.).

Usage:
    import spark_lineage as spl

    spl.track_df(orders, "orders")
    # ... transforms ...

    spl.emit_openlineage(
        source_df   = orders,
        url         = "http://marquez:5000",
        job_name    = "preprocess_orders",
        job_namespace = "data_platform",   # optional, defaults to "default"
        run_id      = airflow_run_id,      # optional, defaults to a random UUID
    )
"""

from __future__ import annotations

import json
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional

from .report import _build_json        # reuse column-trace logic

_PRODUCER      = "https://github.com/spark-lineage/spark-lineage"
_COL_SCHEMA    = "https://openlineage.io/spec/1-2-0/ColumnLineageDatasetFacet.json"
_SCHEMA_SCHEMA = "https://openlineage.io/spec/1-2-0/SchemaDatasetFacet.json"
_RUN_SCHEMA    = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"


# ── public API ────────────────────────────────────────────────────────────────

def build_event(
    source_df,
    run_id: str,
    job_name: str,
    job_namespace: str = "default",
) -> dict:
    """
    Build a complete OpenLineage RunEvent dict from the current registry state.

    Returns the raw dict — useful if you want to inspect it, store it, or ship
    it yourself without calling emit().
    """
    from .core.tracked_df import _BASE
    source_id = _BASE(source_df, "__dict__").get("_dfi_id")
    if not source_id:
        raise ValueError("DataFrame is not tracked. Call spl.track_df(df) first.")

    data = _build_json(source_id, source_df, job_name)

    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer":  _PRODUCER,
        "schemaURL": _RUN_SCHEMA,
        "run":  {"runId": run_id},
        "job":  {"namespace": job_namespace, "name": job_name},
        "inputs":  _build_inputs(data["source"]),
        "outputs": _build_outputs(data["targets"], data["traces"], data["source"]),
    }


def emit(
    source_df,
    url: str,
    job_name: str,
    run_id: Optional[str] = None,
    job_namespace: str = "default",
) -> dict:
    """
    Build and POST an OpenLineage COMPLETE event to a collector.

    Compatible with Marquez (/api/v1/lineage), DataHub, and any OpenLineage
    HTTP transport.  Returns the event dict so callers can log or inspect it.

    Raises urllib.error.URLError if the collector is unreachable.
    """
    _run_id = run_id or str(uuid.uuid4())
    event   = build_event(source_df, _run_id, job_name, job_namespace)

    body = json.dumps(event, default=str).encode("utf-8")
    req  = urllib.request.Request(
        url.rstrip("/") + "/api/v1/lineage",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        status = resp.status

    print(f"[spark_lineage] OpenLineage event → {url}  (HTTP {status})")
    return event


# ── event builders ────────────────────────────────────────────────────────────

def _build_inputs(source: dict) -> list:
    name = source.get("name") or source["id"][:8]
    return [{
        "namespace": "default",
        "name":      name,
        "facets": {
            "schema": _schema_facet(source.get("columns", [])),
        }
    }]


def _build_outputs(targets: list, traces: dict, source: dict) -> list:
    source_name = source.get("name") or source["id"][:8]
    outputs = []

    for t in targets:
        tid  = t["id"]
        name = t.get("name") or t.get("label") or tid[:8]

        col_lineage_fields = {}

        for col in t["columns"]:
            trace  = (traces.get(tid) or {}).get(col)
            if not trace:
                continue

            steps   = trace.get("steps", [])
            sources = trace.get("sources") or []

            # passthrough: same-named column flows unchanged from source
            if not sources and steps and steps[0].get("role") == "source":
                sources = [col]

            if not sources:
                continue

            # find the first step that actually changed the column
            first_transform = next(
                (s for s in steps if s["role"] in ("created", "modified")), None
            )

            transform_type = "DIRECT" if not first_transform else "INDIRECT"
            transform_desc = ""
            subtype        = "IDENTITY"

            if first_transform:
                op     = first_transform.get("operation", "")
                caller = first_transform.get("caller")
                loc    = f" at {caller['file_short']}:{caller['line']}" if caller else ""
                transform_desc = f".{op}(){loc}"
                subtype = "AGGREGATION" if "agg" in op.lower() else "TRANSFORMATION"

            col_lineage_fields[col] = {
                "inputFields": [
                    {
                        "namespace": "default",
                        "name":      source_name,
                        "field":     src_col,
                        "transformations": [{
                            "type":        transform_type,
                            "subtype":     subtype,
                            "description": transform_desc,
                            "masking":     False,
                        }],
                    }
                    for src_col in sources
                ]
            }

        output: dict = {
            "namespace": "default",
            "name":      name,
            "facets": {
                "schema": _schema_facet(t["columns"]),
            }
        }

        if col_lineage_fields:
            output["facets"]["columnLineage"] = {
                "_producer":  _PRODUCER,
                "_schemaURL": _COL_SCHEMA,
                "fields":     col_lineage_fields,
            }

        outputs.append(output)

    return outputs


def _schema_facet(columns: list) -> dict:
    return {
        "_producer":  _PRODUCER,
        "_schemaURL": _SCHEMA_SCHEMA,
        "fields":     [{"name": c} for c in columns],
    }
