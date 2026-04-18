from __future__ import annotations
import json
from datetime import datetime
from typing import Optional
from .core.registry import _registry, LineageRegistry
from .core.node import LineageNode, CallerInfo


# ── public API ────────────────────────────────────────────────────────────────

def get_lineage(df) -> list[LineageNode]:
    """Backward walk: all ancestor nodes of df in topological order."""
    try:
        from .core.tracked_df import _BASE
        df_id = _BASE(df, "__dict__").get("_dfi_id")
    except Exception:
        df_id = getattr(df, "_dfi_id", None)
    if not df_id:
        raise ValueError("DataFrame is not tracked.")
    visited, order = set(), []

    def walk(nid: str):
        if nid in visited:
            return
        visited.add(nid)
        node = _registry.get(nid)
        if node is None:
            return
        for pid in node.parent_ids:
            walk(pid)
        order.append(node)

    walk(df_id)
    return order


def print_lineage(df):
    """Text summary: ancestry chain of df."""
    nodes = get_lineage(df)
    id_to_step = {n.id: i + 1 for i, n in enumerate(nodes)}
    W = 68
    title = nodes[-1].name if nodes[-1].name else "DataFrame"
    lines = ["", "═" * W, f"  LINEAGE  ·  {title}", "═" * W]
    for i, node in enumerate(nodes):
        step = i + 1
        tag = f"[{node.name}]  " if node.name else ""
        if node.operation is None:
            lines.append(f"\n  #{step:<3} {tag}source")
        else:
            args = ", ".join(node.args_repr) if node.args_repr else ""
            lines.append(f"\n  #{step:<3} {tag}.{node.operation}({args})")
            frm = ", ".join(
                f"#{id_to_step[p]}" + (
                    f" [{_registry.get(p).name}]"
                    if _registry.get(p) and _registry.get(p).name else ""
                )
                for p in node.parent_ids if p in id_to_step
            )
            if frm:
                lines.append(f"       from  {frm}")
            if node.caller:
                f_ = node.caller.file.split("/")[-1]
                lines.append(f"         at  {f_}:{node.caller.line}  {node.caller.function}()")
    lines += ["", "═" * W, ""]
    print("\n".join(lines))


def save_report(source_df, path: str, name: str = None):
    """
    Save a lineage report as <path>.json and <path>.html.

    source_df — the tracked root DataFrame.
    path      — file path prefix (no extension).
    name      — optional report title.
    """
    try:
        from .core.tracked_df import _BASE
        df_id = _BASE(source_df, "__dict__").get("_dfi_id")
    except Exception:
        df_id = getattr(source_df, "_dfi_id", None)
    if not df_id:
        raise ValueError("DataFrame is not tracked. Call spl.track_df(source_df) first.")

    source_node = _registry.get(df_id)
    title = name or (source_node.name if source_node else None) or "Lineage Report"
    data = _build_json(df_id, source_df, title)

    json_str = json.dumps(data, default=str).replace("</", "<\\/")
    html = _VIEWER_TEMPLATE.replace(
        "// >>SPARK_LINEAGE_DATA<<\nconst DATA = __DATA__;\n// >>END_SPARK_LINEAGE_DATA<<",
        f"// >>SPARK_LINEAGE_DATA<<\nconst DATA = {json_str};\n// >>END_SPARK_LINEAGE_DATA<<"
    )
    _write(path + ".json", json.dumps(data, indent=2, default=str))
    _write(path + ".html", html)
    print(f"Report saved → {path}.json  |  {path}.html")


# ── JSON builder ──────────────────────────────────────────────────────────────

def _build_json(source_id: str, source_df, title: str) -> dict:
    """
    Builds a self-contained lineage document.

    Top-level shape
    ───────────────
    {
      meta:    report metadata
      sources: [                    ← every source DataFrame that fed any target
        {
          id, name, primary,        ← primary=true for the source_df passed in
          caller,                   ← file:line where track_df() was called
          columns,                  ← column list in schema order
          influence: {              ← for each source column …
            col: {                  ←   … which target IDs it reached …
              target_id: [cols]     ←   … and which target columns it influenced
            }
          }
        }
      ]
      targets: [                    ← every leaf (materialized) DataFrame
        {
          id, label, caller,
          source_dfs,               ← subset of sources[] that fed this target
          columns,                  ← column list in schema order
          col_roles: {col: role},   ← role per column: created|modified|passthrough
          traces: {                 ← full trace per column
            col: {
              expr,                 ← expression string (for created cols)
              sources,              ← ultimate source columns that contributed
              steps: [              ← one step per ancestor node that touches this col
                { operation, role, name, args, caller, context_key }
              ]
            }
          }
        }
      ]
    }
    """
    source_node = _registry.get(source_id)
    try:
        source_cols = list(source_df.columns)
    except Exception:
        source_cols = list(source_node.output_cols) if source_node else []

    leaf_ids = _registry.get_leaf_descendants(source_id)

    targets: list[dict]          = []
    all_src_map: dict[str, dict] = {}   # id → source entry (built incrementally)

    for tid in leaf_ids:
        node = _registry.get(tid)
        if node is None:
            continue

        ancestor_nodes = _get_all_ancestor_nodes(tid)
        col_src_map    = _build_col_source_map(ancestor_nodes)

        # Collect source DFs for this target + register in global map
        target_source_dfs: list[dict] = []
        for an in ancestor_nodes:
            if an.operation is None:
                target_source_dfs.append({
                    "id":     an.id,
                    "name":   an.name,
                    "caller": _caller_dict(an.caller),
                })
                if an.id not in all_src_map:
                    all_src_map[an.id] = {
                        "id":      an.id,
                        "name":    an.name,
                        "primary": an.id == source_id,
                        "caller":  _caller_dict(an.caller),
                        "columns": list(an.output_cols),
                    }

        # Per-column traces + role map
        col_traces: dict[str, dict] = {}
        col_roles:  dict[str, str]  = {}
        for col in node.output_cols:
            trace = _build_col_trace(col, ancestor_nodes, col_src_map)
            col_traces[col] = trace
            steps = trace.get("steps", [])
            if any(s["role"] == "created"  for s in steps): col_roles[col] = "created"
            elif any(s["role"] == "modified" for s in steps): col_roles[col] = "modified"
            else: col_roles[col] = "passthrough"

        targets.append({
            "id":         tid,
            "label":      _node_label(node, tid),
            "name":       node.name,
            "caller":     _caller_dict(node.caller),
            "source_dfs": target_source_dfs,
            "columns":    list(node.output_cols),
            "col_roles":  col_roles,
            "traces":     col_traces,
        })

    # Order: primary first, then alphabetically
    sources = sorted(
        all_src_map.values(),
        key=lambda s: (0 if s["primary"] else 1, s["name"] or "")
    )

    # Embed influence inside each source entry
    for src in sources:
        src["influence"] = _compute_source_influence(src["id"], src["columns"], targets)

    return {
        "meta": {
            "title":        title,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source_count": len(sources),
            "target_count": len(targets),
        },
        "sources": sources,
        "targets": targets,
    }


def _node_label(node: LineageNode, nid: str) -> str:
    if node.name:
        return node.name
    if node.operation:
        func = node.caller.function if node.caller else ""
        return f".{node.operation}()" + (f"  ·  {func}()" if func else "")
    return nid[:8]


def _caller_dict(caller) -> dict | None:
    if caller is None:
        return None
    module   = caller.module or ""
    qualname = caller.qualname or ""
    # Drop Python's "<module>" placeholder — module-level code just uses the module name
    qname_clean = qualname if (qualname and qualname != "<module>") else ""
    parts = [p for p in [module, qname_clean] if p]
    # Fully qualified identifier: module.qualname:line — unambiguous across any codebase
    fqn = ".".join(parts) + ":" + str(caller.line) if parts else None
    return {
        "module":      caller.module,
        "klass":       caller.klass,
        "func":        caller.function,
        "qualname":    caller.qualname,
        "fqn":         fqn,
        "file":        caller.file,
        "file_short":  caller.file.split("/")[-1],
        "line":        caller.line,
        "context_key": caller.qualname,
    }


# ── ancestry helpers ──────────────────────────────────────────────────────────

def _get_all_ancestor_nodes(target_id: str) -> list[LineageNode]:
    """
    Return ALL ancestor nodes of target_id (including every joined branch),
    topological order — roots first, target last.
    """
    all_nodes: dict[str, LineageNode] = {}

    def _back(nid: str):
        if nid in all_nodes:
            return
        node = _registry.get(nid)
        if node is None:
            return
        all_nodes[nid] = node
        for pid in node.parent_ids:
            _back(pid)

    _back(target_id)

    topo: list[str] = []
    visited: set[str] = set()

    def _topo(nid: str):
        if nid in visited or nid not in all_nodes:
            return
        visited.add(nid)
        for pid in all_nodes[nid].parent_ids:
            _topo(pid)
        topo.append(nid)

    _topo(target_id)
    return [all_nodes[nid] for nid in topo]


def _build_col_source_map(ancestor_nodes: list[LineageNode]) -> dict:
    """
    Build {col_name: set_of_(src_node_id, src_col)} from ALL ancestor nodes.
    Tracks source attribution as (source_df_id, col_name) tuples so that
    same-named columns in different source DataFrames are not confused.
    """
    csm: dict[str, set] = {}
    for node in ancestor_nodes:
        if node.operation is None:
            for col in node.output_cols:
                csm[col] = {(node.id, col)}   # (source_df_id, col_name)

    for node in ancestor_nodes:
        if node.operation is None:
            continue
        col_refs = node.column_refs or {}
        for col in (node.output_cols or []):
            parent_had = any(
                col in (_registry.get(pid).output_cols or [])
                for pid in node.parent_ids
                if _registry.get(pid)
            )
            if not parent_had:
                direct: set = set(col_refs.get(col, []))
                resolved: set = set()
                for ref in direct:
                    resolved |= csm.get(ref, set())
                csm[col] = resolved or set()
            elif col in col_refs:
                direct = set(col_refs[col])
                resolved = set()
                for ref in direct:
                    resolved |= csm.get(ref, set())
                if resolved:
                    csm[col] = resolved
    return csm


def _build_col_trace(col: str, ancestor_nodes: list[LineageNode],
                     col_src_map: dict = None) -> dict:
    """
    Build the vertical trace for a single column.
    Root nodes (operation=None) are labelled SOURCE, covering joined DataFrames.
    """
    steps = []
    expr: str = col
    # sources: list of {src_id, col, src_name} — explicit source DF attribution
    if col_src_map:
        raw_pairs = sorted(col_src_map.get(col, set()), key=lambda t: t[1] if isinstance(t, tuple) else t)
        sources: list = []
        for item in raw_pairs:
            if isinstance(item, tuple):
                src_id, src_col = item
                src_node = _registry.get(src_id)
                sources.append({
                    "src_id":   src_id,
                    "col":      src_col,
                    "src_name": src_node.name if src_node else None,
                })
            else:
                sources.append({"src_id": None, "col": item, "src_name": None})
    else:
        sources = []

    for node in ancestor_nodes:
        if col not in (node.output_cols or []):
            continue

        caller = _caller_dict(node.caller)

        if node.operation is None:
            role = "source"
        else:
            parent_had = any(
                col in (_registry.get(pid).output_cols or [])
                for pid in node.parent_ids
                if _registry.get(pid)
            )
            if not parent_had:
                role = "created"
                if node.args_repr:
                    expr = ", ".join(node.args_repr)
            elif col in (node.column_refs or {}) and set(node.column_refs[col]) != {col}:
                role = "modified"
            else:
                role = "passthrough"

        steps.append({
            "operation":   node.operation or "source",
            "role":        role,
            "name":        node.name or "",
            "args":        node.args_repr or [],
            "caller":      caller,
            "context_key": caller["context_key"] if caller else "",
        })

    return {"expr": expr, "sources": sources, "steps": steps}


def _compute_source_influence(src_id: str, source_cols: list, targets: list) -> dict:
    """
    Returns {source_col: {target_id: [influenced_target_cols]}}.
    Matches by (src_id, col) pairs to avoid cross-attributing same-named columns
    from different source DataFrames.
    """
    influence: dict = {}
    for sc in source_cols:
        influence[sc] = {}
        for t in targets:
            tid = t["id"]
            influenced = []
            for col in t["columns"]:
                trace = (t.get("traces") or {}).get(col)
                if trace is None:
                    continue
                src_pairs = trace.get("sources", [])
                steps     = trace.get("steps", [])
                # Match on exact (source_df_id, col_name) pair
                if any(
                    isinstance(p, dict) and p.get("src_id") == src_id and p.get("col") == sc
                    for p in src_pairs
                ):
                    influenced.append(col)
                elif (
                    col == sc
                    and steps
                    and steps[0].get("role") == "source"
                    and steps[0].get("name") == (_registry.get(src_id).name if _registry.get(src_id) else None)
                ):
                    influenced.append(col)
            if influenced:
                influence[sc][tid] = influenced
    return influence


def _write(path: str, content: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


# ── Viewer template ───────────────────────────────────────────────────────────

_VIEWER_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>spark-lineage report</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d1117;color:#e2e8f0;height:100vh;display:flex;flex-direction:column;overflow:hidden;font-size:13px}

/* header */
#hdr{padding:10px 18px;background:#161b27;border-bottom:1px solid #21283a;display:flex;align-items:baseline;gap:12px;flex-shrink:0}
#hdr h1{font-size:.9rem;font-weight:700;color:#7dd3fc}
#hdr .sub{font-size:.7rem;color:#374151}

/* layout */
#wrap{display:flex;flex:1;overflow:hidden}
#left{width:300px;min-width:200px;max-width:460px;background:#0f1623;border-right:1px solid #1a2235;display:flex;flex-direction:column;overflow:hidden;flex-shrink:0}
#drag{width:4px;cursor:col-resize;background:transparent;transition:background .12s;flex-shrink:0}
#drag:hover,#drag.on{background:#2d3f55}
#main{flex:1;overflow-y:auto;padding:28px 36px}

/* section label */
.sec{padding:7px 14px;font-size:.6rem;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.08em;background:#0d1117;border-bottom:1px solid #1a2235;flex-shrink:0}

/* source block (accordion) */
#src-block{border-bottom:1px solid #1a2235;flex:0 1 auto;overflow-y:auto;max-height:28%;min-height:40px}
.src-entry{border-bottom:1px solid #111827}
.src-entry:last-child{border-bottom:none}
.src-df-header{padding:7px 14px;cursor:pointer;user-select:none;display:flex;flex-wrap:wrap;align-items:center;gap:4px 6px;transition:background .1s;border-left:2px solid transparent}
.src-df-header:hover{background:#1a2235}
.src-df-chevron{font-size:.6rem;color:#374151;width:12px;flex-shrink:0;transition:transform .15s}
.src-df-header.expanded .src-df-chevron{transform:rotate(90deg)}
.src-df-label{font-size:.72rem;font-weight:600;color:#7dd3fc;flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.src-df-header.secondary .src-df-label{color:#94a3b8}
.src-df-loc{font-size:.6rem;color:#374151;white-space:nowrap}
/* FQN is the stable identifier — shown prominently so engineers can locate the DF in code */
.src-df-fqn{font-size:.63rem;color:#7dd3fc;font-family:'SF Mono',Consolas,monospace;font-weight:400;margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;width:100%;padding-right:4px;opacity:.7}
.src-df-header.selected{background:#0c1f38;border-left:2px solid #38bdf8}
.src-df-header.selected .src-df-label{color:#7dd3fc}
.src-cols-wrap{padding:4px 14px 8px 30px;display:none;flex-wrap:wrap;gap:3px}
.src-cols-wrap.open{display:flex}
.col-chip{font-size:.6rem;padding:1px 7px;border-radius:9px;background:#1a2235;color:#64748b;border:1px solid #21283a;white-space:nowrap;cursor:pointer;transition:all .15s}
.col-chip:hover{background:#1e2d40;color:#94a3b8;border-color:#2d3f55}
.col-chip.tracked{background:#1e3a5f;color:#7dd3fc;border-color:#38bdf8;box-shadow:0 0 0 1px #38bdf8}
.col-chip.trace-source{background:#052e16;color:#34d399;border-color:#166534}

/* targets list */
#targets-wrap{flex:1 0 100px;overflow-y:auto;padding:4px 0}
.target-item{padding:7px 14px;cursor:pointer;border-left:2px solid transparent;transition:all .1s}
.target-item:hover{background:#1a2235}
.target-item.active{background:#1e2d40;border-left-color:#38bdf8}
.t-label{font-size:.75rem;color:#94a3b8;font-weight:500;word-break:break-word;line-height:1.4}
.t-sub{font-size:.62rem;color:#374151;margin-top:1px}
.target-item.dim{opacity:.4}
.target-item.influenced .t-label{color:#7dd3fc}
/* Source DF chips — always shown under every target.
   sel = currently selected source (bright), unsel = real source but not selected (dimmed) */
.t-influence-badge{display:flex;flex-wrap:wrap;gap:2px;margin-top:3px}
.t-src-chip{font-size:.55rem;padding:1px 6px;border-radius:7px;white-space:nowrap}
.t-src-chip.sel{color:#38bdf8;background:#0c2340;border:1px solid #1d4ed8}
.t-src-chip.unsel{color:#94a3b8;background:#1a2235;border:1px solid #374151}
.t-cols-badge{font-size:.55rem;color:#34d399;background:#052e16;border:1px solid #166534;border-radius:7px;padding:1px 5px;margin-left:2px}

/* columns panel */
#cols-panel{border-top:1px solid #1a2235;flex:0 1 auto;max-height:30%;min-height:60px;display:flex;flex-direction:column}
#col-search-wrap{padding:6px 10px;border-bottom:1px solid #1a2235;flex-shrink:0}
#col-search{width:100%;background:#161b27;border:1px solid #21283a;color:#e2e8f0;border-radius:4px;padding:4px 8px;font-size:.68rem;outline:none}
#col-search:focus{border-color:#2d3f55}
#col-list{overflow-y:auto;flex:1}
.col-item{padding:5px 14px;font-size:.72rem;color:#64748b;cursor:pointer;border-left:2px solid transparent;transition:all .1s;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.col-item:hover{background:#1a2235;color:#94a3b8}
.col-item.active{background:#1e2d40;color:#7dd3fc;border-left-color:#38bdf8}
.col-item.influenced{color:#38bdf8;background:#0c1f38;border-left-color:#1d4ed8}

/* empty/placeholder */
.empty-msg{font-size:.8rem;color:#374151;font-style:italic;padding-top:60px;text-align:center}

/* target overview */
.t-overview{max-width:860px}
.t-ov-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px;gap:14px}
.t-ov-title{font-size:1.15rem;font-weight:700;color:#e2e8f0;margin-bottom:3px;word-break:break-word;line-height:1.4}
.t-ov-sub{font-size:.72rem;color:#64748b}
.t-ov-stats{display:flex;flex-wrap:wrap;gap:0;margin-bottom:16px}
.t-ov-stat{display:inline-flex;align-items:center;gap:6px;margin-right:16px;font-size:.68rem;color:#64748b}
.t-ov-stat b{color:#94a3b8}
.t-ov-stat.cr b{color:#38bdf8}.t-ov-stat.mo b{color:#fb923c}
.t-ov-section{font-size:.6rem;font-weight:700;color:#4b5563;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;margin-top:18px}
.t-ov-section.highlight{color:#38bdf8;text-transform:none;font-size:.65rem;font-weight:400;letter-spacing:0}
.t-ov-cols{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px}
/* per-source-df breakdown */
.src-section{margin:18px 0 6px 0}
.src-section-hdr{display:flex;align-items:baseline;gap:8px;padding:0 0 6px 0;border-bottom:1px solid #1a2235;margin-bottom:10px}
.src-section-name{font-size:.72rem;font-weight:700;color:#7dd3fc}
.src-section-fqn{font-size:.62rem;color:#4b5563;font-family:'SF Mono',Consolas,monospace}
.src-col-group{margin-bottom:10px}
.src-col-group-label{font-size:.57rem;font-weight:700;color:#4b5563;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px}
.src-col-row-item{display:flex;align-items:baseline;flex-wrap:wrap;gap:3px;margin-bottom:4px}
.src-attr-arrow{font-size:.65rem;color:#374151;margin:0 2px}
.src-attr-chip{font-size:.68rem;color:#4b5563;white-space:nowrap}
.no-attr-note{font-size:.62rem;color:#374151;font-style:italic;margin-left:4px}
/* Column names in overview — plain clickable text, no pill */
.t-ov-chip{font-size:.75rem;cursor:pointer;white-space:nowrap;padding:0;background:transparent;border:none}
.t-ov-chip.created{color:#cbd5e1}
.t-ov-chip.modified{color:#cbd5e1}
.t-ov-chip.passthrough{color:#64748b}
.t-ov-chip:hover{color:#7dd3fc;text-decoration:underline}
.t-ov-desc{font-size:.65rem;color:#4b5563;margin:-4px 0 10px 0;line-height:1.5}
.src-summary-list{display:flex;flex-direction:column;gap:5px;margin-bottom:6px}
.src-summary-entry{display:flex;align-items:center;gap:6px}
.src-df-chip-static{font-size:.72rem;font-weight:600;color:#7dd3fc}
.src-df-chips{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:4px}
.src-df-chip{display:inline-block;font-size:.65rem;padding:2px 10px;border-radius:9px;cursor:default;white-space:nowrap}
.src-df-chip.sel{color:#7dd3fc;background:#0c2340;border:1px solid #38bdf8}
.src-df-chip.unsel{color:#6b7280;background:#111827;border:1px solid #374151}
.src-df-chip-loc{font-size:.58rem;color:#4b5563;margin-left:3px}

/* infl summary */
.infl-summary{margin-top:4px;margin-bottom:6px;padding:8px 10px;background:#0c1f38;border:1px solid #1d4ed8;border-radius:6px;font-size:.65rem;color:#7dd3fc}
/* not-influenced banner */
.not-infl-banner{margin:8px 0;padding:7px 12px;background:#1a1206;border:1px solid #78350f;border-radius:6px;font-size:.65rem;color:#d97706}
/* per-source-column attribution rows inside infl-summary */
.src-col-attr-row{display:flex;flex-wrap:wrap;align-items:flex-start;gap:4px;margin-bottom:6px;padding-bottom:6px;border-bottom:1px solid #1a3050}
.src-col-attr-row:last-child{margin-bottom:0;padding-bottom:0;border-bottom:none}
.src-col-attr-lhs{display:flex;align-items:center;gap:4px;flex-shrink:0}
.src-col-name{font-size:.65rem;font-weight:700;color:#34d399;background:#052e16;padding:2px 8px;border-radius:9px;border:1px solid #166534}
.src-col-df-name{font-size:.58rem;color:#4b5563}
.src-col-arrow{color:#374151;font-size:.65rem;margin:0 2px}
.src-col-tgt-chips{display:flex;flex-wrap:wrap;gap:3px}
.infl-col-chip{font-size:.6rem;padding:1px 7px;border-radius:9px;background:#1e3a5f;color:#38bdf8;border:1px solid #2563eb;cursor:pointer;white-space:nowrap}
.infl-col-chip:hover{opacity:.8}
.no-infl-note{font-size:.6rem;color:#374151;font-style:italic}
.infl-cols{display:flex;flex-wrap:wrap;gap:3px;margin-top:5px}

/* download bar */
.dl-bar{display:flex;gap:6px;flex-shrink:0}
.dl-btn{font-size:.62rem;padding:3px 10px;border-radius:4px;background:#161b27;color:#4b5563;border:1px solid #1e2d40;cursor:pointer;transition:all .15s;white-space:nowrap}
.dl-btn:hover{background:#1e2d40;color:#94a3b8;border-color:#2d3f55}

/* trace */
.trace-header{margin-bottom:20px}
.back-btn{background:none;border:none;color:#4b5563;font-size:.65rem;cursor:pointer;padding:0;margin-bottom:6px;display:block;transition:color .12s}
.back-btn:hover{color:#7dd3fc}
.trace-col-name{font-size:1.25rem;font-weight:700;color:#e2e8f0;margin-bottom:3px}
.trace-target{font-size:.7rem;color:#374151;margin-bottom:8px}
.trace-target span{color:#64748b}
.src-refs{display:flex;flex-wrap:wrap;gap:4px;align-items:center;margin-top:8px}
.src-refs-label{font-size:.62rem;color:#374151;margin-right:4px}
.src-ref-chip{font-size:.65rem;color:#34d399;background:#052e16;padding:2px 8px;border-radius:9px;border:1px solid #166534;cursor:pointer;transition:all .15s}
.src-ref-chip:hover{opacity:.8}

/* context group */
.ctx-group{margin-bottom:8px}
.ctx-header{cursor:pointer;padding:8px 12px 8px 14px;background:#0f1623;border:1px solid #1a2235;border-radius:6px;display:flex;justify-content:space-between;align-items:center;user-select:none;transition:all .12s}
.ctx-header:hover{background:#1a2235;border-color:#2d3f55}
.ctx-info{display:flex;flex-direction:column;gap:2px;min-width:0}
.ctx-name{font-size:.78rem;color:#94a3b8;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ctx-loc{font-size:.62rem;color:#4b5563}
.ctx-meta{display:flex;align-items:center;gap:10px;flex-shrink:0;margin-left:12px}
.ctx-roles{font-size:.6rem}
.ctx-toggle-icon{font-size:.7rem;color:#4b5563;width:14px;text-align:center;flex-shrink:0}
.ctx-steps{padding:4px 0 4px 20px}
.ctx-steps.collapsed{display:none}

/* vertical timeline inside context groups */
.timeline{position:relative;padding-left:28px}
.timeline::before{content:'';position:absolute;left:9px;top:12px;bottom:12px;width:1px;background:#1e2d40}
.step{position:relative;margin-bottom:2px}
.step-dot{position:absolute;left:-22px;top:12px;width:11px;height:11px;border-radius:50%;border:2px solid #0d1117}
.step.source   .step-dot{background:#34d399;border-color:#166534}
.step.created  .step-dot{background:#38bdf8;border-color:#0c4a6e;width:13px;height:13px;left:-23px;top:11px}
.step.modified .step-dot{background:#fb923c;border-color:#7c2d12;width:13px;height:13px;left:-23px;top:11px}
.step.passthrough .step-dot{background:#1e2d40;border-color:#334155}
.step-card{padding:9px 14px;border-radius:7px;margin-bottom:2px;border:1px solid transparent}
.step.source    .step-card{background:#052e16;border-color:#166534}
.step.created   .step-card{background:#0c2340;border-color:#1d4ed8}
.step.modified  .step-card{background:#431407;border-color:#c2410c}
.step.passthrough .step-card{background:transparent;border-color:transparent}
.step.passthrough .step-card:hover{background:#0f1623;border-color:#1e2d40}
.step-role{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px}
.step.source    .step-role{color:#34d399}
.step.created   .step-role{color:#38bdf8}
.step.modified  .step-role{color:#fb923c}
.step.passthrough .step-role{color:#2d3f55}
.step-op{font-size:.8rem;color:#cbd5e1;font-weight:600;margin-bottom:2px}
.step.passthrough .step-op{color:#4b5563;font-weight:400;font-size:.72rem}
.step-args{font-family:'SF Mono',Consolas,monospace;font-size:.65rem;color:#6b7280;margin-top:3px;white-space:pre-wrap;word-break:break-all;max-height:64px;overflow-y:auto}
.step-loc{font-size:.62rem;color:#2d3f55;margin-top:4px}
.step-loc span{color:#374151}
</style>
</head>
<body>
<div id="hdr">
  <h1 id="pg-title">Lineage</h1>
  <span class="sub" id="pg-sub"></span>
</div>
<div id="wrap">
  <div id="left">
    <div class="sec">Sources <span id="src-count" style="font-weight:400;text-transform:none;letter-spacing:0;color:#374151"></span></div>
    <div id="src-block"></div>
    <div class="sec">Targets <span id="t-count" style="font-weight:400;text-transform:none;letter-spacing:0;color:#374151"></span></div>
    <div id="targets-wrap"></div>
    <div id="cols-panel" style="display:none">
      <div class="sec">Columns</div>
      <div id="col-search-wrap"><input id="col-search" placeholder="Search columns…" oninput="filterCols(this.value)"></div>
      <div id="col-list"></div>
    </div>
  </div>
  <div id="drag"></div>
  <div id="main"><p class="empty-msg">Select a source column to track its influence, or a target to inspect it.</p></div>
</div>
<script>
// >>SPARK_LINEAGE_DATA<<
const DATA = __DATA__;
// >>END_SPARK_LINEAGE_DATA<<

// ── fast lookups (built once at init) ─────────────────────────────────────
const SRC_MAP = {};
const TGT_MAP = {};
const COL_TO_SRC_ID = {};
DATA.sources.forEach(s => {
  SRC_MAP[s.id] = s;
  (s.columns || []).forEach(c => { if (!COL_TO_SRC_ID[c]) COL_TO_SRC_ID[c] = s.id; });
});
DATA.targets.forEach(t => { TGT_MAP[t.id] = t; });

// ── application state ──────────────────────────────────────────────────────
// Hierarchical selection: source DFs → source columns → target → target column
//
// activeSrcIds   : Set<srcId>  — source DFs whose accordion is open / selected
//                  DF mode: highlight targets that are fed by ALL selected DFs (AND)
// activeSrcCols  : Map<"srcId|col", {srcId, col}> — multi-selected source columns
//                  Column mode: highlight targets/cols influenced by ANY selected col (OR)
// activeTarget   : selected target DF id
// activeCol      : selected target column
const STATE = {
  activeSrcIds:  new Set(),   // DF multi-select (AND filter on targets)
  activeSrcCols: new Map(),   // column multi-select: key = "srcId|col"
  activeTarget:  null,
  activeCol:     null,
};

const GRP_COLLAPSED = {};

// ── helpers ────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? '')
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// Returns influence map for the current selection:
// - Column mode (source cols selected): {targetId: [influencedCols]} — UNION across all selected cols
// - DF mode (source DFs selected, no cols): {targetId: []} — AND: target must use ALL selected DFs
// - null if nothing selected
function currentInfluence() {
  // Column mode — union of influences from every selected source column
  if (STATE.activeSrcCols.size > 0) {
    const result = {};
    STATE.activeSrcCols.forEach(({srcId, col}) => {
      const colInfl = ((SRC_MAP[srcId] || {}).influence || {})[col] || {};
      Object.entries(colInfl).forEach(([tid, cols]) => {
        if (!result[tid]) result[tid] = new Set();
        cols.forEach(c => result[tid].add(c));
      });
    });
    // Convert Sets to arrays
    const out = {};
    Object.entries(result).forEach(([tid, set]) => { out[tid] = [...set]; });
    return out;
  }

  if (STATE.activeSrcIds.size === 0) return null;

  // DF mode — AND: only targets that are fed by EVERY selected source DF
  // (selecting orders + customers → only targets that use both)
  const allSrcIds = Array.from(STATE.activeSrcIds);
  let candidateTids = null;
  allSrcIds.forEach(srcId => {
    const src = SRC_MAP[srcId];
    if (!src) return;
    const thisTids = new Set();
    Object.values(src.influence || {}).forEach(tgtMap => {
      Object.keys(tgtMap).forEach(tid => thisTids.add(tid));
    });
    candidateTids = candidateTids === null
      ? thisTids
      : new Set([...candidateTids].filter(tid => thisTids.has(tid)));
  });
  const result = {};
  (candidateTids || new Set()).forEach(tid => { result[tid] = []; });
  return result;
}

// Which selected source DF names feed a given target (for DF-mode badges)
// Use t.source_dfs (ancestor-based) instead of influence map so that
// sources contributing only count(*) or join keys aren't silently dropped.
function feedingSrcNames(tid) {
  const t = TGT_MAP[tid];
  if (!t) return [];
  const tgtSrcIds = new Set((t.source_dfs || []).map(s => s.id));
  return Array.from(STATE.activeSrcIds)
    .filter(sid => tgtSrcIds.has(sid))
    .map(sid => (SRC_MAP[sid] || {}).name || sid.slice(0, 6));
}

function targetName(t) {
  if (t.caller && t.caller.qualname) return t.caller.qualname + '()';
  if (t.name) return t.name;
  return t.label;
}
function targetSub(t) {
  return t.caller ? t.caller.file_short + ':' + t.caller.line : '';
}
function srcDisplayName(src) {
  const name = src.name || null;
  const loc  = src.caller ? src.caller.file_short + ':' + src.caller.line : null;
  if (name && loc) return name + ' \\u00b7 ' + loc;
  if (name)        return name;
  if (loc)         return loc;
  return src.id.slice(0, 8);
}
function srcShortName(src) {
  return src.name || (src.caller ? src.caller.file_short : src.id.slice(0, 8));
}

// ── init header ───────────────────────────────────────────────────────────
document.getElementById('pg-title').textContent = DATA.meta.title;
document.getElementById('pg-sub').textContent =
  DATA.targets.length + ' target' + (DATA.targets.length !== 1 ? 's' : '');
document.getElementById('src-count').textContent =
  '(' + DATA.sources.length + ')';
document.getElementById('t-count').textContent =
  '(' + DATA.targets.length + ')';

// ── source panel ───────────────────────────────────────────────────────────
// Clicking a source DF header selects/deselects it (multi-select) AND expands/collapses columns.
// Clicking a column chip tracks that column's influence (single column at a time).
function buildSourcePanel() {
  const block = document.getElementById('src-block');
  block.innerHTML = '';
  DATA.sources.forEach(src => {
    const entry = document.createElement('div');
    entry.className = 'src-entry';
    entry.dataset.srcId = src.id;

    const hdr = document.createElement('div');
    hdr.className = 'src-df-header' + (src.primary ? '' : ' secondary');
    hdr.id = 'srchdr_' + src.id;

    const chevron = document.createElement('span');
    chevron.className = 'src-df-chevron';
    chevron.textContent = '\\u25B6';

    const labelEl = document.createElement('span');
    labelEl.className = 'src-df-label';
    labelEl.textContent = src.name || src.id.slice(0, 8);

    hdr.appendChild(chevron);
    hdr.appendChild(labelEl);

    // Fully qualified name: module.qualname:line — unambiguous DF identifier
    if (src.caller && src.caller.fqn) {
      const fqnEl = document.createElement('div');
      fqnEl.className = 'src-df-fqn';
      fqnEl.textContent = src.caller.fqn;
      hdr.appendChild(fqnEl);
    }

    hdr.onclick = () => toggleSrcSelect(src.id);
    entry.appendChild(hdr);

    const colsWrap = document.createElement('div');
    colsWrap.className = 'src-cols-wrap';
    colsWrap.id = 'srccols_' + src.id;
    (src.columns || []).forEach(c => {
      const chip = document.createElement('span');
      chip.className = 'col-chip';
      chip.textContent = c;
      chip.title = 'Track influence of "' + c + '" through pipeline';
      chip.dataset.srcId = src.id;
      chip.dataset.col   = c;
      chip.onclick = e => { e.stopPropagation(); selectSrcCol(src.id, c); };
      colsWrap.appendChild(chip);
    });
    entry.appendChild(colsWrap);
    block.appendChild(entry);
  });
}

// Toggle selection+expansion of a source DF (multi-select)
function toggleSrcSelect(srcId) {
  if (STATE.activeSrcIds.has(srcId)) {
    STATE.activeSrcIds.delete(srcId);
    // Clear any source columns belonging to this DF
    STATE.activeSrcCols.forEach((v, k) => { if (v.srcId === srcId) STATE.activeSrcCols.delete(k); });
  } else {
    STATE.activeSrcIds.add(srcId);
  }
  render();
}

// Source DFs that contribute to the currently selected target column (for auto-expand)
function traceContribSrcIds() {
  if (!STATE.activeTarget || !STATE.activeCol) return new Set();
  const t = TGT_MAP[STATE.activeTarget];
  if (!t || !t.traces) return new Set();
  const trace = t.traces[STATE.activeCol];
  if (!trace) return new Set();
  return new Set((trace.sources || []).map(p => p.src_id).filter(Boolean));
}

function updateSrcPanelUI() {
  const traceSrcs = traceContribSrcIds();  // auto-expand DFs that feed the selected column
  DATA.sources.forEach(src => {
    const hdr  = document.getElementById('srchdr_'  + src.id);
    const cols = document.getElementById('srccols_' + src.id);
    if (!hdr || !cols) return;
    const selected  = STATE.activeSrcIds.has(src.id);
    const traceOpen = traceSrcs.has(src.id);
    hdr.classList.toggle('selected',  selected);
    hdr.classList.toggle('expanded',  selected || traceOpen);
    cols.classList.toggle('open',     selected || traceOpen);
  });
}

// ── source column tracking ─────────────────────────────────────────────────
// Multi-select: click to add, click again to remove. Columns from any source DF can coexist.
function selectSrcCol(srcId, col) {
  const key = srcId + '|' + col;
  if (STATE.activeSrcCols.has(key)) {
    STATE.activeSrcCols.delete(key);
  } else {
    STATE.activeSrcCols.set(key, {srcId, col});
    STATE.activeSrcIds.add(srcId);   // ensure the DF accordion stays open
    STATE.activeCol = null;          // clear target-column mode
  }
  render();
}

// ── target selection ───────────────────────────────────────────────────────
function selectTarget(tid) {
  if (STATE.activeTarget === tid) {
    // Deselect: close the detail view, hide columns panel
    STATE.activeTarget = null;
    STATE.activeCol    = null;
    document.getElementById('cols-panel').style.cssText = 'display:none';
    document.getElementById('col-search').value = '';
  } else {
    STATE.activeTarget = tid;
    STATE.activeCol    = null;
    document.getElementById('cols-panel').style.cssText = 'display:flex;flex-direction:column';
    document.getElementById('col-search').value = '';
  }
  render();
}

function selectColumn(col) {
  if (STATE.activeCol === col) {
    STATE.activeCol = null;
  } else {
    STATE.activeCol = col;
    // Do NOT clear activeSrcCols — keep source column context so chips stay highlighted
    // alongside the trace. The trace auto-expands contributing source DFs independently.
  }
  render();
}
function filterCols(q) { renderColPanel(q); }

// ── main render dispatcher ─────────────────────────────────────────────────
function render() {
  renderSourcePanelState();
  renderTargetList();
  renderColPanel(document.getElementById('col-search').value);
  renderMain();
}

function renderSourcePanelState() {
  updateSrcPanelUI();
  document.querySelectorAll('.col-chip').forEach(el => {
    const key = el.dataset.srcId + '|' + el.dataset.col;
    el.classList.toggle('tracked', STATE.activeSrcCols.has(key));
    el.classList.remove('trace-source');
  });
}

function renderTargetList() {
  const infl      = currentInfluence();
  const isColMode = STATE.activeSrcCols.size > 0;
  document.querySelectorAll('.target-item').forEach(el => {
    const tid   = el.dataset.id;
    const badge = el.querySelector('.t-influence-badge');
    const t     = TGT_MAP[tid];
    el.classList.toggle('active', tid === STATE.activeTarget);

    // highlight / dim the target item
    if (infl === null) {
      el.classList.remove('dim', 'influenced');
    } else if (tid in infl) {
      el.classList.remove('dim'); el.classList.add('influenced');
    } else {
      el.classList.remove('influenced'); el.classList.add('dim');
    }

    // Always render ALL source DF chips for this target.
    // sel = currently selected (bright blue), unsel = not selected (dimmed).
    // This never hides or removes a source — the user always sees what feeds the target.
    const srcChips = (t ? t.source_dfs || [] : []).map(s => {
      const cls  = STATE.activeSrcIds.has(s.id) ? 'sel' : 'unsel';
      const name = esc(s.name || s.id.slice(0, 6));
      return `<span class="t-src-chip ${cls}">${name}</span>`;
    }).join('');

    // In column mode: also show how many target cols the selected source cols influence
    let colsBadge = '';
    if (isColMode && infl && tid in infl) {
      const n = (infl[tid] || []).length;
      if (n > 0) colsBadge = `<span class="t-cols-badge">${n} col${n !== 1 ? 's' : ''}</span>`;
    }

    badge.innerHTML = srcChips + colsBadge;
  });
}

function renderColPanel(filter) {
  if (!STATE.activeTarget) return;
  const t    = TGT_MAP[STATE.activeTarget];
  const list = document.getElementById('col-list');
  list.innerHTML = '';
  if (!t) return;
  const infl = currentInfluence();
  // Only highlight columns when this target is actually in the influence map
  const targetInInfl = infl !== null && STATE.activeTarget in infl;
  const influenced = targetInInfl ? new Set(infl[STATE.activeTarget] || []) : new Set();
  t.columns
    .filter(c => !filter || c.toLowerCase().includes(filter.toLowerCase()))
    .forEach(col => {
      const el = document.createElement('div');
      let cls = 'col-item';
      if (col === STATE.activeCol)       cls += ' active';
      else if (influenced.has(col))      cls += ' influenced';
      el.className = cls; el.textContent = col; el.title = col;
      el.onclick = () => selectColumn(col);
      list.appendChild(el);
    });
}

function renderMain() {
  if (!STATE.activeTarget) {
    document.getElementById('main').innerHTML =
      '<p class="empty-msg">Select a source column to track its influence, or a target to inspect it.</p>';
    return;
  }
  STATE.activeCol ? renderTrace() : renderTargetOverview();
}

// ── per-source breakdown helper ────────────────────────────────────────────
// Groups every target column under the source DF(s) it comes from.
// A column appears under EACH source DF that contributes to it (multi-source is fine).
// Columns with no column-level attribution (count(*) etc.) go in an "Aggregated" section.
function buildSrcSections(t, influenced, chip) {
  const srcDFs = t.source_dfs || [];
  const attribToAny = new Set();

  const sections = srcDFs.map(src => {
    const cls   = STATE.activeSrcIds.has(src.id) ? 'sel' : 'unsel';
    const sname = src.name || src.id.slice(0, 6);
    const fqn   = src.caller && src.caller.fqn ? src.caller.fqn : '';

    // Columns that have at least one source attribution from this source DF
    const attrCols = t.columns.filter(col => {
      const trace = (t.traces || {})[col];
      return trace && (trace.sources || []).some(p => p.src_id === src.id);
    });
    attrCols.forEach(c => attribToAny.add(c));
    if (!attrCols.length) return '';

    const bySrcRole = {created: [], modified: [], passthrough: []};
    attrCols.forEach(col => {
      const role = (t.col_roles || {})[col] || 'passthrough';
      (bySrcRole[role] = bySrcRole[role] || []).push(col);
    });

    const roleGroups = ['created', 'modified', 'passthrough'].map(role => {
      const cols = bySrcRole[role] || [];
      if (!cols.length) return '';
      const label = {created: 'Created', modified: 'Modified', passthrough: 'Passthrough'}[role];
      const rows = cols.map(col => {
        // Which source columns from THIS source DF feed into col?
        const trace = (t.traces || {})[col] || {};
        const srcColChips = (trace.sources || [])
          .filter(p => p.src_id === src.id)
          .map(p => `<span class="src-attr-chip">${esc(p.col)}</span>`)
          .join('');
        const attr = srcColChips
          ? `<span class="src-attr-arrow">\u2190</span>${srcColChips}`
          : '';
        return `<div class="src-col-row-item">${chip(col, role)}${attr}</div>`;
      }).join('');
      return `<div class="src-col-group"><div class="src-col-group-label">${label}</div>${rows}</div>`;
    }).join('');

    return `<div class="src-section">
      <div class="src-section-hdr">
        <span class="src-section-name">${esc(sname)}</span>
        ${fqn ? `<span class="src-section-fqn">${esc(fqn)}</span>` : ''}
      </div>
      ${roleGroups}
    </div>`;
  }).join('');

  // Columns with no column-level attribution (e.g. count(*)) go in a separate section
  const unattrib = t.columns.filter(c => !attribToAny.has(c));
  const aggSection = unattrib.length ? `<div class="src-section">
    <div class="src-section-hdr">
      <span class="src-section-fqn">\u2014 Whole-dataset aggregates (no single column input)</span>
    </div>
    <div class="src-col-group">
      ${unattrib.map(col => {
        const role = (t.col_roles || {})[col] || 'created';
        return `<div class="src-col-row-item">${chip(col, role)}<span class="no-attr-note">e.g. count(*)</span></div>`;
      }).join('')}
    </div>
  </div>` : '';

  return sections + aggSection;
}

// ── target overview ────────────────────────────────────────────────────────
function renderTargetOverview() {
  const t = TGT_MAP[STATE.activeTarget];
  if (!t) return;

  const infl = currentInfluence();
  // Only highlight chips when this target is actually in the influence map.
  const targetInInfl = infl !== null && STATE.activeTarget in infl;
  const influenced = targetInInfl ? new Set(infl[STATE.activeTarget] || []) : null;

  // Count by role for stats bar
  const byRole = { created: [], modified: [], passthrough: [] };
  t.columns.forEach(col => {
    const role = (t.col_roles || {})[col] || 'passthrough';
    (byRole[role] = byRole[role] || []).push(col);
  });

  const chip = (col, role) => {
    return `<span class="t-ov-chip ${role}" onclick="selectColumn('${esc(col)}')">${esc(col)}</span>`;
  };

  // Per-source-column breakdown: each selected source column gets its own row
  // showing exactly which target columns it influences (or "no influence").
  let inflSummaryHTML = '';
  const isColMode = STATE.activeSrcCols.size > 0;
  const isDFMode  = !isColMode && STATE.activeSrcIds.size > 0;

  if (isColMode) {
    const rows = [...STATE.activeSrcCols.values()].map(({srcId, col}) => {
      const src     = SRC_MAP[srcId] || {};
      const sname   = src.name || '';
      const colInfl = ((src.influence || {})[col] || {})[STATE.activeTarget] || [];
      const chips   = colInfl.length
        ? colInfl.map(c =>
            `<span class="infl-col-chip" onclick="selectColumn('${esc(c)}')">${esc(c)}</span>`
          ).join('')
        : `<span class="no-infl-note">no influence in this target</span>`;
      return `<div class="src-col-attr-row">
        <div class="src-col-attr-lhs">
          <span class="src-col-name">${esc(col)}</span>
          ${sname ? `<span class="src-col-df-name">\u2190 ${esc(sname)}</span>` : ''}
          <span class="src-col-arrow">\u2192</span>
        </div>
        <div class="src-col-tgt-chips">${chips}</div>
      </div>`;
    }).join('');
    inflSummaryHTML = `<div class="infl-summary">${rows}</div>`;
  } else if (isDFMode && infl !== null && !(STATE.activeTarget in infl)) {
    // Target not fed by the selected source DFs
    const selNames = Array.from(STATE.activeSrcIds)
      .map(sid => (SRC_MAP[sid] || {}).name || sid.slice(0, 6))
      .join(', ');
    inflSummaryHTML = `<div class="not-infl-banner">
      \u26a0\ufe0f This target is <b>not fed by</b> the selected source(s): <b>${esc(selNames)}</b>.
      The column colors below reflect each column\u2019s transformation role (created / passthrough), not source influence.
    </div>`;
  }

  const name = targetName(t);
  const loc  = t.caller ? t.caller.file_short + ':' + t.caller.line : '';

  document.getElementById('main').innerHTML = `
    <div class="t-overview">
      <div class="t-ov-header">
        <div style="min-width:0;flex:1">
          <div class="t-ov-title">${esc(name)}</div>
          <div class="t-ov-sub">${esc(loc)}</div>
        </div>
        <div class="dl-bar">
          <button class="dl-btn" onclick="dlTargetJSON('${esc(STATE.activeTarget)}')">&#8595;&nbsp;JSON</button>
          <button class="dl-btn" onclick="dlTargetHTML('${esc(STATE.activeTarget)}')">&#8595;&nbsp;HTML</button>
        </div>
      </div>
      <div class="t-ov-stats">
        <span class="t-ov-stat"><b>${t.columns.length}</b> columns</span>
        ${(byRole.created || []).length   ? `<span class="t-ov-stat cr"><b>${byRole.created.length}</b> created</span>` : ''}
        ${(byRole.modified || []).length  ? `<span class="t-ov-stat mo"><b>${byRole.modified.length}</b> modified</span>` : ''}
        ${(byRole.passthrough || []).length ? `<span class="t-ov-stat"><b>${byRole.passthrough.length}</b> passthrough</span>` : ''}
      </div>
      ${inflSummaryHTML}
      <div class="t-ov-section" style="margin-top:12px">Source DataFrames</div>
      <p class="t-ov-desc">Data in this target originates from the following DataFrames. The identifier shown is the fully-qualified function and line where the DataFrame was first tracked.</p>
      <div class="src-summary-list">
        ${(t.source_dfs || []).map(s => {
          const name = s.name || s.id.slice(0, 6);
          const fqn  = s.caller ? (s.caller.fqn || (s.caller.file_short + ':' + s.caller.line)) : '';
          return `<div class="src-summary-entry">
            <span class="src-df-chip-static">${esc(name)}</span>
            ${fqn ? `<span class="src-df-chip-loc">${esc(fqn)}</span>` : ''}
          </div>`;
        }).join('')}
      </div>
      <div class="t-ov-section" style="margin-top:22px">Target DataFrame Columns</div>
      <p class="t-ov-desc">Every column in this target, grouped by which source DataFrame it originates from. Arrows (←) show the exact source column(s) each was derived from.</p>
      ${buildSrcSections(t, influenced, chip)}
    </div>`;
}

// ── column trace ───────────────────────────────────────────────────────────
function renderTrace() {
  const t     = TGT_MAP[STATE.activeTarget];
  const trace = t && t.traces ? t.traces[STATE.activeCol] : null;
  if (!trace) {
    document.getElementById('main').innerHTML =
      '<p class="empty-msg">No trace data for this column</p>';
    return;
  }

  const steps   = [...(trace.steps || [])].reverse();
  const groups  = groupByContext(steps);
  const srcPairs = trace.sources || [];  // [{src_id, col, src_name}]

  // Highlight source chips that directly contributed to this column
  document.querySelectorAll('.col-chip').forEach(el => {
    const contributed = srcPairs.some(p =>
      (p.src_id ? p.src_id === el.dataset.srcId : true) && p.col === el.dataset.col
    );
    el.classList.toggle('trace-source', contributed);
  });

  const srcChips = srcPairs.map(p => {
    const sid   = p.src_id || COL_TO_SRC_ID[p.col] || '';
    const sname = p.src_name || (SRC_MAP[sid] || {}).name || '';
    const label = sname ? p.col + ' \u2190 ' + sname : p.col;
    return `<span class="src-ref-chip" onclick="selectSrcCol('${esc(sid)}','${esc(p.col)}')" title="Derived from ${esc(sname ? sname + '.' + p.col : p.col)}">${esc(label)}</span>`;
  }).join('');

  // Fallback attribution when no column-level sources (e.g. count(*), count(1))
  // Still show which source DF(s) the column was created from
  const hasCreated = steps.some(s => s.role === 'created');
  let attributionHTML = '';
  if (srcChips) {
    attributionHTML = '<div class="src-refs"><span class="src-refs-label">derived from</span>' + srcChips + '</div>';
  } else if (hasCreated && t.source_dfs && t.source_dfs.length) {
    const dfChips = t.source_dfs.map(s => {
      const sname = s.name || (s.id || '').slice(0, 8);
      return `<span class="src-ref-chip" onclick="toggleSrcSelect('${esc(s.id)}')">${esc(sname)}</span>`;
    }).join('');
    attributionHTML = `<div class="src-refs"><span class="src-refs-label">created from</span>${dfChips}<span style="color:#64748b;font-size:.62rem;margin-left:6px">(whole-dataset aggregate \u2014 no single column input)</span></div>`;
  }

  const timelineHTML = groups.map((g, i) => renderGroup(g, i)).join('');

  document.getElementById('main').innerHTML = `
    <div class="trace-header">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:14px">
        <div style="min-width:0;flex:1">
          <button class="back-btn" onclick="selectColumn('${esc(STATE.activeCol)}')">&#8592; ${esc(t ? targetName(t) : STATE.activeTarget)}</button>
          <div class="trace-col-name">${esc(STATE.activeCol)}</div>
        </div>
        <div class="dl-bar">
          <button class="dl-btn" onclick="dlColJSON('${esc(STATE.activeTarget)}','${esc(STATE.activeCol)}')">&#8595;&nbsp;JSON</button>
          <button class="dl-btn" onclick="dlColHTML('${esc(STATE.activeTarget)}','${esc(STATE.activeCol)}')">&#8595;&nbsp;HTML</button>
        </div>
      </div>
      ${attributionHTML}
    </div>
    <div class="timeline">${timelineHTML || '<p class="empty-msg">No steps</p>'}</div>`;
}

// ── context grouping ───────────────────────────────────────────────────────
function groupByContext(steps) {
  const groups = []; let cur = null;
  steps.forEach(s => {
    if (s.role === 'source') {
      if (cur) { groups.push(cur); cur = null; }
      groups.push({ standalone: true, step: s });
      return;
    }
    const key = s.context_key || (s.caller ? s.caller.qualname : '__none__');
    if (!cur || cur.key !== key) {
      if (cur) groups.push(cur);
      cur = { key, caller: s.caller, steps: [], hasSig: false };
    }
    cur.steps.push(s);
    if (s.role === 'created' || s.role === 'modified') cur.hasSig = true;
  });
  if (cur) groups.push(cur);
  return groups;
}

function renderGroup(g, idx) {
  if (g.standalone) return stepHTML(g.step);

  const uid  = 'grp_' + idx + '_' + (g.key || '').replace(/[^a-z0-9]/gi,'_');
  const caller = g.caller;
  const label = caller ? (caller.qualname || caller.func) + '()' : 'pipeline';
  const loc   = caller ? caller.file_short + ':' + caller.line : '';

  const created     = g.steps.filter(s => s.role === 'created').length;
  const modified    = g.steps.filter(s => s.role === 'modified').length;
  const passthrough = g.steps.filter(s => s.role === 'passthrough').length;
  const parts = [];
  if (created)     parts.push(`<span style="color:#38bdf8">${created} created</span>`);
  if (modified)    parts.push(`<span style="color:#fb923c">${modified} modified</span>`);
  if (passthrough) parts.push(`<span style="color:#374151">${passthrough} pass</span>`);
  const rolesHTML = parts.join(' &middot; ');

  if (!(uid in GRP_COLLAPSED)) GRP_COLLAPSED[uid] = !g.hasSig;
  const collapsed = GRP_COLLAPSED[uid];

  const stepsHTML = g.steps.map(s => stepHTML(s)).join('');

  return `
    <div class="ctx-group">
      <div class="ctx-header" onclick="toggleGrp('${uid}')">
        <div class="ctx-info">
          <div class="ctx-name">${esc(label)}</div>
          ${loc ? '<div class="ctx-loc">'+esc(loc)+'</div>' : ''}
        </div>
        <div class="ctx-meta">
          <span class="ctx-roles">${rolesHTML}</span>
          <span class="ctx-toggle-icon" id="${uid}_icon">${collapsed ? '&#9660;' : '&#9650;'}</span>
        </div>
      </div>
      <div class="ctx-steps${collapsed ? ' collapsed' : ''}" id="${uid}">
        <div class="timeline">${stepsHTML}</div>
      </div>
    </div>`;
}

function toggleGrp(uid) {
  GRP_COLLAPSED[uid] = !GRP_COLLAPSED[uid];
  const el   = document.getElementById(uid);
  const icon = document.getElementById(uid + '_icon');
  if (el)   el.classList.toggle('collapsed', GRP_COLLAPSED[uid]);
  if (icon) icon.innerHTML = GRP_COLLAPSED[uid] ? '&#9660;' : '&#9650;';
}

function stepHTML(s) {
  const roleLabel = {source:'Source',created:'Created',modified:'Modified',passthrough:'Passthrough'}[s.role] || s.role;
  const op  = s.role === 'source' ? (s.name || 'source') : '.' + s.operation + '()';
  const args = (s.args || []).join(',\\n');
  const loc  = s.caller ? s.caller.file_short + ':' + s.caller.line : '';
  return `<div class="step ${s.role}">
    <div class="step-dot"></div>
    <div class="step-card">
      <div class="step-role">${esc(roleLabel)}</div>
      <div class="step-op">${esc(op)}</div>
      ${args ? '<div class="step-args">'+esc(args)+'</div>' : ''}
      ${loc  ? '<div class="step-loc"><span>'+esc(loc)+'</span></div>' : ''}
    </div>
  </div>`;
}

// ── downloads ──────────────────────────────────────────────────────────────
function dlTargetJSON(tid) {
  const t = TGT_MAP[tid];
  if (!t) return;
  const payload = {
    meta:    Object.assign({}, DATA.meta, { scope: 'target', target: targetName(t) }),
    sources: (t.source_dfs || []).map(sf => SRC_MAP[sf.id]).filter(Boolean),
    target:  t,
  };
  triggerDownload(JSON.stringify(payload, null, 2), 'application/json',
    safeName(targetName(t)) + '_lineage.json');
}

function dlTargetHTML(tid) {
  const t = TGT_MAP[tid];
  if (!t) return;
  const srcIds = new Set((t.source_dfs || []).map(s => s.id));
  const sources = DATA.sources.filter(s => srcIds.has(s.id)).map(s => {
    const influence = {};
    Object.entries(s.influence || {}).forEach(([col, tgts]) => {
      if (tgts[tid]) influence[col] = { [tid]: tgts[tid] };
    });
    return Object.assign({}, s, { influence });
  });
  const mini = {
    meta:    Object.assign({}, DATA.meta, { title: targetName(t), source_count: sources.length, target_count: 1 }),
    sources,
    targets: [t],
  };
  triggerDownload(injectData(mini), 'text/html',
    safeName(targetName(t)) + '_lineage.html');
}

function dlColJSON(tid, col) {
  const t = TGT_MAP[tid];
  const trace = t && t.traces ? t.traces[col] : null;
  if (!t || !trace) return;
  const payload = {
    meta:   Object.assign({}, DATA.meta, { scope: 'column', target: targetName(t), column: col }),
    target: { id: t.id, label: t.label, caller: t.caller },
    column: col,
    trace,
  };
  triggerDownload(JSON.stringify(payload, null, 2), 'application/json',
    safeName(targetName(t) + '_' + col) + '_trace.json');
}

function dlColHTML(tid, col) {
  const t = TGT_MAP[tid];
  if (!t) return;
  const trace = t.traces ? t.traces[col] : null;
  const tSlim = Object.assign({}, t, {
    columns:   [col],
    col_roles: { [col]: (t.col_roles || {})[col] || 'passthrough' },
    traces:    { [col]: trace },
  });
  const srcIds = new Set((t.source_dfs || []).map(s => s.id));
  const sources = DATA.sources.filter(s => srcIds.has(s.id)).map(s => {
    const influence = {};
    Object.entries(s.influence || {}).forEach(([sc, tgts]) => {
      if (tgts[tid] && tgts[tid].includes(col)) influence[sc] = { [tid]: [col] };
    });
    return Object.assign({}, s, { influence });
  });
  const mini = {
    meta:    Object.assign({}, DATA.meta, { title: targetName(t) + ' \\u00b7 ' + col, source_count: sources.length, target_count: 1 }),
    sources,
    targets: [tSlim],
  };
  triggerDownload(injectData(mini), 'text/html',
    safeName(targetName(t) + '_' + col) + '_trace.html');
}

function injectData(filteredData) {
  const fullHTML = document.documentElement.outerHTML;
  const S = '// >>SPARK_LINEAGE_DATA<<';
  const E = '// >>END_SPARK_LINEAGE_DATA<<';
  const si = fullHTML.indexOf(S);
  const ei = fullHTML.indexOf(E) + E.length;
  if (si < 0) return fullHTML;
  const newBlock = S + '\\nconst DATA = ' + JSON.stringify(filteredData) + ';\\n' + E;
  return fullHTML.slice(0, si) + newBlock + fullHTML.slice(ei);
}

function triggerDownload(content, mime, filename) {
  const blob = new Blob([content], { type: mime });
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement('a'), { href: url, download: filename });
  document.body.appendChild(a); a.click();
  setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 200);
}

function safeName(s) {
  return String(s).replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 60);
}

// ── build initial target list ──────────────────────────────────────────────
function buildTargets() {
  const wrap = document.getElementById('targets-wrap');
  wrap.innerHTML = '';
  DATA.targets.forEach(t => {
    const el = document.createElement('div');
    el.className = 'target-item';
    el.dataset.id = t.id;
    const sub = targetSub(t);
    el.innerHTML = `
      <div class="t-label">${esc(targetName(t))}</div>
      ${sub ? '<div class="t-sub">'+esc(sub)+'</div>' : ''}
      <div class="t-influence-badge"></div>`;
    el.onclick = () => selectTarget(t.id);
    wrap.appendChild(el);
  });
}

// ── resizable sidebar ─────────────────────────────────────────────────────
const drag = document.getElementById('drag');
const left = document.getElementById('left');
let dragging = false, startX = 0, startW = 0;
drag.addEventListener('mousedown', e => {
  dragging = true; startX = e.clientX; startW = left.offsetWidth;
  drag.classList.add('on');
  document.body.style.cssText = 'cursor:col-resize;user-select:none';
});
document.addEventListener('mousemove', e => {
  if (!dragging) return;
  left.style.width = Math.max(200, Math.min(460, startW + e.clientX - startX)) + 'px';
});
document.addEventListener('mouseup', () => {
  dragging = false; drag.classList.remove('on');
  document.body.style.cssText = '';
});

// ── bootstrap ─────────────────────────────────────────────────────────────
buildSourcePanel();
buildTargets();
</script>
</body>
</html>
"""
