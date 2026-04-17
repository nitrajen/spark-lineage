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
    source_node = _registry.get(source_id)
    try:
        source_cols = list(source_df.columns)
    except Exception:
        source_cols = list(source_node.output_cols) if source_node else []

    leaf_ids = _registry.get_leaf_descendants(source_id)

    targets: list[dict] = []
    traces:  dict       = {}
    all_source_ids: dict[str, dict] = {}

    for tid in leaf_ids:
        node = _registry.get(tid)
        if node is None:
            continue

        ancestor_nodes = _get_all_ancestor_nodes(tid)
        col_src_map    = _build_col_source_map(ancestor_nodes)

        # Which source DFs contributed to this specific target?
        target_source_dfs: list[dict] = []
        for an in ancestor_nodes:
            if an.operation is None:
                sf = {"id": an.id, "name": an.name, "columns": list(an.output_cols)}
                target_source_dfs.append(sf)
                if an.id not in all_source_ids:
                    all_source_ids[an.id] = {
                        "id":      an.id,
                        "name":    an.name,
                        "columns": list(an.output_cols),
                        "primary": an.id == source_id,
                    }

        label = _node_label(node, tid)
        targets.append({
            "id":         tid,
            "label":      label,
            "name":       node.name,
            "columns":    list(node.output_cols),
            "caller":     _caller_dict(node.caller),
            "source_dfs": target_source_dfs,
        })

        traces[tid] = {}
        for col in node.output_cols:
            traces[tid][col] = _build_col_trace(col, ancestor_nodes, col_src_map)

    # Order: primary source first, then alphabetically by name
    all_sources = sorted(
        all_source_ids.values(),
        key=lambda s: (0 if s["primary"] else 1, s["name"] or "")
    )

    # Influence maps for ALL source DataFrames (not just the primary)
    all_source_influence: dict[str, dict] = {}
    for src in all_sources:
        all_source_influence[src["id"]] = _compute_source_influence(
            src["columns"], targets, traces
        )

    return {
        "meta": {
            "title":        title,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "target_count": len(targets),
            "source_count": len(all_sources),
        },
        "source": {
            "id":      source_id,
            "name":    source_node.name if source_node else None,
            "columns": source_cols,
        },
        "all_sources":          all_sources,
        "all_source_influence": all_source_influence,
        # backward-compat key for the primary source
        "source_influence": all_source_influence.get(source_id, {}),
        "targets": targets,
        "traces":  traces,
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
    return {
        "module":     caller.module,
        "klass":      caller.klass,
        "func":       caller.function,
        "qualname":   caller.qualname,
        "file":       caller.file,
        "file_short": caller.file.split("/")[-1],
        "line":       caller.line,
        "context_key": caller.qualname,   # stable grouping key for JS
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
    Build {col_name: set_of_ultimate_source_cols} from ALL ancestor nodes.
    Seeds from every root node (primary source + any joined DataFrames).
    """
    csm: dict[str, set] = {}
    for node in ancestor_nodes:
        if node.operation is None:
            for col in node.output_cols:
                csm[col] = {col}

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
    sources: list[str] = sorted(col_src_map.get(col, set())) if col_src_map else []

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


def _compute_source_influence(source_cols: list, targets: list, traces: dict) -> dict:
    """
    Returns {source_col: {target_id: [influenced_target_cols]}}.
    """
    influence: dict = {}
    for sc in source_cols:
        influence[sc] = {}
        for t in targets:
            tid = t["id"]
            influenced = []
            for col in t["columns"]:
                trace = (traces.get(tid) or {}).get(col)
                if trace is None:
                    continue
                steps   = trace.get("steps", [])
                sources = trace.get("sources", [])
                if sc in sources:
                    influenced.append(col)
                elif col == sc and steps and steps[0].get("role") == "source":
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

/* source block */
#src-block{border-bottom:1px solid #1a2235;flex-shrink:0;overflow-y:auto;max-height:240px}
.src-entry{padding:8px 14px;border-bottom:1px solid #111827}
.src-entry:last-child{border-bottom:none}
.src-df-name{font-size:.75rem;font-weight:700;color:#7dd3fc;margin-bottom:5px;cursor:pointer;user-select:none;display:flex;align-items:center;gap:6px}
.src-df-name.secondary{color:#4b5563;font-size:.7rem}
.src-df-name.active{color:#38bdf8}
.src-df-name .src-dot{width:7px;height:7px;border-radius:50%;background:currentColor;flex-shrink:0}
.src-cols{display:flex;flex-wrap:wrap;gap:3px}
.col-chip{font-size:.6rem;padding:1px 7px;border-radius:9px;background:#1a2235;color:#64748b;border:1px solid #21283a;white-space:nowrap;cursor:pointer;transition:all .15s}
.col-chip:hover{background:#1e2d40;color:#94a3b8;border-color:#2d3f55}
.col-chip.selected{background:#1e3a5f;color:#7dd3fc;border-color:#38bdf8}
.col-chip.trace-source{background:#052e16;color:#34d399;border-color:#166534}

/* targets list */
#targets-wrap{flex:1;overflow-y:auto;padding:4px 0}
.target-item{padding:7px 14px;cursor:pointer;border-left:2px solid transparent;transition:all .1s}
.target-item:hover{background:#1a2235}
.target-item.active{background:#1e2d40;border-left-color:#38bdf8}
.t-label{font-size:.75rem;color:#94a3b8;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.t-sub{font-size:.62rem;color:#374151;margin-top:1px}
.target-item.dim{opacity:.22}
.target-item.influenced .t-label{color:#7dd3fc}
.t-influence-badge{display:none;font-size:.58rem;color:#38bdf8;background:#0c2340;border:1px solid #1d4ed8;border-radius:9px;padding:1px 6px;margin-top:3px}
.target-item.influenced .t-influence-badge{display:inline-block}

/* columns panel */
#cols-panel{border-top:1px solid #1a2235;max-height:44%;display:flex;flex-direction:column;flex-shrink:0}
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
.t-ov-title{font-size:1.15rem;font-weight:700;color:#e2e8f0;margin-bottom:3px;word-break:break-all}
.t-ov-sub{font-size:.72rem;color:#4b5563}
.t-ov-stats{display:flex;flex-wrap:wrap;gap:0;margin-bottom:16px}
.t-ov-stat{display:inline-flex;align-items:center;gap:6px;margin-right:16px;font-size:.68rem;color:#4b5563}
.t-ov-stat b{color:#94a3b8}
.t-ov-stat.cr b{color:#38bdf8}.t-ov-stat.mo b{color:#fb923c}
.t-ov-section{font-size:.6rem;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;margin-top:18px}
.t-ov-section.highlight{color:#38bdf8;text-transform:none;font-size:.65rem;font-weight:400;letter-spacing:0}
.t-ov-cols{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px}
.t-ov-chip{font-size:.65rem;padding:2px 9px;border-radius:9px;white-space:nowrap;cursor:pointer;transition:opacity .15s}
.t-ov-chip:hover{opacity:.75}
.t-ov-chip.created{background:#0c2340;color:#38bdf8;border:1px solid #1d4ed8}
.t-ov-chip.modified{background:#431407;color:#fb923c;border:1px solid #c2410c}
.t-ov-chip.passthrough{background:#1a2235;color:#4b5563;border:1px solid #21283a}
.t-ov-chip.hl{box-shadow:0 0 0 2px #38bdf8}
.src-df-chips{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:4px}
.src-df-chip{display:inline-block;font-size:.65rem;color:#7dd3fc;background:#0c2340;padding:2px 10px;border-radius:9px;border:1px solid #1d4ed8;cursor:pointer}
.src-df-chip:hover{opacity:.8}

/* download bar */
.dl-bar{display:flex;gap:6px;flex-shrink:0}
.dl-btn{font-size:.62rem;padding:3px 10px;border-radius:4px;background:#161b27;color:#4b5563;border:1px solid #1e2d40;cursor:pointer;transition:all .15s;white-space:nowrap}
.dl-btn:hover{background:#1e2d40;color:#94a3b8;border-color:#2d3f55}

/* trace */
.trace-header{margin-bottom:20px}
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
  <div id="main"><p class="empty-msg">Select a target to explore its lineage</p></div>
</div>
<script>
// >>SPARK_LINEAGE_DATA<<
const DATA = __DATA__;
// >>END_SPARK_LINEAGE_DATA<<

// ── application state ──────────────────────────────────────────────────────
const STATE = {
  srcId:  null,   // active source DataFrame id
  srcCol: null,   // active source column name
  tid:    null,   // active target id
  col:    null,   // active target column
};

// per-group collapse state: context_key → bool (true = collapsed)
const GRP_COLLAPSED = {};

// lookup: source col name → source DF id (first match)
const COL_TO_SRC_ID = {};
(DATA.all_sources || []).forEach(src => {
  (src.columns || []).forEach(c => { if (!COL_TO_SRC_ID[c]) COL_TO_SRC_ID[c] = src.id; });
});

// ── helpers ────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? '')
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function getInfluence(srcId, col) {
  return ((DATA.all_source_influence || {})[srcId] || {})[col] || {};
}

function currentInfluence() {
  if (!STATE.srcId || !STATE.srcCol) return null;
  const infl = getInfluence(STATE.srcId, STATE.srcCol);
  return Object.keys(infl).length ? infl : {};
}

function targetName(t) {
  if (t.caller && t.caller.qualname) return t.caller.qualname + '()';
  if (t.name) return t.name;
  return t.label;
}
function targetSub(t) {
  return t.caller ? t.caller.file_short + ':' + t.caller.line : '';
}
function srcName(src) {
  return src.name || src.id.slice(0, 8);
}

// ── init header ───────────────────────────────────────────────────────────
document.getElementById('pg-title').textContent = DATA.meta.title;
document.getElementById('pg-sub').textContent =
  DATA.targets.length + ' target' + (DATA.targets.length !== 1 ? 's' : '');
document.getElementById('src-count').textContent =
  '(' + (DATA.all_sources ? DATA.all_sources.length : 1) + ')';
document.getElementById('t-count').textContent =
  '(' + DATA.targets.length + ')';

// ── build source panel ─────────────────────────────────────────────────────
function buildSourcePanel() {
  const block = document.getElementById('src-block');
  block.innerHTML = '';
  (DATA.all_sources || [DATA.source]).forEach(src => {
    const entry = document.createElement('div');
    entry.className = 'src-entry';

    const nameEl = document.createElement('div');
    nameEl.className = 'src-df-name' + (src.primary === false ? ' secondary' : '');
    nameEl.id = 'sdf_' + src.id;
    nameEl.innerHTML = '<span class="src-dot"></span>' + esc(srcName(src));
    nameEl.title = src.primary ? 'Primary source' : 'Contributing source (joined)';
    nameEl.onclick = () => selectSrcDF(src.id);
    entry.appendChild(nameEl);

    const colsEl = document.createElement('div');
    colsEl.className = 'src-cols';
    (src.columns || []).forEach(c => {
      const chip = document.createElement('span');
      chip.className = 'col-chip';
      chip.textContent = c;
      chip.title = 'Track influence of "' + c + '" through pipeline';
      chip.dataset.srcId = src.id;
      chip.dataset.col   = c;
      chip.onclick = e => { e.stopPropagation(); selectSrcCol(src.id, c); };
      colsEl.appendChild(chip);
    });
    entry.appendChild(colsEl);
    block.appendChild(entry);
  });
}

// ── source selection ───────────────────────────────────────────────────────
function selectSrcDF(srcId) {
  if (STATE.srcId === srcId && STATE.srcCol === null) {
    STATE.srcId = null;
  } else {
    STATE.srcId  = srcId;
    STATE.srcCol = null;
  }
  render();
}

function selectSrcCol(srcId, col) {
  if (STATE.srcId === srcId && STATE.srcCol === col) {
    STATE.srcId  = null;
    STATE.srcCol = null;
  } else {
    STATE.srcId  = srcId;
    STATE.srcCol = col;
  }
  render();
}

// ── target selection ───────────────────────────────────────────────────────
function selectTarget(tid) {
  STATE.tid = tid;
  STATE.col = null;
  document.getElementById('cols-panel').style.cssText = 'display:flex;flex-direction:column';
  document.getElementById('col-search').value = '';
  render();
}

function selectColumn(col) {
  STATE.col = col;
  render();
}
function filterCols(q) { renderColPanel(q); }

// ── main render dispatcher ─────────────────────────────────────────────────
function render() {
  renderSourcePanel();
  renderTargetList();
  renderColPanel(document.getElementById('col-search').value);
  renderMain();
}

function renderSourcePanel() {
  document.querySelectorAll('.src-df-name').forEach(el => {
    const sid = el.id.replace('sdf_', '');
    el.classList.toggle('active', sid === STATE.srcId);
  });
  document.querySelectorAll('.col-chip').forEach(el => {
    el.classList.toggle('selected',
      el.dataset.srcId === STATE.srcId && el.dataset.col === STATE.srcCol);
    el.classList.remove('trace-source');  // cleared here, set by renderTrace
  });
}

function renderTargetList() {
  const infl = currentInfluence();
  document.querySelectorAll('.target-item').forEach(el => {
    const tid   = el.dataset.id;
    const badge = el.querySelector('.t-influence-badge');
    el.classList.toggle('active', tid === STATE.tid);
    if (infl === null) {
      el.classList.remove('dim', 'influenced');
      badge.textContent = '';
    } else {
      const cols = infl[tid];
      if (cols && cols.length) {
        el.classList.remove('dim'); el.classList.add('influenced');
        badge.textContent = cols.length + ' col' + (cols.length !== 1 ? 's' : '') + ' influenced';
      } else {
        el.classList.remove('influenced'); el.classList.add('dim');
        badge.textContent = '';
      }
    }
  });
}

function renderColPanel(filter) {
  if (!STATE.tid) return;
  const t    = DATA.targets.find(x => x.id === STATE.tid);
  const list = document.getElementById('col-list');
  list.innerHTML = '';
  if (!t) return;
  const infl = currentInfluence();
  const influenced = infl ? new Set(infl[STATE.tid] || []) : new Set();
  t.columns
    .filter(c => !filter || c.toLowerCase().includes(filter.toLowerCase()))
    .forEach(col => {
      const el = document.createElement('div');
      let cls = 'col-item';
      if (col === STATE.col)            cls += ' active';
      else if (influenced.has(col))     cls += ' influenced';
      el.className = cls; el.textContent = col; el.title = col;
      el.onclick = () => selectColumn(col);
      list.appendChild(el);
    });
}

function renderMain() {
  if (!STATE.tid) {
    document.getElementById('main').innerHTML =
      '<p class="empty-msg">Select a target to explore its lineage</p>';
    return;
  }
  if (!STATE.col) {
    renderTargetOverview();
  } else {
    renderTrace();
  }
}

// ── target overview ────────────────────────────────────────────────────────
function renderTargetOverview() {
  const t      = DATA.targets.find(x => x.id === STATE.tid);
  const traces = DATA.traces[STATE.tid] || {};
  if (!t) return;

  // Classify columns by role
  const byRole = { created: [], modified: [], passthrough: [] };
  t.columns.forEach(col => {
    const trace = traces[col];
    if (!trace) { byRole.passthrough.push(col); return; }
    const steps = trace.steps || [];
    if (steps.some(s => s.role === 'created'))      byRole.created.push(col);
    else if (steps.some(s => s.role === 'modified')) byRole.modified.push(col);
    else                                              byRole.passthrough.push(col);
  });

  const infl = currentInfluence();
  const influenced = infl ? new Set(infl[STATE.tid] || []) : null;

  const chip = (col, role) => {
    const hl = influenced && influenced.has(col) ? ' hl' : '';
    return `<span class="t-ov-chip ${role}${hl}" onclick="selectColumn('${esc(col)}')">${esc(col)}</span>`;
  };

  // Contributing source DataFrames
  const srcDFs = t.source_dfs || [];
  const srcDFsHTML = srcDFs.length
    ? `<div class="t-ov-section">Contributing Sources</div>
       <div class="src-df-chips">${srcDFs.map(s =>
         `<span class="src-df-chip" onclick="selectSrcDF('${s.id}')">${esc(s.name || s.id.slice(0,8))}</span>`
       ).join('')}</div>`
    : '';

  const inflNote = influenced
    ? `<div class="t-ov-section highlight">&#9650; Columns highlighted by source col &ldquo;${esc(STATE.srcCol)}&rdquo;</div>`
    : '';

  const name = targetName(t);
  const loc  = t.caller ? t.caller.file_short + ':' + t.caller.line : '';

  document.getElementById('main').innerHTML = `
    <div class="t-overview">
      <div class="t-ov-header">
        <div style="min-width:0">
          <div class="t-ov-title">${esc(name)}</div>
          <div class="t-ov-sub">${esc(loc)}</div>
        </div>
        <div class="dl-bar">
          <button class="dl-btn" onclick="dlTargetJSON('${esc(STATE.tid)}')">&#8595;&nbsp;JSON</button>
          <button class="dl-btn" onclick="dlTargetHTML('${esc(STATE.tid)}')">&#8595;&nbsp;HTML</button>
        </div>
      </div>
      <div class="t-ov-stats">
        <span class="t-ov-stat"><b>${t.columns.length}</b> columns</span>
        ${byRole.created.length   ? `<span class="t-ov-stat cr"><b>${byRole.created.length}</b> created</span>` : ''}
        ${byRole.modified.length  ? `<span class="t-ov-stat mo"><b>${byRole.modified.length}</b> modified</span>` : ''}
        ${byRole.passthrough.length ? `<span class="t-ov-stat"><b>${byRole.passthrough.length}</b> passthrough</span>` : ''}
      </div>
      ${srcDFsHTML}
      ${inflNote}
      ${byRole.created.length   ? '<div class="t-ov-section">Created</div><div class="t-ov-cols">'+byRole.created.map(c=>chip(c,'created')).join('')+'</div>' : ''}
      ${byRole.modified.length  ? '<div class="t-ov-section">Modified</div><div class="t-ov-cols">'+byRole.modified.map(c=>chip(c,'modified')).join('')+'</div>' : ''}
      ${byRole.passthrough.length ? '<div class="t-ov-section">Passthrough</div><div class="t-ov-cols">'+byRole.passthrough.map(c=>chip(c,'passthrough')).join('')+'</div>' : ''}
    </div>`;
}

// ── column trace ───────────────────────────────────────────────────────────
function renderTrace() {
  const t     = DATA.targets.find(x => x.id === STATE.tid);
  const trace = (DATA.traces[STATE.tid] || {})[STATE.col];
  if (!trace) {
    document.getElementById('main').innerHTML =
      '<p class="empty-msg">No trace data for this column</p>';
    return;
  }

  const steps   = [...(trace.steps || [])].reverse();
  const groups  = groupByContext(steps);
  const srcCols = trace.sources || [];

  // Highlight source chips that contributed to this column
  document.querySelectorAll('.col-chip').forEach(el => {
    el.classList.toggle('trace-source', srcCols.includes(el.dataset.col));
  });

  const srcChips = srcCols.map(s => {
    const sid = COL_TO_SRC_ID[s] || '';
    return `<span class="src-ref-chip" onclick="selectSrcCol('${esc(sid)}','${esc(s)}')" title="Track influence of ${esc(s)}">${esc(s)}</span>`;
  }).join('');

  const timelineHTML = groups.map((g, i) => renderGroup(g, i)).join('');

  document.getElementById('main').innerHTML = `
    <div class="trace-header">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:14px">
        <div>
          <div class="trace-col-name">${esc(STATE.col)}</div>
          <div class="trace-target">in <span>${esc(t ? targetName(t) : STATE.tid)}</span></div>
        </div>
        <div class="dl-bar">
          <button class="dl-btn" onclick="dlColJSON('${esc(STATE.tid)}','${esc(STATE.col)}')">&#8595;&nbsp;JSON</button>
          <button class="dl-btn" onclick="dlColHTML('${esc(STATE.tid)}','${esc(STATE.col)}')">&#8595;&nbsp;HTML</button>
        </div>
      </div>
      ${srcChips ? '<div class="src-refs"><span class="src-refs-label">derived from</span>'+srcChips+'</div>' : ''}
    </div>
    <div class="timeline">${timelineHTML || '<p class="empty-msg">No steps</p>'}</div>`;
}

// ── context grouping ───────────────────────────────────────────────────────
function groupByContext(steps) {
  const groups = []; let cur = null;
  steps.forEach(s => {
    // source role always gets its own standalone "group" (no header)
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
  const n = g.steps.length;

  const created     = g.steps.filter(s => s.role === 'created').length;
  const modified    = g.steps.filter(s => s.role === 'modified').length;
  const passthrough = g.steps.filter(s => s.role === 'passthrough').length;
  const parts = [];
  if (created)     parts.push(`<span style="color:#38bdf8">${created} created</span>`);
  if (modified)    parts.push(`<span style="color:#fb923c">${modified} modified</span>`);
  if (passthrough) parts.push(`<span style="color:#374151">${passthrough} pass</span>`);
  const rolesHTML = parts.join(' &middot; ');

  // Default collapsed only if purely passthrough
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
  const args = (s.args || []).join(',\n');
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
  const t = DATA.targets.find(x => x.id === tid);
  if (!t) return;
  const payload = {
    meta:       Object.assign({}, DATA.meta, { scope: 'target', target: targetName(t) }),
    target:     t,
    source_dfs: t.source_dfs || [],
    traces:     DATA.traces[tid] || {},
  };
  triggerDownload(JSON.stringify(payload, null, 2), 'application/json',
    safeName(targetName(t)) + '_lineage.json');
}

function dlTargetHTML(tid) {
  const t = DATA.targets.find(x => x.id === tid);
  if (!t) return;
  const mini = buildSubsetData([tid]);
  mini.meta = Object.assign({}, DATA.meta, { title: targetName(t), target_count: 1 });
  triggerDownload(injectData(mini), 'text/html',
    safeName(targetName(t)) + '_lineage.html');
}

function dlColJSON(tid, col) {
  const t     = DATA.targets.find(x => x.id === tid);
  const trace = (DATA.traces[tid] || {})[col];
  if (!t || !trace) return;
  const payload = {
    meta:   Object.assign({}, DATA.meta, { scope: 'column', target: targetName(t), column: col }),
    target: { id: t.id, label: t.label, caller: t.caller },
    column: col,
    trace:  trace,
  };
  triggerDownload(JSON.stringify(payload, null, 2), 'application/json',
    safeName(targetName(t) + '_' + col) + '_trace.json');
}

function dlColHTML(tid, col) {
  const t = DATA.targets.find(x => x.id === tid);
  if (!t) return;
  const tSlim = Object.assign({}, t, { columns: [col] });
  const mini = {
    meta:                 Object.assign({}, DATA.meta, { title: targetName(t) + ' \u00b7 ' + col, target_count: 1 }),
    source:               DATA.source,
    all_sources:          t.source_dfs || DATA.all_sources,
    all_source_influence: {},
    source_influence:     {},
    targets:              [tSlim],
    traces:               { [tid]: { [col]: (DATA.traces[tid] || {})[col] } },
  };
  triggerDownload(injectData(mini), 'text/html',
    safeName(targetName(t) + '_' + col) + '_trace.html');
}

function buildSubsetData(tids) {
  const targets = DATA.targets.filter(t => tids.includes(t.id));
  const traces  = {};
  const asi     = {};
  tids.forEach(tid => { traces[tid] = DATA.traces[tid] || {}; });
  (DATA.all_sources || []).forEach(src => {
    asi[src.id] = {};
    (src.columns || []).forEach(col => {
      const f = {};
      tids.forEach(tid => {
        const c = ((DATA.all_source_influence[src.id] || {})[col] || {})[tid];
        if (c) f[tid] = c;
      });
      if (Object.keys(f).length) asi[src.id][col] = f;
    });
  });
  return {
    meta:                 DATA.meta,
    source:               DATA.source,
    all_sources:          DATA.all_sources,
    all_source_influence: asi,
    source_influence:     {},
    targets:              targets,
    traces:               traces,
  };
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
