from __future__ import annotations
import json
from datetime import datetime
from typing import Optional
from .core.registry import _registry, LineageRegistry
from .core.node import LineageNode, CallerInfo


# ── public API ────────────────────────────────────────────────────────────────

def get_lineage(df) -> list[LineageNode]:
    """Backward walk: all ancestor nodes of df in topological order."""
    if not hasattr(df, "_dfi_id"):
        raise ValueError("DataFrame is not tracked. Call dfi.track_df(df) first.")
    visited, order = set(), []

    def walk(df_id: str):
        if df_id in visited:
            return
        visited.add(df_id)
        node = _registry.get(df_id)
        if node is None:
            return
        for pid in node.parent_ids:
            walk(pid)
        order.append(node)

    walk(df._dfi_id)
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
                f"#{id_to_step[p]}" + (f" [{_registry.get(p).name}]" if _registry.get(p) and _registry.get(p).name else "")
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
    Save a source-centric lineage report.

    source_df — the tracked root DataFrame (passed to dfi.track_df earlier).
    path      — output path prefix; writes <path>.json and <path>.html.
    name      — optional title shown in the report.

    The report shows:
      • Source: the tracked DataFrame, its identity and columns.
      • Targets: every leaf DataFrame derived from the source.
      • Column traces: for each target column, a vertical step-by-step trace
        from source through every intermediate transformation to that column.
    """
    if not hasattr(source_df, "_dfi_id"):
        raise ValueError("DataFrame is not tracked. Call dfi.track_df(source_df) first.")

    source_id = source_df._dfi_id
    title     = name or _registry.get(source_id).name or "Lineage Report"
    data      = _build_json(source_id, source_df, title)

    _write(path + ".json", json.dumps(data, indent=2, default=str))
    _write(path + ".html", _VIEWER_TEMPLATE.replace("__DATA__", json.dumps(data)))
    print(f"Report saved → {path}.json  |  {path}.html")


# ── JSON builder ──────────────────────────────────────────────────────────────

def _build_json(source_id: str, source_df, title: str) -> dict:
    source_node = _registry.get(source_id)
    try:
        source_cols = list(source_df.columns)
    except Exception:
        source_cols = list(source_node.output_cols) if source_node else []

    # Find all targets (leaf descendants)
    leaf_ids = _registry.get_leaf_descendants(source_id)

    targets = []
    for tid in leaf_ids:
        node = _registry.get(tid)
        if node is None:
            continue
        label = _node_label(node, tid)
        targets.append({
            "id":      tid,
            "label":   label,
            "name":    node.name,
            "columns": list(node.output_cols),
            "caller":  _caller_dict(node.caller),
        })

    # Precompute column traces for every (target, column) pair.
    # Uses the FULL ancestor tree of each target (all joined branches included),
    # not just the primary source→target path.
    traces = {}
    all_source_ids: dict[str, dict] = {}   # id → {name, columns} across all targets

    for t in targets:
        tid = t["id"]
        ancestor_nodes = _get_all_ancestor_nodes(tid)
        col_src_map    = _build_col_source_map(ancestor_nodes)
        traces[tid]    = {}
        for col in t["columns"]:
            traces[tid][col] = _build_col_trace(col, ancestor_nodes, col_src_map)

        # Collect every root found while walking this target's ancestors
        for node in ancestor_nodes:
            if node.operation is None and node.id not in all_source_ids:
                all_source_ids[node.id] = {
                    "id":      node.id,
                    "name":    node.name,
                    "columns": list(node.output_cols),
                    "primary": node.id == source_id,
                }

    # Order: primary source first, others alphabetically by name
    all_sources = sorted(
        all_source_ids.values(),
        key=lambda s: (0 if s["primary"] else 1, s["name"] or "")
    )

    source_influence = _compute_source_influence(source_cols, targets, traces)

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
        "all_sources":      all_sources,
        "targets":          targets,
        "traces":           traces,
        "source_influence": source_influence,
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
    }


def _get_all_ancestor_nodes(target_id: str) -> list[LineageNode]:
    """
    Return ALL ancestor nodes of target_id (including every joined branch),
    in topological order — roots first, target last.
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

    Seeds from every root node (any DataFrame that was tracked and contributed
    to this target — the primary source, joined DataFrames, etc.).
    Resolves multi-hop derivations:
        total → net_amount → {amount, discount}
    """
    # Seed: every root's columns map to themselves
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
                direct = set(col_refs.get(col, []))
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
    Build the vertical trace for a single column using ALL ancestor nodes.

    Any root node (operation=None) is labelled as SOURCE — this correctly
    handles joined DataFrames: a column from `customers` that passes through
    a join shows SOURCE at the bottom of its trace, not CREATED at join.
    """
    steps = []
    expr: str = col
    sources: list[str] = sorted(col_src_map.get(col, set())) if col_src_map else []

    for node in ancestor_nodes:
        if col not in (node.output_cols or []):
            continue

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
            elif node.column_refs:
                role = "passthrough"
            else:
                role = "passthrough"

        steps.append({
            "operation": node.operation or "source",
            "role":      role,
            "name":      node.name or "",
            "args":      node.args_repr or [],
            "caller":    _caller_dict(node.caller),
        })

    return {"expr": expr, "sources": sources, "steps": steps}


def _compute_source_influence(source_cols: list, targets: list, traces: dict) -> dict:
    """
    Returns {source_col: {target_id: [influenced_target_cols]}}.
    A target column is influenced by a source column if:
      - the source column appears in trace["sources"] (derived from it), OR
      - the column name matches the source column and starts with role "source"
        (i.e., it passes through unchanged).
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
                steps = trace.get("steps", [])
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
<title>Lineage Report</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d1117;color:#e2e8f0;height:100vh;display:flex;flex-direction:column;overflow:hidden;font-size:13px}

/* header */
#hdr{padding:10px 18px;background:#161b27;border-bottom:1px solid #21283a;display:flex;align-items:baseline;gap:12px;flex-shrink:0}
#hdr h1{font-size:.9rem;font-weight:700;color:#7dd3fc}
#hdr .sub{font-size:.7rem;color:#374151}

/* layout */
#wrap{display:flex;flex:1;overflow:hidden}

/* left panel */
#left{width:280px;min-width:180px;max-width:440px;background:#0f1623;border-right:1px solid #1a2235;display:flex;flex-direction:column;overflow:hidden;flex-shrink:0}
#drag{width:4px;cursor:col-resize;background:transparent;transition:background .12s;flex-shrink:0}
#drag:hover,#drag.on{background:#2d3f55}

/* section headers */
.sec{padding:7px 14px;font-size:.6rem;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.08em;background:#0d1117;border-bottom:1px solid #1a2235;flex-shrink:0}

/* source block */
#src-block{border-bottom:1px solid #1a2235;flex-shrink:0;overflow-y:auto;max-height:220px}
.src-entry{padding:8px 14px;border-bottom:1px solid #111827}
.src-entry:last-child{border-bottom:none}
.src-name{font-size:.78rem;font-weight:700;color:#7dd3fc;margin-bottom:5px}
.src-name.secondary{color:#475569;font-size:.72rem}
.src-cols{display:flex;flex-wrap:wrap;gap:3px}
.col-chip{font-size:.6rem;padding:1px 7px;border-radius:9px;background:#1a2235;color:#64748b;border:1px solid #21283a;white-space:nowrap;cursor:pointer;transition:all .15s}
.col-chip:hover{background:#1e2d40;color:#94a3b8;border-color:#2d3f55}
.col-chip.selected{background:#1e3a5f;color:#7dd3fc;border-color:#38bdf8}

/* targets list */
#targets-wrap{flex:1;overflow-y:auto;padding:4px 0}
.target-item{padding:7px 14px;cursor:pointer;border-left:2px solid transparent;transition:all .1s}
.target-item:hover{background:#1a2235}
.target-item.active{background:#1e2d40;border-left-color:#38bdf8}
.t-label{font-size:.75rem;color:#94a3b8;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.t-sub{font-size:.62rem;color:#374151;margin-top:1px}
.target-item.dim{opacity:.25}
.target-item.influenced .t-label{color:#7dd3fc}
.t-influence-badge{display:none;font-size:.58rem;color:#38bdf8;background:#0c2340;border:1px solid #1d4ed8;border-radius:9px;padding:1px 6px;margin-top:3px}
.target-item.influenced .t-influence-badge{display:inline-block}

/* columns panel (inside left, below targets) */
#cols-panel{border-top:1px solid #1a2235;max-height:45%;display:flex;flex-direction:column;flex-shrink:0}
#col-search-wrap{padding:6px 10px;border-bottom:1px solid #1a2235;flex-shrink:0}
#col-search{width:100%;background:#161b27;border:1px solid #21283a;color:#e2e8f0;border-radius:4px;padding:4px 8px;font-size:.68rem;outline:none}
#col-search:focus{border-color:#2d3f55}
#col-list{overflow-y:auto;flex:1}
.col-item{padding:5px 14px;font-size:.72rem;color:#64748b;cursor:pointer;border-left:2px solid transparent;transition:all .1s;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.col-item:hover{background:#1a2235;color:#94a3b8}
.col-item.active{background:#1e2d40;color:#7dd3fc;border-left-color:#38bdf8}
.col-item.influenced{color:#38bdf8;background:#0c1f38;border-left-color:#1d4ed8}

/* main trace area */
#main{flex:1;overflow-y:auto;padding:28px 36px}
.empty-msg{font-size:.8rem;color:#374151;font-style:italic;padding-top:60px;text-align:center}

/* trace */
.trace-header{margin-bottom:24px}
.trace-col-name{font-size:1.3rem;font-weight:700;color:#e2e8f0;margin-bottom:4px}
.trace-target{font-size:.7rem;color:#374151}
.trace-target span{color:#64748b}

/* vertical timeline */
.timeline{position:relative;padding-left:28px}
.timeline::before{content:'';position:absolute;left:9px;top:12px;bottom:12px;width:1px;background:#1e2d40}

.step{position:relative;margin-bottom:2px}
.step-dot{position:absolute;left:-22px;top:12px;width:11px;height:11px;border-radius:50%;border:2px solid #0d1117}

/* role colors */
.step.source   .step-dot{background:#34d399;border-color:#166534}
.step.created  .step-dot{background:#38bdf8;border-color:#0c4a6e;width:13px;height:13px;left:-23px;top:11px}
.step.modified .step-dot{background:#fb923c;border-color:#7c2d12;width:13px;height:13px;left:-23px;top:11px}
.step.passthrough .step-dot{background:#1e2d40;border-color:#334155}

.step-card{padding:10px 14px;border-radius:7px;margin-bottom:2px;border:1px solid transparent}
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

.step-args{font-family:'SF Mono',Consolas,monospace;font-size:.65rem;color:#6b7280;margin-top:3px;white-space:pre-wrap;word-break:break-all;max-height:60px;overflow-y:auto}
.step.passthrough .step-args{color:#2d3f55;font-size:.6rem;max-height:40px}

.step-loc{font-size:.62rem;color:#2d3f55;margin-top:4px}
.step-loc span{color:#374151}
.step.passthrough .step-loc span{color:#2d3f55}

/* passthrough group collapse */
.pt-group{margin-bottom:2px}
.pt-toggle{font-size:.65rem;color:#374151;cursor:pointer;padding:4px 0;user-select:none;text-align:center;border:1px dashed #1e2d40;border-radius:4px;margin:2px 0}
.pt-toggle:hover{color:#64748b;border-color:#2d3f55}
.pt-inner{display:block}
.pt-inner.collapsed{display:none}

/* target overview */
.t-overview{padding:0 36px 0 36px}
.t-ov-title{font-size:1.1rem;font-weight:700;color:#e2e8f0;margin-bottom:2px}
.t-ov-sub{font-size:.72rem;color:#374151;margin-bottom:18px}
.t-ov-section{font-size:.6rem;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;margin-top:18px}
.t-ov-cols{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px}
.t-ov-chip{font-size:.65rem;padding:2px 9px;border-radius:9px;white-space:nowrap;cursor:pointer}
.t-ov-chip.created{background:#0c2340;color:#38bdf8;border:1px solid #1d4ed8}
.t-ov-chip.modified{background:#431407;color:#fb923c;border:1px solid #c2410c}
.t-ov-chip.passthrough{background:#1a2235;color:#4b5563;border:1px solid #21283a}
.t-ov-chip:hover{opacity:.8}
.t-ov-src{display:flex;flex-wrap:wrap;gap:4px}
.t-ov-stat{display:inline-flex;align-items:center;gap:6px;margin-right:14px;font-size:.68rem;color:#4b5563}
.t-ov-stat b{color:#94a3b8}
</style>
</head>
<body>
<div id="hdr">
  <h1 id="pg-title">Lineage</h1>
  <span class="sub" id="pg-sub"></span>
</div>
<div id="wrap">

  <div id="left">
    <div class="sec">Sources  <span id="src-count" style="font-weight:400;text-transform:none;letter-spacing:0;color:#374151"></span></div>
    <div id="src-block"></div>

    <div class="sec">Targets  <span id="t-count" style="font-weight:400;text-transform:none;letter-spacing:0;color:#374151"></span></div>
    <div id="targets-wrap"></div>

    <div id="cols-panel" style="display:none">
      <div class="sec">Columns</div>
      <div id="col-search-wrap"><input id="col-search" placeholder="Search…" oninput="filterCols(this.value)"></div>
      <div id="col-list"></div>
    </div>
  </div>

  <div id="drag"></div>

  <div id="main">
    <p class="empty-msg">Select a target, then a column to trace its lineage</p>
  </div>

</div>
<script>
const DATA = __DATA__;

// ── init ──────────────────────────────────────────────────────────────────────
document.getElementById('pg-title').textContent = DATA.meta.title;
document.getElementById('pg-sub').textContent   =
  DATA.targets.length + ' target' + (DATA.targets.length !== 1 ? 's' : '');
document.getElementById('t-count').textContent  =
  '(' + DATA.targets.length + ')';

// Build all sources
const srcBlock = document.getElementById('src-block');
document.getElementById('src-count').textContent =
  '(' + (DATA.all_sources ? DATA.all_sources.length : 1) + ')';

(DATA.all_sources || [DATA.source]).forEach(src => {
  const entry = document.createElement('div');
  entry.className = 'src-entry';
  const nameEl = document.createElement('div');
  nameEl.className = 'src-name' + (src.primary === false ? ' secondary' : '');
  nameEl.textContent = src.name || src.id.slice(0,8);
  if (src.primary === false) nameEl.title = 'Contributing source (joined)';
  entry.appendChild(nameEl);
  const colsEl = document.createElement('div');
  colsEl.className = 'src-cols';
  (src.columns || []).forEach(c => {
    const chip = document.createElement('span');
    chip.className = 'col-chip'; chip.textContent = c;
    if (src.primary !== false) {
      chip.title = 'Click to highlight influenced targets';
      chip.dataset.col = c;
      chip.onclick = () => selectSourceCol(c);
    }
    colsEl.appendChild(chip);
  });
  entry.appendChild(colsEl);
  srcBlock.appendChild(entry);
});

// ── targets list ─────────────────────────────────────────────────────────────
let activeTarget = null, activeCol = null, activeSourceCol = null;

function targetDisplayName(t) {
  // Prefer qualname (e.g. "SalesAnalyticsPipeline.customer_lifetime_value")
  // over raw operation label (e.g. ".orderBy() · customer_lifetime_value()")
  if (t.caller && t.caller.qualname) return t.caller.qualname + '()';
  if (t.name) return t.name;
  return t.label;
}

function targetSubLine(t) {
  if (!t.caller) return '';
  const op = t.label.split('·')[0].trim();   // e.g. ".orderBy()"
  return op + '  ·  ' + t.caller.file_short + ':' + t.caller.line;
}

function buildTargets() {
  const wrap = document.getElementById('targets-wrap');
  wrap.innerHTML = '';
  DATA.targets.forEach(t => {
    const el = document.createElement('div');
    el.className = 'target-item';
    el.dataset.id = t.id;
    const sub = targetSubLine(t);
    el.innerHTML = `<div class="t-label">${esc(targetDisplayName(t))}</div>${sub ? '<div class="t-sub">'+esc(sub)+'</div>' : ''}<div class="t-influence-badge"></div>`;
    el.onclick = () => selectTarget(t.id);
    wrap.appendChild(el);
  });
  applySourceColFilter();
}

// ── source column filter ──────────────────────────────────────────────────────
function selectSourceCol(col) {
  if (activeSourceCol === col) {
    // toggle off
    activeSourceCol = null;
    document.querySelectorAll('.col-chip').forEach(el => el.classList.remove('selected'));
  } else {
    activeSourceCol = col;
    document.querySelectorAll('.col-chip').forEach(el =>
      el.classList.toggle('selected', el.dataset.col === col));
  }
  applySourceColFilter();
  // refresh column list if a target is already selected
  if (activeTarget) buildColList(document.getElementById('col-search').value);
}

function applySourceColFilter() {
  if (!activeSourceCol) {
    // clear all dim/influenced
    document.querySelectorAll('.target-item').forEach(el => {
      el.classList.remove('dim', 'influenced');
      const badge = el.querySelector('.t-influence-badge');
      if (badge) badge.textContent = '';
    });
    return;
  }
  const influence = (DATA.source_influence || {})[activeSourceCol] || {};
  document.querySelectorAll('.target-item').forEach(el => {
    const tid  = el.dataset.id;
    const cols = influence[tid];
    if (cols && cols.length) {
      el.classList.remove('dim');
      el.classList.add('influenced');
      const badge = el.querySelector('.t-influence-badge');
      if (badge) badge.textContent = cols.length + ' col' + (cols.length > 1 ? 's' : '') + ' influenced';
    } else {
      el.classList.remove('influenced');
      el.classList.add('dim');
      const badge = el.querySelector('.t-influence-badge');
      if (badge) badge.textContent = '';
    }
  });
}

function selectTarget(tid) {
  activeTarget = tid;
  activeCol    = null;
  document.querySelectorAll('.target-item').forEach(el =>
    el.classList.toggle('active', el.dataset.id === tid));
  buildColList('');
  document.getElementById('cols-panel').style.display = 'flex';
  document.getElementById('cols-panel').style.flexDirection = 'column';
  document.getElementById('col-search').value = '';
  renderTargetOverview(tid);
}

function renderTargetOverview(tid) {
  const t      = DATA.targets.find(x => x.id === tid);
  const traces = DATA.traces[tid] || {};

  // Classify each column by its most significant role
  const byRole = { created: [], modified: [], passthrough: [] };
  const allSources = new Set();

  t.columns.forEach(col => {
    const trace = traces[col];
    if (!trace) { byRole.passthrough.push(col); return; }
    (trace.sources || []).forEach(s => allSources.add(s));
    const steps = trace.steps || [];
    if (steps.some(s => s.role === 'created'))       byRole.created.push(col);
    else if (steps.some(s => s.role === 'modified'))  byRole.modified.push(col);
    else                                               byRole.passthrough.push(col);
  });

  const chip = (col, role) =>
    `<span class="t-ov-chip ${role}" title="click to trace" onclick="selectColumnFromOverview('${esc(col)}')">${esc(col)}</span>`;

  const srcChips = [...allSources].map(s =>
    `<span style="font-size:.65rem;color:#34d399;background:#052e16;padding:2px 9px;border-radius:9px;border:1px solid #166534">${esc(s)}</span>`
  ).join('');

  const name = t.caller && t.caller.qualname ? t.caller.qualname + '()' : t.label;
  const loc  = t.caller ? t.caller.file_short + ':' + t.caller.line : '';

  document.getElementById('main').innerHTML = `
    <div class="t-overview">
      <div class="t-ov-title">${esc(name)}</div>
      <div class="t-ov-sub">${esc(loc)}</div>
      <div style="display:flex;flex-wrap:wrap;gap:0">
        <span class="t-ov-stat"><b>${t.columns.length}</b> columns</span>
        ${byRole.created.length   ? `<span class="t-ov-stat"><b>${byRole.created.length}</b> created</span>` : ''}
        ${byRole.modified.length  ? `<span class="t-ov-stat"><b>${byRole.modified.length}</b> modified</span>` : ''}
        ${byRole.passthrough.length ? `<span class="t-ov-stat"><b>${byRole.passthrough.length}</b> passthrough</span>` : ''}
      </div>
      ${allSources.size ? '<div class="t-ov-section">Derived from source columns</div><div class="t-ov-src">'+srcChips+'</div>' : ''}
      ${byRole.created.length   ? '<div class="t-ov-section">Created in this target</div><div class="t-ov-cols">'+byRole.created.map(c=>chip(c,'created')).join('')+'</div>' : ''}
      ${byRole.modified.length  ? '<div class="t-ov-section">Modified along the way</div><div class="t-ov-cols">'+byRole.modified.map(c=>chip(c,'modified')).join('')+'</div>' : ''}
      ${byRole.passthrough.length ? '<div class="t-ov-section">Passed through unchanged</div><div class="t-ov-cols">'+byRole.passthrough.map(c=>chip(c,'passthrough')).join('')+'</div>' : ''}
    </div>`;
}

function selectColumnFromOverview(col) {
  // Clicking a column chip in the overview selects it directly
  activeCol = col;
  buildColList(document.getElementById('col-search').value);
  renderTrace(activeTarget, col);
}

// ── columns list ─────────────────────────────────────────────────────────────
function buildColList(filter) {
  const t    = DATA.targets.find(x => x.id === activeTarget);
  const list = document.getElementById('col-list');
  list.innerHTML = '';
  if (!t) return;
  // columns influenced by the active source col in this target
  const influencedSet = activeSourceCol
    ? new Set(((DATA.source_influence || {})[activeSourceCol] || {})[t.id] || [])
    : new Set();
  t.columns
    .filter(c => !filter || c.toLowerCase().includes(filter.toLowerCase()))
    .forEach(col => {
      const el = document.createElement('div');
      let cls = 'col-item';
      if (col === activeCol)        cls += ' active';
      else if (influencedSet.has(col)) cls += ' influenced';
      el.className = cls;
      el.textContent = col; el.title = col;
      el.onclick = () => selectColumn(col);
      list.appendChild(el);
    });
}

function filterCols(q) { buildColList(q); }

function selectColumn(col) {
  activeCol = col;
  buildColList(document.getElementById('col-search').value);
  renderTrace(activeTarget, col);
}

// ── trace renderer ────────────────────────────────────────────────────────────
function renderTrace(tid, col) {
  const t      = DATA.targets.find(x => x.id === tid);
  const trace  = (DATA.traces[tid] || {})[col];
  if (!trace) {
    document.getElementById('main').innerHTML =
      '<p class="empty-msg">No trace data for this column</p>';
    return;
  }

  const grouped = groupPassthroughs([...(trace.steps || [])].reverse());

  const timelineHTML = grouped.map((item, idx) => {
    if (item._group) {
      const uid = 'pg' + idx;
      const n   = item.steps.length;
      return `<div class="pt-group">
        <div class="pt-inner" id="${uid}">${item.steps.map(s => stepHTML(s)).join('')}</div>
        <div class="pt-toggle" onclick="togglePT('${uid}',this)">
          ▴ collapse ${n} passthrough step${n > 1 ? 's' : ''}
        </div>
      </div>`;
    }
    return stepHTML(item);
  }).join('');

  const srcList = (trace.sources || []).map(s =>
    `<span style="font-size:.7rem;color:#34d399;background:#052e16;padding:1px 8px;border-radius:9px;border:1px solid #166534">${esc(s)}</span>`
  ).join(' ');

  document.getElementById('main').innerHTML = `
    <div class="trace-header">
      <div class="trace-col-name">${esc(col)}</div>
      <div class="trace-target">in target <span>${esc(t ? t.label : tid)}</span></div>
      ${srcList ? '<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:4px;align-items:center"><span style="font-size:.62rem;color:#374151;margin-right:4px">derived from</span>'+srcList+'</div>' : ''}
    </div>
    <div class="timeline">${timelineHTML || '<p class="empty-msg">No steps</p>'}</div>
  `;
}

function groupPassthroughs(steps) {
  const out = []; let buf = [];
  steps.forEach(s => {
    if (s.role === 'passthrough') { buf.push(s); }
    else {
      if (buf.length) { out.push({ _group: true, steps: [...buf] }); buf = []; }
      out.push(s);
    }
  });
  if (buf.length) out.push({ _group: true, steps: [...buf] });
  return out;
}

function stepHTML(s) {
  const roleLabel = {source:'Source',created:'Created here',modified:'Modified here',passthrough:'Passthrough'}[s.role] || s.role;
  const op        = s.operation === 'source' ? (s.name ? s.name : 'source') : '.' + s.operation + '()';
  const args      = (s.args || []).join(',\\n');
  const loc       = s.caller
    ? (s.caller.qualname || s.caller.func) + '()  ·  ' + s.caller.file_short + ':' + s.caller.line
    : '';
  return `<div class="step ${s.role}">
    <div class="step-dot"></div>
    <div class="step-card">
      <div class="step-role">${roleLabel}</div>
      <div class="step-op">${esc(op)}</div>
      ${args ? '<div class="step-args">'+esc(args)+'</div>' : ''}
      ${loc  ? '<div class="step-loc"><span>'+esc(loc)+'</span></div>' : ''}
    </div>
  </div>`;
}

function togglePT(uid, btn) {
  const inner = document.getElementById(uid);
  const collapsed = inner.classList.toggle('collapsed');
  const n = inner.querySelectorAll('.step').length;
  btn.textContent = collapsed
    ? '▾ expand ' + n + ' passthrough step' + (n > 1 ? 's' : '')
    : '▴ collapse ' + n + ' passthrough step' + (n > 1 ? 's' : '');
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── resizable sidebar ────────────────────────────────────────────────────────
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
  left.style.width = Math.max(180, Math.min(440, startW + e.clientX - startX)) + 'px';
});
document.addEventListener('mouseup', () => {
  dragging = false; drag.classList.remove('on');
  document.body.style.cssText = '';
});

buildTargets();
</script>
</body>
</html>
"""
