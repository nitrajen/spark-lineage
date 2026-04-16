"""
Column-level lineage via Spark's internal query plan.
Uses optimizedPlan() (column-pruned) to find source columns,
and analyzed() to extract the expression that derives a column.
Zero hardcoding — all information comes from the plan tree.
"""
from __future__ import annotations


def get_col_lineage(df, col_name: str) -> dict:
    """
    For col_name in df, return:
      sources  — leaf-level column names (from scan nodes) that contribute to it
      expr     — the Spark expression string that produces it (best-effort)
    """
    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' not found. Available: {df.columns}")

    col_df = df.select(col_name)
    opt    = col_df._jdf.queryExecution().optimizedPlan()
    ana    = col_df._jdf.queryExecution().analyzed()

    return {
        "col":     col_name,
        "sources": sorted(_leaf_attr_names(opt)),
        "expr":    _find_col_expr(ana, col_name),
    }


def _leaf_attr_names(plan) -> set[str]:
    """Recursively collect attribute names from all leaf plan nodes."""
    children = plan.children()
    if children.size() == 0:
        output = plan.output()
        return {output.apply(i).name() for i in range(output.size())}
    result = set()
    for i in range(children.size()):
        result |= _leaf_attr_names(children.apply(i))
    return result


def _find_col_expr(plan, col_name: str) -> str:
    """
    Walk the plan tree top-down looking for the first expression
    whose output attribute is named col_name. Returns its string form.
    Falls back to col_name (passthrough) if not found.
    """
    exprs = plan.expressions()
    for i in range(exprs.size()):
        expr = exprs.apply(i)
        try:
            if str(expr.toAttribute().name()) == col_name:
                try:
                    return str(expr.sql())
                except Exception:
                    return str(expr)
        except Exception:
            pass

    # Recurse into children
    children = plan.children()
    for i in range(children.size()):
        result = _find_col_expr(children.apply(i), col_name)
        if result != col_name:
            return result

    return col_name  # passthrough — column not derived, just selected
