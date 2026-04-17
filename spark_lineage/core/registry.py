from typing import Optional
from .node import LineageNode, CallerInfo


class LineageRegistry:
    def __init__(self):
        self._nodes: dict[str, LineageNode] = {}
        self._views: dict[str, str] = {}  # view_name -> df_id

    # ── registration ──────────────────────────────────────────────────────────

    def register_root(self, df_id: str, name: Optional[str] = None,
                      output_cols: list = None, caller=None):
        node = LineageNode(
            id=df_id, operation=None, args_repr=[],
            caller=caller, parent_ids=[], name=name,
        )
        if output_cols:
            node.output_cols = list(output_cols)
        self._nodes[df_id] = node

    def register_child(self, df_id: str, parent_ids: list[str],
                       operation: str, args_repr: list[str],
                       caller: CallerInfo, name: Optional[str] = None,
                       output_cols: list[str] = None,
                       column_refs: dict = None):
        node = LineageNode(
            id=df_id, operation=operation, args_repr=args_repr,
            caller=caller, parent_ids=parent_ids, name=name,
        )
        if output_cols is not None:
            node.output_cols = output_cols
        if column_refs:
            node.column_refs = column_refs
        self._nodes[df_id] = node

    def register_view(self, view_name: str, df_id: str):
        self._views[view_name] = df_id

    # ── lookups ───────────────────────────────────────────────────────────────

    def get(self, df_id: str) -> Optional[LineageNode]:
        return self._nodes.get(df_id)

    def get_by_view(self, view_name: str) -> Optional[str]:
        return self._views.get(view_name)

    # ── forward traversal ────────────────────────────────────────────────────

    def _children_index(self) -> dict[str, list[str]]:
        """Build a parent→children mapping from the stored nodes."""
        idx: dict[str, list[str]] = {}
        for nid, node in self._nodes.items():
            for pid in node.parent_ids:
                idx.setdefault(pid, []).append(nid)
        return idx

    def get_all_descendants(self, df_id: str) -> set[str]:
        """All node IDs reachable forward from df_id (exclusive)."""
        children = self._children_index()
        result: set[str] = set()
        queue = [df_id]
        while queue:
            for child in children.get(queue.pop(), []):
                if child not in result:
                    result.add(child)
                    queue.append(child)
        return result

    def get_leaf_descendants(self, df_id: str) -> list[str]:
        """Leaf descendants: reachable from df_id with no further children,
        excluding write nodes (they are persistence ops, not result DataFrames)."""
        children = self._children_index()
        all_desc = self.get_all_descendants(df_id)
        return [
            d for d in all_desc
            if not children.get(d)
            and not (self._nodes[d].operation or "").startswith("write.")
        ]

    # ── path between source and target ───────────────────────────────────────

    def get_path(self, source_id: str, target_id: str) -> list[LineageNode]:
        """
        All nodes on paths from source_id to target_id, topologically ordered
        (source first, target last).  Handles fan-out (joins) correctly by
        including all nodes that lie on *any* path between the two.
        """
        # Backward walk from target → get all ancestors
        ancestors: set[str] = set()

        def _back(nid: str):
            if nid in ancestors:
                return
            ancestors.add(nid)
            node = self._nodes.get(nid)
            if node:
                for pid in node.parent_ids:
                    _back(pid)

        _back(target_id)

        # Forward reachable from source
        on_path = (self.get_all_descendants(source_id) | {source_id}) & ancestors

        # Topological order via post-order DFS from target, restricted to on_path
        topo: list[str] = []
        visited: set[str] = set()

        def _topo(nid: str):
            if nid in visited or nid not in on_path:
                return
            visited.add(nid)
            node = self._nodes.get(nid)
            if node:
                for pid in node.parent_ids:
                    _topo(pid)
            topo.append(nid)

        _topo(target_id)
        return [self._nodes[nid] for nid in topo if nid in self._nodes]


_registry = LineageRegistry()
