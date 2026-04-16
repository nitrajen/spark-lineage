from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import inspect
import os


@dataclass
class CallerInfo:
    file: str        # absolute path
    module: str      # e.g. "etl", "pipelines.feature_store"
    klass: str       # class name, or "" if top-level / plain function
    function: str    # bare function/method name
    qualname: str    # full qualified name, e.g. "MyClass.my_method" or "my_func"
    line: int

    @classmethod
    def capture(cls, depth: int = 2) -> "CallerInfo":
        stack  = inspect.stack()
        if depth >= len(stack):
            depth = len(stack) - 1
        frame_info = stack[depth]
        frame      = frame_info.frame

        # qualname lives on the code object as __qualname__ (Python 3.3+)
        qualname = frame.f_code.co_qualname if hasattr(frame.f_code, "co_qualname") else frame_info.function

        # Derive class from qualname: "Outer.Inner.method" → klass = "Outer.Inner"
        parts = qualname.split(".")
        # Filter out <locals> segments (closures / nested functions)
        parts = [p for p in parts if not p.startswith("<")]
        if len(parts) >= 2:
            klass    = ".".join(parts[:-1])
            function = parts[-1]
        else:
            klass    = ""
            function = parts[0] if parts else frame_info.function

        # Derive a dotted module path from the file path
        filepath = frame_info.filename
        module   = _file_to_module(filepath)

        return cls(
            file=filepath, module=module, klass=klass,
            function=function, qualname=qualname,
            line=frame_info.lineno,
        )

    # Grouping key used by the report — (module, class, function)
    @property
    def context_key(self) -> tuple[str, str, str]:
        return (self.module, self.klass, self.function)

    def __str__(self) -> str:
        loc = f"{self.qualname}()" if self.qualname else self.function + "()"
        return f"{self.file}:{self.line} in {loc}"


def _file_to_module(filepath: str) -> str:
    """
    Best-effort conversion of an absolute path to a dotted module name.
    E.g. /project/pipelines/feature_store.py  →  pipelines.feature_store
    Falls back to just the stem if no package structure is found.
    """
    try:
        # Walk up looking for __init__.py to find the package root
        directory  = os.path.dirname(os.path.abspath(filepath))
        stem       = os.path.splitext(os.path.basename(filepath))[0]
        parts      = [stem]
        current    = directory
        while True:
            if os.path.exists(os.path.join(current, "__init__.py")):
                parts.append(os.path.basename(current))
                current = os.path.dirname(current)
            else:
                break
        parts.reverse()
        return ".".join(parts) if parts else stem
    except Exception:
        return os.path.splitext(os.path.basename(filepath))[0]


@dataclass
class LineageNode:
    id: str
    operation: Optional[str]    # None = root / source node
    args_repr: list[str]
    caller: Optional[CallerInfo]
    parent_ids: list[str]
    name: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    output_cols: list[str] = field(default_factory=list)
    # {output_col: [directly_referenced_input_cols]} — populated for operations
    # that produce new/modified columns (withColumn, select, agg, etc.).
    # Empty dict means we didn't extract refs for this node type (filter, sort…).
    column_refs: dict = field(default_factory=dict)
