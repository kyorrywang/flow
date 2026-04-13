from typing import Any
from nodes.writer import SafeFormatDict

def resolve_context_value(path: str, context: dict[str, Any]) -> Any:
    if path.startswith("context."):
        current: Any = context
        for part in path.split(".")[1:]:
            current = current[part]
        return current
    return path

def render_value(value: Any, context: dict[str, Any], *, index: int) -> Any:
    if isinstance(value, str):
        merged = dict(context)
        merged["index"] = index
        return value.format_map(SafeFormatDict(merged))
    if isinstance(value, list):
        return [render_value(item, context, index=index) for item in value]
    if isinstance(value, dict):
        return {key: render_value(item, context, index=index) for key, item in value.items()}
    return value
