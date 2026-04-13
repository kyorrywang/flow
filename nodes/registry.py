from typing import Any, Callable, Dict
from nodes.base import BaseNode, NodeEnvironment

_REGISTRY: Dict[str, Callable[[dict[str, Any], NodeEnvironment], BaseNode]] = {}

def register_node(node_type: str, factory: Callable[[dict[str, Any], NodeEnvironment], BaseNode]) -> None:
    _REGISTRY[node_type] = factory

def build_node(node_type: str, node_def: dict[str, Any], env: NodeEnvironment) -> BaseNode:
    if node_type not in _REGISTRY:
        raise ValueError(f"Unknown node type: {node_type}")
    return _REGISTRY[node_type](node_def, env)
