from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from flow import FlowEngine
from nodes.writer import OutputWriter
from nodes.registry import build_node
from store import RunRecord


@dataclass
class TemplateDefinition:
    name: str
    start_node: str
    nodes: list[dict[str, Any]]


class TemplateRuntime:
    def __init__(
        self,
        definition: TemplateDefinition,
        *,
        engine: FlowEngine,
        writer: OutputWriter,
        global_config: dict[str, Any] | None = None,
    ) -> None:
        self.definition = definition
        self._engine = engine
        self._writer = writer
        self._config = global_config or {}
        self.node_map = {node["id"]: node for node in definition.nodes}
        self._register_nodes()

    @property
    def engine(self) -> FlowEngine:
        return self._engine

    @property
    def writer(self) -> OutputWriter:
        return self._writer

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        engine: FlowEngine,
        writer: OutputWriter,
        global_config: dict[str, Any] | None = None,
    ) -> "TemplateRuntime":
        definition = load_template_definition(path)
        return cls(definition, engine=engine, writer=writer, global_config=global_config)

    def _register_nodes(self) -> None:
        for node in self.definition.nodes:
            node_instance = build_node(node["type"], node, self)
            self.engine.register_node(
                node["id"], 
                node_instance.execute,
                metadata={"retry": node.get("retry", 0), "retry_delay": node.get("retry_delay", 0)}
            )



def load_template_definition(path: str | Path) -> TemplateDefinition:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return TemplateDefinition(
        name=data["name"],
        start_node=data["start"],
        nodes=data["nodes"],
    )
