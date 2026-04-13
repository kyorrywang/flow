from typing import Any
from flow import DONE, StepResult
from nodes.base import NodeEnvironment

class DoneNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]

    def execute(self, state: Any) -> StepResult:
        return StepResult(
            next_node=self.node_id,
            status=DONE,
            context_update={},
            event_payload={"node_type": "done"},
        )
