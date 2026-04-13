from typing import Any
from flow import WAITING_INPUT, StepResult
from nodes.base import NodeEnvironment

class WaitInputNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.on_approved = node_def["on_approved"]
        self.on_rejected = node_def.get("on_rejected", self.node_id)
        self.message = node_def.get("message", "")

    def execute(self, state: Any) -> StepResult:
        context = dict(state.context)
        context["resume_to"] = self.on_approved
        context["reject_to"] = self.on_rejected
        context["gate_message"] = self.message
        return StepResult(
            next_node=self.node_id,
            status=WAITING_INPUT,
            context_update=context,
            event_type="waiting_for_input",
            event_payload={"message": self.message},
        )
