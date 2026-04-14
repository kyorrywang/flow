from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment

class BranchNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.condition_key = node_def["condition_key"]
        self.branches = node_def["branches"]
        self.default_next = node_def["default_next"]
        self.env = env

    def execute(self, state: Any) -> StepResult:
        from utils.template_utils import resolve_context_value
        context = dict(state.context)
        
        try:
            val = resolve_context_value(self.condition_key, context)
            val_str = str(val).lower() if isinstance(val, bool) else str(val)
        except Exception:
            val_str = ""
            
        next_node = self.branches.get(val_str, self.default_next)

        return StepResult(
            next_node=next_node,
            status=RUNNING,
            context_update={},
            event_payload={"branch_value": val_str, "selected_branch": next_node},
        )
