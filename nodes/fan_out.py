from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment
from utils.fanout_utils import compute_child_template_path, build_child_context
from utils.template_utils import resolve_context_value

class FanOutNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.next_node = node_def["next"]
        self.count_from = node_def["count_from"]
        self.target = node_def["target"]
        self.child_flow_name = node_def.get("child_flow_name")
        self.child_context_mapping = node_def.get("child_context", {})
        self.save_as = node_def.get("save_as", f"{self.node_id}_children")
        self.env = env

    def execute(self, state: Any) -> StepResult:
        context = dict(state.context)
        count = int(resolve_context_value(self.count_from, context))
        child_template_path = compute_child_template_path(
            context.get("template_path", ""),
            self.child_flow_name,
        )

        def make_child_context(index: int) -> dict[str, Any]:
            return build_child_context(
                index=index,
                context=context,
                target=self.target,
                state_run_id=state.run_id,
                state_flow_name=state.flow_name,
                child_flow_name=self.child_flow_name,
                child_template_path=child_template_path,
                child_context_mapping=self.child_context_mapping,
            )

        children = self.env.engine.spawn_children(
            state.run_id,
            count=count,
            flow_name=self.child_flow_name or state.flow_name,
            start_node=self.target,
            context_builder=make_child_context,
            run_id_prefix=f"{state.run_id}__{self.node_id}",
        )

        return StepResult(
            next_node=self.next_node,
            status=RUNNING,
            context_update={self.save_as: [child.run_id for child in children]},
            event_payload={"spawned_children": len(children), "target": self.target},
        )
