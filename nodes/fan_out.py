from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment
from nodes.utils import resolve_context_value, render_value

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
        child_flow_name = self.child_flow_name or state.flow_name
        run_id_prefix = f"{state.run_id}-{self.node_id}"

        def build_child_context(index: int) -> dict[str, Any]:
            child_context = dict(context)
            child_context["fanout_index"] = index
            child_context["fanout_target"] = self.target

            for key, value in self.child_context_mapping.items():
                child_context[key] = render_value(value, child_context, index=index)
            return child_context

        children = self.env.engine.spawn_children(
            state.run_id,
            count=count,
            flow_name=child_flow_name,
            start_node=self.target,
            context_builder=build_child_context,
            run_id_prefix=run_id_prefix,
        )

        return StepResult(
            next_node=self.next_node,
            status=RUNNING,
            context_update={self.save_as: [child.run_id for child in children]},
            event_payload={"spawned_children": len(children), "target": self.target},
        )
