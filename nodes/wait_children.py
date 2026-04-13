from typing import Any
from flow import WAITING_CHILDREN, DONE, FAILED, StepResult
from nodes.base import NodeEnvironment

class WaitChildrenNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.next_node = node_def["next"]
        self.children_key = node_def["children_key"]
        self.on_child_failure = node_def.get("on_child_failure", "fail_parent")
        self.env = env

    def execute(self, state: Any) -> StepResult:
        context = dict(state.context)
        children_ids = context.get(self.children_key, [])
        all_done = True
        failed_count = 0
        
        for cid in children_ids:
            try:
                child = self.env.engine.store.get_run(cid)
                if child.status == FAILED:
                    if self.on_child_failure == "fail_parent":
                        return StepResult(
                            next_node=self.node_id,
                            status=FAILED,
                            context_update={},
                            event_payload={"error": f"Child run {cid} failed"},
                        )
                    failed_count += 1
                elif child.status != DONE:
                    all_done = False
                    break
            except KeyError:
                all_done = False
                break
        
        if all_done:
            return StepResult(
                next_node=self.next_node,
                status="running",
                context_update={},
                event_payload={"waiting_children": "all_done", "count": len(children_ids), "failed_count": failed_count},
            )
        else:
            return StepResult(
                next_node=self.node_id,
                status=WAITING_CHILDREN,
                context_update={},
                event_type="waiting_for_children",
                event_payload={"waiting_children": "pending", "count": len(children_ids), "failed_count": failed_count},
            )
