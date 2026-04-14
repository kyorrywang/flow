from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment

class GatherNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.next_node = node_def["next"]
        self.children_key = node_def["children_key"]
        self.extract_keys = node_def.get("extract_keys", [])
        self.save_as = node_def.get("save_as", f"{self.node_id}_gathered")
        self.format_template = node_def.get("format_template")
        self.join_str = node_def.get("join_str", "\n\n")
        self.env = env

    def execute(self, state: Any) -> StepResult:
        context = dict(state.context)
        children_ids = context.get(self.children_key, [])
        gathered = []
        
        missing_ids = []
        for cid in children_ids:
            try:
                child = self.env.engine.store.get_run(cid)
                child_data = {"child_run_id": cid}
                
                for k in self.extract_keys:
                    if k in child.context:
                        child_data[k] = child.context[k]
                        
                gathered.append(child_data)
            except KeyError:
                missing_ids.append(cid)
                continue
                
        result_value: Any = gathered
        if self.format_template:
            from tools.writer import SafeFormatDict
            text_blocks = []
            for data in gathered:
                text_blocks.append(self.format_template.format_map(SafeFormatDict(data)))
            result_value = self.join_str.join(text_blocks)
        
        return StepResult(
            next_node=self.next_node,
            status=RUNNING,
            context_update={self.save_as: result_value},
            event_payload={"gathered_count": len(gathered), "missing_count": len(missing_ids), "missing_ids": missing_ids},
        )
