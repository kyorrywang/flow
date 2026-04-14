from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment
from utils.llm_utils import build_llm_client

class LLMJsonNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.next_node = node_def["next"]
        self.prompt_template = node_def["prompt_template"]
        self.system_prompt = node_def.get("system_prompt")
        self.save_as = node_def.get("save_as")
        self.extract_keys = node_def.get("extract_keys", [])
        self.output_file = node_def.get("output_file")
        self.overwrite = node_def.get("overwrite", True)
        self.env = env
        self.llm = build_llm_client(node_def.get("llm", {}), env)

    def execute(self, state: Any) -> StepResult:
        from tools.writer import SafeFormatDict
        import json
        context = dict(state.context)
        prompt = self.prompt_template.format_map(SafeFormatDict(context))

        context_update = {}
        if self.llm is None:
            parsed_json = {"stub": True}
            for k in self.extract_keys:
                parsed_json[k] = 3  # Dummy value for testing
        else:
            parsed_json, _ = self.llm.generate_json(system=self.system_prompt, prompt=prompt)

        if self.save_as:
            context_update[self.save_as] = parsed_json

        if isinstance(parsed_json, dict):
            for k in self.extract_keys:
                if k in parsed_json:
                    context_update[k] = parsed_json[k]

        # 写入文件
        if self.output_file:
            relative_path = self.env.writer.render_path(self.output_file, context)
            content = json.dumps(parsed_json, ensure_ascii=False, indent=2)
            written_path = self.env.writer.write(
                run_id=state.run_id,
                relative_path=relative_path,
                content=content,
                overwrite=self.overwrite,
            )
            files = dict(context.get("files", {}))
            files[self.save_as or self.node_id] = written_path
            context_update["files"] = files

        return StepResult(
            next_node=self.next_node,
            status=RUNNING,
            context_update=context_update,
            event_payload={"node_type": "llm_json", "extracted": list(context_update.keys())},
        )
