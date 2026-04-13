from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment

class LLMNode:
    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        self.node_id = node_def["id"]
        self.next_node = node_def["next"]
        self.prompt_template = node_def["prompt_template"]
        self.system_prompt = node_def.get("system_prompt")
        self.save_as = node_def.get("save_as")
        self.output_file = node_def.get("output_file")
        self.overwrite = node_def.get("overwrite", True)
        self.env = env
        self.llm = self._resolve_llm_client(node_def.get("llm", {}))

    def _resolve_llm_client(self, node_llm_config: dict[str, Any]) -> Any:
        merged_config = dict(self.env.config.get("llm", {}))
        merged_config.update(node_llm_config)

        required_fields = ("provider", "api_key", "model", "base_url")
        if not all(merged_config.get(field) for field in required_fields):
            return None

        from nodes.llm import LLMConfig, LLMClient
        return LLMClient(
            LLMConfig(
                provider=merged_config["provider"],
                api_key=merged_config["api_key"],
                model=merged_config["model"],
                base_url=merged_config["base_url"],
                timeout=merged_config.get("timeout", 60),
                temperature=merged_config.get("temperature", 0.2),
                max_tokens=merged_config.get("max_tokens", 8192),
            )
        )

    def execute(self, state: Any) -> StepResult:
        from nodes.writer import SafeFormatDict
        context = dict(state.context)
        prompt = self.prompt_template.format_map(SafeFormatDict(context))
        
        if self.llm is None:
            content = f"# Stub Output: {self.node_id}\n\nPrompt:\n{prompt}\n"
        else:
            result = self.llm.generate(system=self.system_prompt, prompt=prompt)
            content = result.text.strip()
            
        context_update = {}
        if self.save_as:
            context_update[self.save_as] = content
            
        if self.output_file:
            relative_path = self.env.writer.render_path(self.output_file, context)
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
            event_payload={"node_type": "llm", "save_as": self.save_as},
        )
