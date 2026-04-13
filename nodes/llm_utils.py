from typing import Any
from nodes.base import NodeEnvironment

def build_llm_client(node_llm_config: dict[str, Any], env: NodeEnvironment) -> Any:
    merged_config = dict(env.config.get("llm", {}))
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
            timeout=merged_config.get("timeout", 180),
            temperature=merged_config.get("temperature", 0.2),
            max_tokens=merged_config.get("max_tokens", 8192),
        )
    )
