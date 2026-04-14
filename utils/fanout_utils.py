"""
FanOut 节点相关的工具函数
"""
from __future__ import annotations
import json
import os
from typing import Any, Callable
from utils.template_utils import render_value


def compute_child_template_path(
    parent_template_path: str,
    child_flow_name: str | None,
) -> str | None:
    """计算子流程模板路径"""
    if parent_template_path and child_flow_name:
        parent_dir = os.path.dirname(parent_template_path)
        return os.path.join(parent_dir, f"{child_flow_name}.yaml")
    return parent_template_path or None


def build_child_context(
    index: int,
    context: dict[str, Any],
    target: str,
    state_run_id: str,
    state_flow_name: str,
    child_flow_name: str | None,
    child_template_path: str | None,
    child_context_mapping: dict[str, Any],
) -> dict[str, Any]:
    """构建子流程上下文"""
    # 使用 JSON 序列化/反序列化创建深拷贝，确保类型正确
    child_context = json.loads(json.dumps(context, ensure_ascii=False))
    child_context["fanout_index"] = index
    child_context["fanout_target"] = target
    child_context["parent_run_id"] = state_run_id
    child_context["flow_name"] = child_flow_name or state_flow_name
    if child_template_path:
        child_context["template_path"] = child_template_path

    for key, value in child_context_mapping.items():
        child_context[key] = render_value(value, child_context, index=index)
    return child_context
