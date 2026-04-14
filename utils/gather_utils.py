"""
Gather 节点相关的工具函数
"""
from __future__ import annotations
from typing import Any
from tools.writer import SafeFormatDict


def format_gathered_data(
    gathered: list[dict[str, Any]],
    format_template: str,
    join_str: str = "\n\n",
) -> str:
    """将收集的子流程数据格式化为文本"""
    text_blocks = []
    for data in gathered:
        text_blocks.append(format_template.format_map(SafeFormatDict(data)))
    return join_str.join(text_blocks)
