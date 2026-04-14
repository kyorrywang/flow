"""
JSON 处理工具函数 - 专门处理 LLM 返回的 JSON 解析和修复
"""
from __future__ import annotations
import json
import re
from typing import Any


class JSONParseError(Exception):
    """JSON 解析错误，携带原始文本以便调试"""
    def __init__(self, message: str, raw_text: str) -> None:
        super().__init__(message)
        self.raw_text = raw_text


def clean_llm_json(raw_text: str) -> str:
    """
    清理 LLM 返回的 JSON 文本
    
    处理：
    - 移除 markdown 代码块包裹
    - 提取 JSON 内容（从第一个 { 到最后一个 }）
    - 清理 BOM 和不可见字符
    - 修复常见格式问题（尾部逗号等）
    """
    cleaned = raw_text.strip()

    # 1. 移除 markdown 代码块包裹
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # 移除第一行（如 ```json）
        if lines[0].startswith("```"):
            lines = lines[1:]
        # 移除最后一行（```）
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    # 2. 如果文本不是以 { 开头，尝试提取第一个 { 到最后一个 }
    if not cleaned.startswith("{"):
        start_idx = cleaned.find("{")
        end_idx = cleaned.rfind("}")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            cleaned = cleaned[start_idx:end_idx+1]

    # 3. 清理 BOM 和其他不可见字符
    cleaned = cleaned.replace("\ufeff", "").strip()

    # 4. 修复常见的 JSON 格式问题
    # 移除尾部逗号（如 {"a": 1,} → {"a": 1}）
    cleaned = re.sub(r',\s*}', '}', cleaned)
    cleaned = re.sub(r',\s*]', ']', cleaned)

    return cleaned


def lenient_json_parse(text: str) -> Any:
    """
    宽松的 JSON 解析器
    
    尝试多种方式解析 JSON：
    1. 标准 json.loads
    2. 使用 ast.literal_eval 解析 Python 风格字典（支持单引号等）
    
    返回解析后的对象，如果失败抛出 JSONParseError
    """
    cleaned = clean_llm_json(text)

    # 1. 尝试标准 JSON 解析
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # 2. 尝试使用 ast.literal_eval 解析（支持单引号、True/False/None 等）
    try:
        import ast
        python_style = (
            cleaned
            .replace('true', 'True')
            .replace('false', 'False')
            .replace('null', 'None')
        )
        return ast.literal_eval(python_style)
    except Exception:
        pass

    # 3. 所有尝试都失败
    raise JSONParseError(f"无法解析为有效 JSON: {text}", raw_text=text)


def parse_llm_json(text: str) -> dict[str, Any]:
    """
    解析 LLM 返回的 JSON，确保返回字典类型
    
    如果解析结果不是字典，抛出 JSONParseError
    """
    result = lenient_json_parse(text)
    if not isinstance(result, dict):
        raise JSONParseError(f"期望返回 JSON 对象（字典），但得到了: {type(result).__name__}", raw_text=text)
    return result
