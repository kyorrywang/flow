from typing import Any
import re
from tools.writer import SafeFormatDict

def resolve_nested_path(value: Any, path: str) -> Any:
    """解析嵌套路径如 volumes[0][name]"""
    if not path:
        return value
    
    # 处理列表索引和字典键
    # 匹配 [index] 或 [key]
    pattern = r'\[([^\]]+)\]'
    matches = re.findall(pattern, path)
    base_key = path.split('[')[0]
    
    # 获取基础值
    if isinstance(value, dict) and base_key in value:
        current = value[base_key]
    elif base_key == '' and value is not None:
        current = value
    else:
        return value
    
    # 逐级访问
    for match in matches:
        # 尝试转换为整数
        try:
            idx = int(match)
            if isinstance(current, list):
                current = current[idx]
            else:
                return current
        except ValueError:
            # 作为字典键
            if isinstance(current, dict):
                current = current.get(match, current)
            else:
                return current
    
    return current

def resolve_context_value(path: str, context: dict[str, Any]) -> Any:
    if path.startswith("context."):
        current: Any = context
        for part in path.split(".")[1:]:
            current = current[part]
        return current
    
    # 支持 {variable|filter} 语法
    if path.startswith("{") and path.endswith("}"):
        inner = path[1:-1].strip()
        # 处理管道操作符
        if "|" in inner:
            parts = inner.split("|", 1)
            var_name = parts[0].strip()
            operation = parts[1].strip()
            
            # 获取变量值
            value = context.get(var_name)
            if value is None:
                # 尝试嵌套路径
                for key in var_name.split("."):
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        value = context.get(var_name)
                        break
            
            # 处理操作
            if operation == "length" or operation == "len":
                return len(value) if value is not None else 0
            elif operation.startswith("filter:"):
                # 简单过滤（暂不实现复杂过滤）
                return value
            return value
        
        # 简单变量引用或嵌套路径
        var_name = inner.strip()
        # 尝试解析嵌套路径如 volumes[0][name]
        if '[' in var_name:
            return resolve_nested_path(context, var_name)
        if var_name in context:
            return context[var_name]
        # 尝试嵌套路径
        parts = var_name.split(".")
        current = context
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return path  # 无法解析，返回原值
        return current
    
    return path

def render_value(value: Any, context: dict[str, Any], *, index: int) -> Any:
    if isinstance(value, str):
        merged = dict(context)
        merged["index"] = index
        
        # 处理嵌套路径如 volumes[fanout_index-1][name]
        # 先替换 fanout_index 为实际值
        import re
        def replace_fanout_index(match):
            full = match.group(0)
            # 提取 fanout_index 表达式
            expr = match.group(1)
            try:
                # 简单计算 fanout_index-1
                if 'fanout_index' in expr:
                    val = index
                    if '-1' in expr or '- 1' in expr:
                        val = index - 1
                    return str(val)
                return expr
            except:
                return expr
        
        # 替换 [fanout_index-1] 为实际索引
        processed = re.sub(r'\[(fanout_index\s*-\s*1)\]', lambda m: f'[{index-1}]', value)
        # 替换 [fanout_index] 为实际索引
        processed = re.sub(r'\[(fanout_index)\]', lambda m: f'[{index}]', processed)
        
        # 处理嵌套路径如 volumes[0][name]
        def resolve_nested(match):
            full_path = match.group(0)
            result = resolve_nested_path(merged, full_path.strip('{}'))
            if isinstance(result, (str, int, float, bool)):
                return str(result)
            return full_path  # 复杂类型保持原样
        
        # 先尝试 format_map 处理简单变量
        try:
            result = processed.format_map(SafeFormatDict(merged))
            # 如果结果包含未解析的 {path[...]} 模式，用 resolve_nested 处理
            if '{' in result and '[' in result:
                result = re.sub(r'\{[^}]+\[[^\]]+\][^\}]*\}', resolve_nested, result)
            return result
        except:
            return processed
    if isinstance(value, list):
        return [render_value(item, context, index=index) for item in value]
    if isinstance(value, dict):
        return {key: render_value(item, context, index=index) for key, item in value.items()}
    return value
