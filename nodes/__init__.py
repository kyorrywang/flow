from nodes.registry import register_node, build_node
from nodes.done import DoneNode
from nodes.wait_input import WaitInputNode
from nodes.wait_children import WaitChildrenNode
from nodes.fan_out import FanOutNode
from nodes.llm_node import LLMNode

register_node("done", DoneNode)
register_node("wait_input", WaitInputNode)
register_node("wait_children", WaitChildrenNode)
register_node("fan_out", FanOutNode)
register_node("llm", LLMNode)
