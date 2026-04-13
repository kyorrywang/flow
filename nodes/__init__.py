from nodes.registry import register_node, build_node
from nodes.done import DoneNode
from nodes.wait_input import WaitInputNode
from nodes.wait_children import WaitChildrenNode
from nodes.fan_out import FanOutNode
from nodes.llm_node import LLMNode
from nodes.llm_json import LLMJsonNode
from nodes.gather import GatherNode
from nodes.branch import BranchNode

def ensure_defaults_registered() -> None:
    register_node("done", DoneNode)
    register_node("wait_input", WaitInputNode)
    register_node("wait_children", WaitChildrenNode)
    register_node("fan_out", FanOutNode)
    register_node("llm", LLMNode)
    register_node("llm_json", LLMJsonNode)
    register_node("gather", GatherNode)
    register_node("branch", BranchNode)
