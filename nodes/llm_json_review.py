from typing import Any
from flow import RUNNING, StepResult
from nodes.base import NodeEnvironment
from nodes.llm_json import LLMJsonNode
from tools.writer import SafeFormatDict
from utils.llm_utils import build_llm_client


class LLMJsonReviewNode(LLMJsonNode):
    """LLM JSON node with embedded review loop.
    
    Executes: generate → review → (modify → review)×N → save → next
    Review loop exits early on "pass" or when max_rounds reached.
    """

    def __init__(self, node_def: dict[str, Any], env: NodeEnvironment) -> None:
        super().__init__(node_def, env)
        
        # Parse review_loop configuration
        review_loop = node_def.get("review_loop", {})
        self.max_rounds = review_loop.get("max_rounds", 3)
        self.review_system_prompt = review_loop.get("review_system_prompt")
        self.review_criteria = review_loop.get("review_criteria", [])
        self.modify_system_prompt = review_loop.get("modify_system_prompt")
        
        # Build review/modify LLM client (reuse generate config, optionally override)
        review_llm_config = dict(review_loop.get("llm", {}))
        # Default review to low temperature for consistent judgment
        review_llm_config.setdefault("temperature", 0.2)
        self.review_llm = build_llm_client(review_llm_config, env)
        
        # Modify uses same config as generate (creative revision)
        self.modify_llm = self.llm

    def execute(self, state: Any) -> StepResult:
        context = dict(state.context)
        prompt = self.prompt_template.format_map(SafeFormatDict(context))
        
        # Step 1: Generate initial content
        if self.llm is None:
            content = {"stub": True}
            for k in self.extract_keys:
                content[k] = 3
        else:
            content, _ = self.llm.generate_json(
                system=self.system_prompt,
                prompt=prompt
            )
        
        # Step 2: Review loop
        review_history = []
        
        for round_num in range(1, self.max_rounds + 1):
            # Build review prompt
            review_prompt = self._build_review_prompt(content, round_num)
            
            # Execute review
            review_result, _ = self.review_llm.generate_json(
                system=self.review_system_prompt,
                prompt=review_prompt
            )
            
            review_status = review_result.get("review_status", "pass")
            review_comments = review_result.get("comments", [])
            
            review_history.append({
                "round": round_num,
                "review_status": review_status,
                "comments": review_comments,
            })
            
            # Early exit on pass
            if review_status == "pass":
                break
            
            # Max rounds reached — exit without modifying
            if round_num >= self.max_rounds:
                break
            
            # Step 3: Modify based on review feedback
            modify_prompt_text = self._build_modify_prompt(
                content, review_comments, round_num
            )
            
            content, _ = self.modify_llm.generate_json(
                system=self.modify_system_prompt,
                prompt=modify_prompt_text
            )
        
        # Step 4: Build context update and write file
        context_update = {}
        if self.save_as:
            context_update[self.save_as] = content
        
        if isinstance(content, dict):
            for k in self.extract_keys:
                if k in content:
                    context_update[k] = content[k]
        
        context_update["review_history"] = review_history
        
        # Write output file (overwrite mode)
        if self.output_file:
            import json
            relative_path = self.env.writer.render_path(self.output_file, context)
            content_json = json.dumps(content, ensure_ascii=False, indent=2)
            written_path = self.env.writer.write(
                run_id=state.run_id,
                relative_path=relative_path,
                content=content_json,
                overwrite=self.overwrite,
            )
            files = dict(context.get("files", {}))
            files[self.save_as or self.node_id] = written_path
            context_update["files"] = files
        
        return StepResult(
            next_node=self.next_node,
            status=RUNNING,
            context_update=context_update,
            event_payload={
                "node_type": "llm_json_review",
                "extracted": list(context_update.keys()),
                "review_rounds": len(review_history),
            },
        )

    def _build_review_prompt(self, content: Any, round_num: int) -> str:
        """Build the review prompt with content and criteria."""
        import json
        
        parts = []
        
        # Round info
        parts.append(f"你正在进行第 {round_num} 轮审查。")
        parts.append(f"最多 {self.max_rounds} 轮。")
        
        # Content to review
        if isinstance(content, dict):
            content_str = json.dumps(content, ensure_ascii=False, indent=2)
        else:
            content_str = str(content)
        parts.append(f"\n【审查对象】\n{content_str}")
        
        # Review criteria
        if self.review_criteria:
            parts.append("\n【审查标准】")
            for i, criterion in enumerate(self.review_criteria, 1):
                name = criterion.get("name", f"标准{i}")
                question = criterion.get("question", "")
                parts.append(f"{i}. {name}：{question}")
        
        # Output format instructions
        parts.append("\n【输出格式】")
        parts.append('如果内容合格，返回：{"review_status": "pass", "comments": []}')
        parts.append('如果需要修改，返回：{"review_status": "revise", "comments": [{"point": "具体修改点", "reason": "为什么需要修改"}]}')
        parts.append("\n【规则】")
        parts.append("- 最多提出 3 个修改点（按重要性排序）")
        parts.append("- 修改点要具体可执行，不要泛泛而谈")
        parts.append("- 如果只是小瑕疵不影响整体，可以跳过")
        
        return "\n".join(parts)

    def _build_modify_prompt(self, content: Any, comments: list, round_num: int) -> str:
        """Build the modify prompt with original content and review comments."""
        import json
        
        parts = []
        parts.append(f"你是作者，请根据第 {round_num} 轮审查意见修改内容。")
        
        # Original content
        if isinstance(content, dict):
            content_str = json.dumps(content, ensure_ascii=False, indent=2)
        else:
            content_str = str(content)
        parts.append(f"\n【原始内容】\n{content_str}")
        
        # Review comments
        if comments:
            parts.append("\n【审查意见】")
            for i, comment in enumerate(comments, 1):
                point = comment.get("point", "")
                reason = comment.get("reason", "")
                parts.append(f"{i}. 修改点：{point}")
                if reason:
                    parts.append(f"   原因：{reason}")
        
        # Modification instructions
        parts.append("\n【修改要求】")
        parts.append("- 保持核心创意不变")
        parts.append("- 针对性解决审查意见中指出的问题")
        parts.append("- 输出修改后的完整内容（不要只输出修改部分）")
        
        return "\n".join(parts)
