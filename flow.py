from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Callable

from llm import LLMClient, LLMConfig
from store import RunRecord, SqliteStore


RUNNING = "running"
PAUSED = "paused"
WAITING_INPUT = "waiting_input"
FAILED = "failed"
DONE = "done"


@dataclass
class StepResult:
    next_node: str
    status: str = RUNNING
    context_update: dict[str, Any] = field(default_factory=dict)
    event_type: str = "node_completed"
    event_payload: dict[str, Any] = field(default_factory=dict)


NodeHandler = Callable[["FlowEngine", RunRecord], StepResult]


class FlowEngine:
    def __init__(self, store: SqliteStore, llm: LLMClient | None = None) -> None:
        self.store = store
        self.llm = llm
        self.nodes: dict[str, NodeHandler] = {
            "start": self.node_start,
            "think": self.node_think,
            "wait_input": self.node_wait_input,
            "finalize": self.node_finalize,
            "done": self.node_done,
        }

    def create_run(
        self,
        *,
        initial_context: dict[str, Any] | None = None,
        flow_name: str = "simple_flow",
        run_id: str | None = None,
    ) -> RunRecord:
        actual_run_id = run_id or str(uuid.uuid4())
        context = {
            "user_input": "",
            "approved": False,
            "llm_output": "",
            "final_output": "",
        }
        if initial_context:
            context.update(initial_context)

        return self.store.create_run(
            run_id=actual_run_id,
            flow_name=flow_name,
            current_node="start",
            status=RUNNING,
            context=context,
        )

    def run_until_stop(self, run_id: str, max_steps: int = 100) -> RunRecord:
        state = self.store.get_run(run_id)
        steps = 0

        while state.status == RUNNING and steps < max_steps:
            state = self.step(run_id)
            steps += 1

        return state

    def step(self, run_id: str) -> RunRecord:
        state = self.store.get_run(run_id)

        if state.status in {PAUSED, WAITING_INPUT, DONE}:
            return state

        handler = self.nodes.get(state.current_node)
        if handler is None:
            raise KeyError(f"Unknown node: {state.current_node}")

        self.store.append_event(
            run_id,
            state.current_node,
            "node_started",
            {"status": state.status},
        )

        try:
            result = handler(state)
            next_context = dict(state.context)
            next_context.update(result.context_update)

            updated = self.store.update_run(
                run_id,
                current_node=result.next_node,
                status=result.status,
                context=next_context,
            )
            self.store.append_event(
                run_id,
                state.current_node,
                result.event_type,
                result.event_payload,
            )
            return updated
        except Exception as exc:
            failure_context = dict(state.context)
            failure_context["last_error"] = str(exc)
            updated = self.store.update_run(
                run_id,
                status=FAILED,
                context=failure_context,
            )
            self.store.append_event(
                run_id,
                state.current_node,
                "node_failed",
                {"error": str(exc)},
            )
            return updated

    def pause(self, run_id: str, reason: str = "paused by operator") -> RunRecord:
        current = self.store.get_run(run_id)
        updated = self.store.update_run(run_id, status=PAUSED)
        self.store.append_event(
            run_id,
            current.current_node,
            "paused",
            {"reason": reason},
        )
        return updated

    def resume(self, run_id: str) -> RunRecord:
        current = self.store.get_run(run_id)
        if current.status == DONE:
            return current

        next_status = RUNNING
        updated = self.store.update_run(run_id, status=next_status)
        self.store.append_event(
            run_id,
            current.current_node,
            "resumed",
            {"from_status": current.status},
        )
        return updated

    def submit_input(
        self,
        run_id: str,
        user_input: str,
        *,
        approved: bool = True,
    ) -> RunRecord:
        state = self.store.get_run(run_id)
        next_context = dict(state.context)
        next_context["user_input"] = user_input
        next_context["approved"] = approved

        updated = self.store.update_run(
            run_id,
            current_node="finalize",
            status=RUNNING,
            context=next_context,
        )
        self.store.append_event(
            run_id,
            state.current_node,
            "input_received",
            {"approved": approved},
        )
        return updated

    def node_start(self, state: RunRecord) -> StepResult:
        return StepResult(
            next_node="think",
            status=RUNNING,
            event_payload={"message": "entered flow"},
        )

    def node_think(self, state: RunRecord) -> StepResult:
        task = state.context.get("task", "").strip()
        if not task:
            raise ValueError("context.task is required")

        if self.llm is None:
            llm_output = (
                "LLM client is not configured yet. "
                f"Task to analyze: {task}"
            )
        else:
            result = self.llm.generate(
                system=(
                    "You are a workflow planning assistant. "
                    "Write a concise plan and one follow-up question."
                ),
                prompt=f"User task: {task}",
            )
            llm_output = result.text

        return StepResult(
            next_node="wait_input",
            status=WAITING_INPUT,
            context_update={"llm_output": llm_output},
            event_payload={"preview": llm_output[:200]},
        )

    def node_wait_input(self, state: RunRecord) -> StepResult:
        return StepResult(
            next_node="wait_input",
            status=WAITING_INPUT,
            event_type="waiting_for_input",
            event_payload={"message": "waiting for user approval or clarification"},
        )

    def node_finalize(self, state: RunRecord) -> StepResult:
        llm_output = state.context.get("llm_output", "")
        user_input = state.context.get("user_input", "")
        approved = state.context.get("approved", False)

        if not approved:
            return StepResult(
                next_node="wait_input",
                status=WAITING_INPUT,
                event_type="approval_rejected",
                event_payload={"message": "user rejected the current proposal"},
            )

        final_output = (
            "Workflow complete.\n\n"
            f"Draft:\n{llm_output}\n\n"
            f"User confirmation:\n{user_input}"
        )
        return StepResult(
            next_node="done",
            status=DONE,
            context_update={"final_output": final_output},
            event_payload={"message": "workflow completed"},
        )

    def node_done(self, state: RunRecord) -> StepResult:
        return StepResult(
            next_node="done",
            status=DONE,
            event_payload={"message": "already completed"},
        )


def build_llm_from_args(args: argparse.Namespace) -> LLMClient | None:
    config_values = load_llm_config(args.config)

    provider = args.provider or config_values.get("provider")
    api_key = args.api_key or config_values.get("api_key")
    model = args.model or config_values.get("model")
    base_url = args.base_url or config_values.get("base_url")
    timeout = args.timeout if args.timeout is not None else config_values.get("timeout", 60)
    temperature = (
        args.temperature
        if args.temperature is not None
        else config_values.get("temperature", 0.2)
    )
    max_tokens = (
        args.max_tokens
        if args.max_tokens is not None
        else config_values.get("max_tokens", 2000)
    )

    if not (provider and api_key and model and base_url):
        return None

    config = LLMConfig(
        provider=provider,
        api_key=api_key,
        model=model,
        base_url=base_url,
        timeout=timeout,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return LLMClient(config)


def load_llm_config(config_path: str | None) -> dict[str, Any]:
    if not config_path:
        return {}

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"LLM config file not found: {path}")

    return json.loads(path.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal resumable AI workflow engine")
    parser.add_argument("--db", default="flow.db", help="sqlite db path")
    parser.add_argument("--config", help="path to llm config json")
    parser.add_argument("--provider", choices=["openai", "anthropic"])
    parser.add_argument("--api-key")
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--temperature", type=float)
    parser.add_argument("--max-tokens", type=int)

    subparsers = parser.add_subparsers(dest="command", required=True)

    create_cmd = subparsers.add_parser("create", help="create a new run")
    create_cmd.add_argument("--task", required=True)
    create_cmd.add_argument("--run-id")

    run_cmd = subparsers.add_parser("run", help="run until paused or completed")
    run_cmd.add_argument("--run-id", required=True)
    run_cmd.add_argument("--max-steps", type=int, default=100)

    pause_cmd = subparsers.add_parser("pause", help="pause a run")
    pause_cmd.add_argument("--run-id", required=True)
    pause_cmd.add_argument("--reason", default="paused by operator")

    resume_cmd = subparsers.add_parser("resume", help="resume a run")
    resume_cmd.add_argument("--run-id", required=True)

    input_cmd = subparsers.add_parser("input", help="submit human input")
    input_cmd.add_argument("--run-id", required=True)
    input_cmd.add_argument("--text", required=True)
    input_cmd.add_argument("--approved", action="store_true")

    show_cmd = subparsers.add_parser("show", help="show run state and events")
    show_cmd.add_argument("--run-id", required=True)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    store = SqliteStore(args.db)
    llm = build_llm_from_args(args)
    engine = FlowEngine(store, llm)

    if args.command == "create":
        record = engine.create_run(
            run_id=args.run_id,
            initial_context={"task": args.task},
        )
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "run":
        record = engine.run_until_stop(args.run_id, max_steps=args.max_steps)
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "pause":
        record = engine.pause(args.run_id, reason=args.reason)
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "resume":
        record = engine.resume(args.run_id)
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "input":
        record = engine.submit_input(
            args.run_id,
            args.text,
            approved=args.approved,
        )
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "show":
        record = store.get_run(args.run_id)
        events = store.list_events(args.run_id)
        print(
            json.dumps(
                {
                    "run": record.__dict__,
                    "events": events,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return


if __name__ == "__main__":
    main()
