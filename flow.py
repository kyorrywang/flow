from __future__ import annotations

import argparse
import json
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from store import RunRecord, SqliteStore


RUNNING = "running"
PAUSED = "paused"
WAITING_INPUT = "waiting_input"
WAITING_CHILDREN = "waiting_children"
FAILED = "failed"
DONE = "done"


@dataclass
class StepResult:
    next_node: str
    status: str = RUNNING
    context_update: dict[str, Any] = field(default_factory=dict)
    event_type: str = "node_completed"
    event_payload: dict[str, Any] = field(default_factory=dict)


NodeHandler = Callable[[RunRecord], StepResult]


class FlowEngine:
    def __init__(
        self,
        store: SqliteStore,
    ) -> None:
        self.store = store
        self.nodes: dict[str, NodeHandler] = {}

    def register_node(self, name: str, handler: NodeHandler) -> None:
        self.nodes[name] = handler

    def create_run(
        self,
        *,
        flow_name: str,
        initial_context: dict[str, Any] | None = None,
        run_id: str | None = None,
        parent_run_id: str | None = None,
        start_node: str,
    ) -> RunRecord:
        actual_run_id = run_id or str(uuid.uuid4())
        context = dict(initial_context or {})
        return self.store.create_run(
            run_id=actual_run_id,
            parent_run_id=parent_run_id,
            flow_name=flow_name,
            current_node=start_node,
            status=RUNNING,
            context=context,
        )

    def spawn_children(
        self,
        parent_run_id: str,
        *,
        count: int,
        flow_name: str,
        start_node: str,
        context_builder: Callable[[int], dict[str, Any]],
        run_id_prefix: str | None = None,
        ) -> list[RunRecord]:
        children: list[RunRecord] = []
        for index in range(1, count + 1):
            child_run_id = None
            if run_id_prefix:
                child_run_id = f"{run_id_prefix}-{index:03d}"
            child = self.create_run(
                flow_name=flow_name,
                initial_context=context_builder(index),
                run_id=child_run_id,
                parent_run_id=parent_run_id,
                start_node=start_node,
            )
            children.append(child)
            self.store.append_event(
                parent_run_id,
                start_node,
                "child_spawned",
                {"child_run_id": child.run_id, "index": index, "flow_name": flow_name},
            )
        return children

    def run_until_stop(self, run_id: str, max_steps: int = 100) -> RunRecord:
        state = self.store.get_run(run_id)
        steps = 0
        while state.status == RUNNING and steps < max_steps:
            state = self.step(run_id)
            steps += 1
        return state

    def run_tree(self, root_run_id: str, max_steps: int = 1000) -> RunRecord:
        steps = 0
        while steps < max_steps:
            running_ids = []
            queue = [root_run_id]
            while queue:
                curr_id = queue.pop(0)
                record = self.store.get_run(curr_id)
                if record.status == RUNNING:
                    running_ids.append(curr_id)
                children = self.store.get_children(curr_id)
                queue.extend([c.run_id for c in children])
                
            if not running_ids:
                break
                
            for rid in running_ids:
                if steps >= max_steps:
                    break
                self.step(rid)
                steps += 1
                
        return self.store.get_run(root_run_id)

    def step(self, run_id: str) -> RunRecord:
        state = self.store.get_run(run_id)

        if state.status in {PAUSED, WAITING_INPUT, WAITING_CHILDREN, DONE, FAILED}:
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
            if updated.status in {DONE, FAILED} and updated.parent_run_id:
                parent = self.store.get_run(updated.parent_run_id)
                if parent.status == WAITING_CHILDREN:
                    self.resume(parent.run_id)
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
        updated = self.store.update_run(run_id, status=RUNNING)
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
        next_node: str | None = None,
    ) -> RunRecord:
        state = self.store.get_run(run_id)
        context = dict(state.context)
        context["user_input"] = user_input
        context["approved"] = approved

        target_node = next_node
        if target_node is None:
            target_node = context.get("resume_to") if approved else context.get("reject_to")
        if target_node is None:
            target_node = state.current_node
        updated = self.store.update_run(
            run_id,
            current_node=target_node,
            status=RUNNING,
            context=context,
        )
        self.store.append_event(
            run_id,
            state.current_node,
            "input_received",
            {"approved": approved, "next_node": target_node},
        )
        return updated


def load_llm_config() -> dict[str, Any]:
    path = Path("config.json")
    if not path.exists():
        return {}

    return json.loads(path.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tiny template-driven workflow engine")

    subparsers = parser.add_subparsers(dest="command", required=True)

    create_cmd = subparsers.add_parser("create", help="create a new run from a template")
    create_cmd.add_argument("--template", required=True)
    create_cmd.add_argument("--run-id")
    create_cmd.add_argument("--parent-run-id")
    create_cmd.add_argument("--context-json", default="{}")

    run_cmd = subparsers.add_parser("run", help="run until paused or completed")
    run_cmd.add_argument("--run-id", required=True)
    run_cmd.add_argument("--max-steps", type=int, default=1000)
    run_cmd.add_argument("--tree", action="store_true", help="run parent and all child runs automatically")

    input_cmd = subparsers.add_parser("input", help="submit human input")
    input_cmd.add_argument("--run-id", required=True)
    input_cmd.add_argument("--text", required=True)
    input_cmd.add_argument("--approved", action="store_true")
    input_cmd.add_argument("--next-node")

    pause_cmd = subparsers.add_parser("pause", help="pause a run")
    pause_cmd.add_argument("--run-id", required=True)
    pause_cmd.add_argument("--reason", default="paused by operator")

    resume_cmd = subparsers.add_parser("resume", help="resume a run")
    resume_cmd.add_argument("--run-id", required=True)

    show_cmd = subparsers.add_parser("show", help="show run state and events")
    show_cmd.add_argument("--run-id", required=True)

    return parser


def resolve_template_path(template_value: str) -> Path:
    raw = Path(template_value)
    if raw.exists():
        return raw

    candidate = Path("templates") / template_value
    if candidate.suffix == "":
        candidate = candidate.with_suffix(".yaml")
    if candidate.exists():
        return candidate

    raise FileNotFoundError(f"Template not found: {template_value}")


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip()).strip("-").lower()
    return slug or "run"


def get_run_root(run_id: str) -> Path:
    return Path("outputs") / run_id


def get_run_db_path(run_id: str) -> Path:
    parts = run_id.split("-")
    for i in range(len(parts), 0, -1):
        candidate_root = "-".join(parts[:i])
        db_path = Path("outputs") / candidate_root / "run.db"
        if db_path.exists():
            return db_path
    return Path("outputs") / run_id / "run.db"


def build_engine_for_create(template_path: Path, run_id: str) -> tuple[FlowEngine, Any]:
    from template import TemplateRuntime
    from nodes.writer import OutputWriter
    import nodes  # trigger registry

    store = SqliteStore(get_run_db_path(run_id))
    config_values = load_llm_config()
    engine = FlowEngine(store)
    writer = OutputWriter(root_dir="outputs")
    runtime = TemplateRuntime.from_file(template_path, engine=engine, writer=writer, global_config=config_values)
    return engine, runtime


def build_engine_for_existing_run(run_id: str) -> tuple[FlowEngine, Any, RunRecord]:
    from template import TemplateRuntime
    from nodes.writer import OutputWriter
    import nodes  # trigger registry

    db_path = get_run_db_path(run_id)
    store = SqliteStore(db_path)
    record = store.get_run(run_id)
    template_path = Path(record.context["template_path"])
    config_values = load_llm_config()
    engine = FlowEngine(store)
    writer = OutputWriter(root_dir="outputs")
    runtime = TemplateRuntime.from_file(template_path, engine=engine, writer=writer, global_config=config_values)
    return engine, runtime, record


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "create":
        from template import load_template_definition

        template_path = resolve_template_path(args.template)
        definition = load_template_definition(template_path)
        run_id = args.run_id or f"{slugify(definition.name)}-{uuid.uuid4().hex[:8]}"
        engine, runtime = build_engine_for_create(template_path, run_id)
        context = json.loads(args.context_json)
        context["template_path"] = str(template_path)
        record = engine.create_run(
            flow_name=runtime.definition.name,
            run_id=run_id,
            parent_run_id=args.parent_run_id,
            initial_context=context,
            start_node=runtime.definition.start_node,
        )
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "run":
        engine, _runtime, _record = build_engine_for_existing_run(args.run_id)
        if args.tree:
            record = engine.run_tree(args.run_id, max_steps=args.max_steps)
        else:
            record = engine.run_until_stop(args.run_id, max_steps=args.max_steps)
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "input":
        engine, _runtime, _record = build_engine_for_existing_run(args.run_id)
        record = engine.submit_input(
            args.run_id,
            args.text,
            approved=args.approved,
            next_node=args.next_node,
        )
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    store = SqliteStore(get_run_db_path(args.run_id))
    engine = FlowEngine(store)

    if args.command == "pause":
        record = engine.pause(args.run_id, reason=args.reason)
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "resume":
        record = engine.resume(args.run_id)
        print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
        return

    if args.command == "show":
        record = store.get_run(args.run_id)
        events = store.list_events(args.run_id)
        print(
            json.dumps(
                {"run": record.__dict__, "events": events},
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
