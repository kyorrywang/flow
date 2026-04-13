from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunRecord:
    run_id: str
    parent_run_id: str | None
    flow_name: str
    current_node: str
    status: str
    context: dict[str, Any]
    version: int
    created_at: str
    updated_at: str


class SqliteStore:
    def __init__(self, db_path: str | Path = "flow.db") -> None:
        self.db_path = Path(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    parent_run_id TEXT,
                    flow_name TEXT NOT NULL,
                    current_node TEXT NOT NULL,
                    status TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (parent_run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    node TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
                CREATE INDEX IF NOT EXISTS idx_runs_parent_run_id ON runs(parent_run_id);
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(runs)").fetchall()
            }
            if "parent_run_id" not in columns:
                conn.execute("ALTER TABLE runs ADD COLUMN parent_run_id TEXT")

    def create_run(
        self,
        run_id: str,
        flow_name: str,
        current_node: str,
        status: str,
        parent_run_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> RunRecord:
        now = utc_now()
        payload = json.dumps(context or {}, ensure_ascii=False)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, parent_run_id, flow_name, current_node, status, context_json,
                    version, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (run_id, parent_run_id, flow_name, current_node, status, payload, now, now),
            )

        self.append_event(run_id, current_node, "run_created", {"status": status})
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> RunRecord:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()

        if row is None:
            raise KeyError(f"Run not found: {run_id}")

        return self._row_to_run(row)

    def update_run(
        self,
        run_id: str,
        *,
        current_node: str | None = None,
        status: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> RunRecord:
        current = self.get_run(run_id)
        next_node = current_node if current_node is not None else current.current_node
        next_status = status if status is not None else current.status
        next_context = context if context is not None else current.context
        updated_at = utc_now()

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET current_node = ?,
                    status = ?,
                    context_json = ?,
                    version = version + 1,
                    updated_at = ?
                WHERE run_id = ?
                """,
                (
                    next_node,
                    next_status,
                    json.dumps(next_context, ensure_ascii=False),
                    updated_at,
                    run_id,
                ),
            )

        return self.get_run(run_id)

    def append_event(
        self,
        run_id: str,
        node: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO events (run_id, node, event_type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    node,
                    event_type,
                    json.dumps(payload or {}, ensure_ascii=False),
                    utc_now(),
                ),
            )

    def get_children(self, parent_run_id: str) -> list[RunRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE parent_run_id = ?",
                (parent_run_id,),
            ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def list_events(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, node, event_type, payload_json, created_at
                FROM events
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()

        return [
            {
                "id": row["id"],
                "run_id": row["run_id"],
                "node": row["node"],
                "event_type": row["event_type"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            parent_run_id=row["parent_run_id"],
            flow_name=row["flow_name"],
            current_node=row["current_node"],
            status=row["status"],
            context=json.loads(row["context_json"]),
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
