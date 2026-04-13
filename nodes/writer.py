from __future__ import annotations

from pathlib import Path
from typing import Any


class OutputWriter:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        *,
        run_id: str,
        relative_path: str,
        content: str,
        overwrite: bool = True,
    ) -> str:
        target = self.root_dir / run_id / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and not overwrite:
            return str(target.resolve())
        target.write_text(content, encoding="utf-8")
        return str(target.resolve())

    def render_path(self, template: str, context: dict[str, Any]) -> str:
        return template.format_map(SafeFormatDict(context))


class SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
