import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


RULES_PATH = Path(__file__).resolve().parents[2] / "rules.json"
DB_PATH = Path(__file__).resolve().parents[2] / "shared" / "goldspade.sqlite3"


@dataclass
class RulesState:
    raw: Dict[str, Any]
    mtime: float


class RuleEngine:
    def __init__(self, rules_path: Path) -> None:
        self.rules_path = rules_path
        self.state: RulesState | None = None

    def load(self) -> RulesState:
        data = json.loads(self.rules_path.read_text(encoding="utf-8"))
        return RulesState(raw=data, mtime=self.rules_path.stat().st_mtime)

    async def watch(self) -> None:
        while True:
            await asyncio.sleep(5)
            try:
                current_mtime = self.rules_path.stat().st_mtime
            except FileNotFoundError:
                continue
            if self.state is None or current_mtime > self.state.mtime:
                self.state = self.load()


class TaskRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def ensure_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    user_info TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.commit()
        finally:
            conn.close()


async def main() -> None:
    engine = RuleEngine(RULES_PATH)
    engine.state = engine.load()
    repo = TaskRepository(DB_PATH)
    repo.ensure_schema()

    watcher = asyncio.create_task(engine.watch())
    try:
        await watcher
    finally:
        watcher.cancel()


if __name__ == "__main__":
    asyncio.run(main())
