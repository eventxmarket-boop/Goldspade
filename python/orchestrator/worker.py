import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import aiohttp


RULES_PATH = Path(__file__).resolve().parents[2] / "rules.json"
DB_PATH = Path(__file__).resolve().parents[2] / "shared" / "goldspade.sqlite3"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "shared" / "schema.sql"


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
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            conn.commit()
        finally:
            conn.close()


class CognitiveTaskClient:
    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
        poll_interval_seconds: float = 3.0,
        max_wait_seconds: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.poll_interval_seconds = poll_interval_seconds
        self.max_wait_seconds = max_wait_seconds

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def submit_work_order(
        self,
        title: str,
        description: str,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = {
            "title": title,
            "description": description,
            "metadata": metadata or {},
        }

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        last_error: Exception | None = None

        async with aiohttp.ClientSession(timeout=timeout, headers=self._headers()) as session:
            for attempt in range(1, self.max_retries + 1):
                try:
                    async with session.post(self.base_url, json=payload) as resp:
                        body_text = await resp.text()
                        if resp.status >= 500:
                            raise RuntimeError(
                                f"ticket service returned {resp.status}: {body_text[:200]}"
                            )
                        if resp.status >= 400:
                            raise ValueError(
                                f"ticket request rejected with {resp.status}: {body_text[:200]}"
                            )
                        return await resp.json()
                except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    last_error = exc
                    if attempt == self.max_retries:
                        break
                    await asyncio.sleep(min(2 ** (attempt - 1), 5))

        raise RuntimeError("failed to submit work order") from last_error

    async def poll_work_order_result(self, ticket_id: str) -> Any:
        status_url = f"{self.base_url}/{ticket_id}/status"
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        deadline = asyncio.get_running_loop().time() + self.max_wait_seconds

        async with aiohttp.ClientSession(timeout=timeout, headers=self._headers()) as session:
            while True:
                now = asyncio.get_running_loop().time()
                if now >= deadline:
                    raise TimeoutError(
                        f"work order {ticket_id} did not complete within {self.max_wait_seconds} seconds"
                    )

                try:
                    async with session.get(status_url) as resp:
                        body_text = await resp.text()
                        if resp.status >= 500:
                            raise RuntimeError(
                                f"status endpoint returned {resp.status}: {body_text[:200]}"
                            )
                        if resp.status >= 400:
                            raise ValueError(
                                f"status request rejected with {resp.status}: {body_text[:200]}"
                            )
                        payload = await resp.json()
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    raise RuntimeError(f"failed to poll work order {ticket_id}") from exc

                status = payload.get("status")
                if status == "processing":
                    remaining = deadline - asyncio.get_running_loop().time()
                    await asyncio.sleep(min(self.poll_interval_seconds, max(remaining, 0)))
                    continue

                if status == "completed":
                    resolution_data = payload.get("resolution_data")
                    if resolution_data is None:
                        raise RuntimeError(
                            f"resolution_data missing for completed ticket {ticket_id}"
                        )
                    return resolution_data

                raise RuntimeError(
                    f"unexpected work order status for {ticket_id}: {status!r}"
                )

    async def execute_task(
        self,
        title: str,
        description: str,
        metadata: Dict[str, Any] | None = None,
    ) -> Any:
        submit_result = await self.submit_work_order(title, description, metadata)
        ticket_id = submit_result.get("ticket_id")
        if not ticket_id:
            raise RuntimeError("ticket_id missing from submission response")
        return await self.poll_work_order_result(ticket_id)


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
