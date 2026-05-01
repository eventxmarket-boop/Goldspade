import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import logging
from uuid import uuid4

import aiohttp
import redis.asyncio as redis


log = logging.getLogger(__name__)

RULES_PATH = Path(__file__).resolve().parents[2] / "rules.json"
DB_PATH = Path(__file__).resolve().parents[2] / "shared" / "goldspade.sqlite3"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "shared" / "schema.sql"


@dataclass
class RulesState:
    raw: Dict[str, Any]
    mtime: float


class RuleEngine:
    def __init__(self, rules_path: Path, repo: "TaskRepository") -> None:
        self.rules_path = rules_path
        self.repo = repo
        self.state: RulesState | None = None
        self._last_trigger_at = 0.0
        self._file_mtime: float | None = None

    def load(self) -> RulesState:
        data = json.loads(self.rules_path.read_text(encoding="utf-8"))
        return RulesState(raw=data, mtime=self.rules_path.stat().st_mtime)

    def _reload_if_needed(self) -> None:
        try:
            current_mtime = self.rules_path.stat().st_mtime
        except FileNotFoundError:
            return

        if self.state is None or self._file_mtime is None or current_mtime > self._file_mtime:
            self.state = self.load()
            self._file_mtime = self.state.mtime

    def _should_trigger(self) -> bool:
        return (time.time() - self._last_trigger_at) >= 60

    def _insert_mock_task(self) -> None:
        if self.state is None:
            return

        mock_user_info = self.state.raw.get("mock_user_info")
        if not isinstance(mock_user_info, dict):
            log.warning("mock_user_info missing or invalid in rules.json; skipping trigger")
            return

        task_id = self.repo.insert_task(json.dumps(mock_user_info, ensure_ascii=False))
        self._last_trigger_at = time.time()
        log.info("Inserted triggered mock task %s", task_id)

    async def watch(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                self._reload_if_needed()
                if self.state is None:
                    await asyncio.sleep(5)
                    continue

                rules = self.state.raw
                monitor_url = rules.get("monitor_url")
                trigger_keyword = rules.get("trigger_keyword")
                poll_interval = float(rules.get("poll_interval", 5))

                if not monitor_url or not trigger_keyword:
                    await asyncio.sleep(poll_interval)
                    continue

                try:
                    async with session.get(str(monitor_url)) as resp:
                        body_text = await resp.text()
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    log.warning("market monitor request failed: %s", exc)
                    await asyncio.sleep(poll_interval)
                    continue

                if trigger_keyword in body_text and self._should_trigger():
                    self._insert_mock_task()

                await asyncio.sleep(poll_interval)


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

    def insert_task(self, user_info: str) -> str:
        task_id = str(uuid4())

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO tasks (task_id, user_info, status, created_at, updated_at)
                VALUES (?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (task_id, user_info),
            )
            conn.commit()
            return task_id
        finally:
            conn.close()

    def fetch_pending_task(self) -> Dict[str, Any] | None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT task_id, user_info
                FROM tasks
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                conn.rollback()
                return None

            conn.execute(
                """
                UPDATE tasks
                SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE task_id = ?
                """,
                (row["task_id"],),
            )
            conn.commit()
            return {"task_id": row["task_id"], "user_info": row["user_info"]}
        finally:
            conn.close()

    def update_task_status(self, task_id: str, status: str) -> None:
        if status not in {"success", "failed"}:
            raise ValueError(f"unsupported task status: {status!r}")

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE tasks
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE task_id = ?
                """,
                (status, task_id),
            )
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


async def process_task_queue(
    repo: TaskRepository,
    client: CognitiveTaskClient,
    redis_client: redis.Redis,
) -> None:
    while True:
        task = repo.fetch_pending_task()
        if task is None:
            await asyncio.sleep(5)
            continue

        task_id = task["task_id"]
        user_info = task["user_info"]
        try:
            resolution_data = await client.execute_task(
                title=f"Process task {task_id}",
                description="Auto-submitted by the workflow orchestrator",
                metadata={"task_id": task_id, "user_info": user_info},
            )
            fused_context = json.loads(user_info)
            fused_context["task_uuid"] = task_id
            fused_context["authorization_token"] = resolution_data
            await redis_client.lpush("list:ready_tasks", json.dumps(fused_context))
        except (RuntimeError, TimeoutError):
            repo.update_task_status(task_id, "failed")
        except (json.JSONDecodeError, TypeError, ValueError, redis.RedisError) as exc:
            log.error("❌ [Task %s] Context fusion or Redis push failed: %s", task_id, exc)
            repo.update_task_status(task_id, "failed")


async def main() -> None:
    repo = TaskRepository(DB_PATH)
    repo.ensure_schema()
    engine = RuleEngine(RULES_PATH, repo)
    engine.state = engine.load()

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    redis_client = redis.from_url(redis_url)
    try:
        await redis_client.ping()
    except redis.RedisError as exc:
        raise RuntimeError(f"failed to connect to Redis: {exc}") from exc

    client = CognitiveTaskClient(
        base_url=os.environ.get("WORK_ORDER_BASE_URL", "http://localhost:8080/work-orders"),
        api_key=os.environ.get("WORK_ORDER_API_KEY"),
    )
    watcher = asyncio.create_task(engine.watch())
    worker_task = asyncio.create_task(process_task_queue(repo, client, redis_client))
    try:
        await asyncio.gather(watcher, worker_task)
    finally:
        watcher.cancel()
        worker_task.cancel()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
