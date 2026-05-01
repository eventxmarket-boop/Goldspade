import asyncio
import json
import os
from typing import Any, Dict

import redis.asyncio as redis


REQUIRED_FIELDS = {
    "task_uuid",
    "business_zone_code",
    "egress_node_dsn",
    "target_endpoint",
    "http_context",
    "authorization_token",
    "payload_template",
}


def _green(text: str) -> str:
    return f"\033[92m{text}\033[0m"


async def verify_pipeline() -> Dict[str, Any]:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    client = redis.from_url(redis_url)
    try:
        raw = await client.rpop("list:ready_tasks")
        if raw is None:
            raise RuntimeError("list:ready_tasks is empty")

        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise TypeError("ready task payload must be a JSON object")

        actual_fields = set(payload.keys())
        missing_fields = REQUIRED_FIELDS - actual_fields
        extra_fields = actual_fields - REQUIRED_FIELDS
        assert not missing_fields, f"missing fields: {sorted(missing_fields)}"
        assert not extra_fields, f"unexpected fields: {sorted(extra_fields)}"

        http_context = payload["http_context"]
        assert isinstance(http_context, dict), "http_context must be a JSON object"

        print(
            _green(
                "Pipeline validation successful: Python payload matches Go struct."
            )
        )
        return payload
    finally:
        await client.close()


def main() -> None:
    asyncio.run(verify_pipeline())


if __name__ == "__main__":
    main()
