import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


DB_PATH = Path(__file__).resolve().parents[2] / "shared" / "goldspade.sqlite3"


def build_user_info() -> str:
    payload = {
        "business_zone_code": "ZONE_01",
        "egress_node_dsn": "http://127.0.0.1:8888",
        "target_endpoint": "http://localhost:9090/mock_match",
        "http_context": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Cookie": "session=mock-session-001; route=node-a",
        },
        "payload_template": "action_id=MOCK&symbol={SYMBOL}&side={SIDE}&qty={QTY}&auth_response={TOKEN}",
    }
    return json.dumps(payload, ensure_ascii=False)


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(5):
        rows.append(
            (
                str(uuid4()),
                build_user_info(),
                "pending",
                now,
                now,
            )
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executemany(
            """
            INSERT INTO tasks (task_id, user_info, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    print(f"Seeded {len(rows)} mock tasks into {DB_PATH}")


if __name__ == "__main__":
    main()
