import csv
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
CLIENTS_PATH = ROOT_DIR / "clients.csv"


class DataLoader:
    def __init__(self) -> None:
        self.clients_path = CLIENTS_PATH
        self._ensure_clients_file()

    def _ensure_clients_file(self) -> None:
        if self.clients_path.exists():
            return

        self.clients_path.parent.mkdir(parents=True, exist_ok=True)
        self.clients_path.write_text(
            "task_id,document_id,full_name,procedure_code\n",
            encoding="utf-8",
        )

    def _read_clients(self) -> list[dict[str, str]]:
        self._ensure_clients_file()

        clients: list[dict[str, str]] = []
        with self.clients_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not row:
                    continue
                document_id = (row.get("document_id") or "").strip()
                full_name = (row.get("full_name") or "").strip()
                procedure_code = (row.get("procedure_code") or "").strip()
                if not document_id or not full_name or not procedure_code:
                    continue
                clients.append(
                    {
                        "document_id": document_id,
                        "full_name": full_name,
                        "procedure_code": procedure_code,
                    }
                )
        return clients

    def build_fused_contexts(self, target_url: str) -> list[dict[str, Any]]:
        contexts: list[dict[str, Any]] = []
        clients = self._read_clients()

        for client in clients:
            document_id = client["document_id"]
            full_name = client["full_name"]
            procedure_code = client["procedure_code"]

            contexts.append(
                {
                    "business_zone_code": "ZONE_01",
                    "egress_node_dsn": "http://127.0.0.1:8888",
                    "target_endpoint": target_url,
                    "http_context": {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                        "Cookie": f"session=local-test; client={document_id}",
                    },
                    "payload_template": (
                        f"id={document_id}&name={full_name}&procedure={procedure_code}&token={{TOKEN}}"
                    ),
                }
            )

        return contexts
