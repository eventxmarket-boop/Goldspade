import csv
import random
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
CLIENTS_PATH = ROOT_DIR / "clients.csv"
PROXIES_PATH = ROOT_DIR / "proxies.txt"


class DataLoader:
    def __init__(self) -> None:
        self.clients_path = CLIENTS_PATH
        self.proxies_path = PROXIES_PATH
        self._ensure_clients_file()
        self._ensure_proxies_file()

    def _ensure_clients_file(self) -> None:
        if self.clients_path.exists():
            return

        self.clients_path.parent.mkdir(parents=True, exist_ok=True)
        self.clients_path.write_text(
            "task_id,document_id,full_name,procedure_code\n",
            encoding="utf-8",
        )

    def _ensure_proxies_file(self) -> None:
        if self.proxies_path.exists():
            return
        self.proxies_path.parent.mkdir(parents=True, exist_ok=True)
        # 默认写入一个占位符，实战时换成你购买的代理列表
        self.proxies_path.write_text(
            "http://127.0.0.1:8888\n",
            encoding="utf-8",
        )

    def _get_random_proxy(self) -> str:
        try:
            proxies = [
                line.strip() 
                for line in self.proxies_path.read_text(encoding="utf-8").splitlines() 
                if line.strip()
            ]
            return random.choice(proxies) if proxies else ""
        except Exception:
            return ""

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

            # 动态抽取代理，如果 txt 为空则回退到本地测试网关
            proxy_dsn = self._get_random_proxy() or "http://127.0.0.1:8888"

            contexts.append(
                {
                    "business_zone_code": "ZONE_01",
                    "egress_node_dsn": proxy_dsn,
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