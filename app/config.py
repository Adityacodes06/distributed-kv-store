from __future__ import annotations
import os
from typing import List


class Settings:

    def __init__(self) -> None:
        self.node_id: str = os.getenv("NODE_ID", "node1")
        self.node_role: str = os.getenv("NODE_ROLE", "leader")
        self.node_host: str = os.getenv("NODE_HOST", "0.0.0.0")
        self.node_port: int = int(os.getenv("NODE_PORT", "8000"))
        peers_raw: str = os.getenv("PEERS", "")
        self.peers: List[str] = [p.strip() for p in peers_raw.split(",") if p.strip()]
        self.leader_url: str = os.getenv("LEADER_URL", "http://localhost:8000")
        self.data_dir: str = os.getenv("DATA_DIR", "./data")

    @property
    def is_leader(self) -> bool:
        return self.node_role == "leader"


settings = Settings()
