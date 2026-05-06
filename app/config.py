"""
Configuration module — loads settings from environment variables.

Distributed Systems Concept:
  Each node in a distributed cluster needs its own identity (NODE_ID),
  a defined role (leader vs follower), and knowledge of its peers.
  Configuration is externalised to environment variables so that the
  same Docker image can be reused for every node with different .env files.
"""

from __future__ import annotations

import os
from typing import List


class Settings:
    """Application-wide settings, read from environment variables."""

    def __init__(self) -> None:
        self.node_id: str = os.getenv("NODE_ID", "node1")
        self.node_role: str = os.getenv("NODE_ROLE", "leader")  # "leader" | "follower"
        self.node_host: str = os.getenv("NODE_HOST", "0.0.0.0")
        self.node_port: int = int(os.getenv("NODE_PORT", "8000"))

        # Comma-separated peer URLs (other nodes in the cluster)
        peers_raw: str = os.getenv("PEERS", "")
        self.peers: List[str] = [p.strip() for p in peers_raw.split(",") if p.strip()]

        # The leader's base URL — followers use this to forward writes
        self.leader_url: str = os.getenv("LEADER_URL", "http://localhost:8000")

        # Directory where WAL and SQLite files are stored
        self.data_dir: str = os.getenv("DATA_DIR", "./data")

    @property
    def is_leader(self) -> bool:
        return self.node_role == "leader"


# Singleton instance used throughout the application
settings = Settings()
