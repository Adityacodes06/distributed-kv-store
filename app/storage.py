"""
Storage engine — append-only Write-Ahead Log (WAL) + SQLite KV store.

Architecture:
  ┌────────────┐         ┌────────────┐
  │  WAL File  │ ──────► │  SQLite DB │
  │ (durability│         │ (fast reads│
  │  + replay) │         │  by key)   │
  └────────────┘         └────────────┘

Distributed Systems Concept — Write-Ahead Logging:
  Every mutation (put/delete) is first appended to a sequential log
  file *before* being applied to the SQLite store.  This provides:

  1. **Durability** — the log survives crashes.
  2. **Replication** — followers replay the same log entries.
  3. **Recovery** — a restarting node replays the WAL to rebuild state.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Dict, List, Optional

from app.models import LogEntry


class WriteAheadLog:
    """
    Append-only log stored as a newline-delimited JSON file.

    Each line is a JSON-encoded `LogEntry`.  The log is the *source
    of truth* for replication — the leader ships log entries to
    followers, and followers append + apply them in order.
    """

    def __init__(self, data_dir: str) -> None:
        os.makedirs(data_dir, exist_ok=True)
        self._path = os.path.join(data_dir, "wal.jsonl")
        self._lock = threading.Lock()
        self._entries: List[LogEntry] = []
        self._load()

    # ── Persistence ─────────────────────────────────────────

    def _load(self) -> None:
        """Replay the on-disk WAL into memory on startup."""
        if not os.path.exists(self._path):
            return
        with open(self._path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    self._entries.append(LogEntry(**json.loads(line)))

    def _persist(self, entry: LogEntry) -> None:
        """Append a single entry to the on-disk log file."""
        with open(self._path, "a") as f:
            f.write(entry.model_dump_json() + "\n")

    # ── Public API ──────────────────────────────────────────

    def _last_index_unsafe(self) -> int:
        """Return last index WITHOUT acquiring the lock (caller must hold it)."""
        return self._entries[-1].index if self._entries else -1

    @property
    def last_index(self) -> int:
        """Return the index of the latest log entry, or -1 if empty."""
        with self._lock:
            return self._last_index_unsafe()

    def append(self, operation: str, key: str, value: Optional[str] = None) -> LogEntry:
        """Create a new log entry, persist it, and return it."""
        with self._lock:
            index = len(self._entries)
            entry = LogEntry(index=index, operation=operation, key=key, value=value)
            self._persist(entry)
            self._entries.append(entry)
            return entry

    def append_entry(self, entry: LogEntry) -> None:
        """
        Append a pre-built entry (used by followers receiving replicated data).
        Skips entries that have already been applied (idempotent).
        """
        with self._lock:
            if entry.index <= self._last_index_unsafe():
                return  # Already applied — idempotent
            self._persist(entry)
            self._entries.append(entry)

    def entries_after(self, after_index: int) -> List[LogEntry]:
        """Return all entries with index > after_index (used for sync)."""
        with self._lock:
            return [e for e in self._entries if e.index > after_index]

    def all_entries(self) -> List[LogEntry]:
        """Return a copy of all log entries."""
        with self._lock:
            return list(self._entries)


class KVStore:
    """
    SQLite-backed key-value store.

    Provides fast O(1) reads by key.  State is derived by applying
    WAL entries in order — the SQLite DB is essentially a
    *materialised view* of the append-only log.
    """

    def __init__(self, data_dir: str) -> None:
        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, "kv.db")
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._create_table()

    def _create_table(self) -> None:
        with self._conn:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)"
            )

    # ── CRUD ────────────────────────────────────────────────

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM kv WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else None

    def put(self, key: str, value: str) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
                    (key, value),
                )

    def delete(self, key: str) -> bool:
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))
                return cursor.rowcount > 0

    def apply_entry(self, entry: LogEntry) -> None:
        """Apply a single WAL entry to the KV store."""
        if entry.operation == "put":
            self.put(entry.key, entry.value or "")
        elif entry.operation == "delete":
            self.delete(entry.key)


class StorageEngine:
    """
    Unified facade combining WAL + KV store.

    Usage:
      engine = StorageEngine("./data")
      entry = engine.write("put", "name", "Aditya")   # WAL + KV
      value = engine.read("name")                       # KV only
    """

    def __init__(self, data_dir: str) -> None:
        self.wal = WriteAheadLog(data_dir)
        self.kv = KVStore(data_dir)

        # On startup, replay WAL to rebuild the KV store
        # (handles crash recovery — the WAL is the source of truth)
        self._replay_wal()

    def _replay_wal(self) -> None:
        """Rebuild the KV store from the WAL (crash recovery)."""
        for entry in self.wal.all_entries():
            self.kv.apply_entry(entry)

    # ── Public helpers ──────────────────────────────────────

    def write(self, operation: str, key: str, value: Optional[str] = None) -> LogEntry:
        """Append to WAL then apply to KV store (leader path)."""
        entry = self.wal.append(operation, key, value)
        self.kv.apply_entry(entry)
        return entry

    def apply_replicated_entry(self, entry: LogEntry) -> None:
        """Apply a replicated entry from the leader (follower path)."""
        self.wal.append_entry(entry)
        self.kv.apply_entry(entry)

    def read(self, key: str) -> Optional[str]:
        return self.kv.get(key)

    @property
    def log_index(self) -> int:
        return self.wal.last_index
