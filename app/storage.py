from __future__ import annotations
import json
import os
import sqlite3
import threading
from typing import Dict, List, Optional
from app.models import LogEntry


class WriteAheadLog:

    def __init__(self, data_dir: str) -> None:
        os.makedirs(data_dir, exist_ok=True)
        self._path = os.path.join(data_dir, "wal.jsonl")
        self._lock = threading.Lock()
        self._entries: List[LogEntry] = []
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    self._entries.append(LogEntry(**json.loads(line)))

    def _persist(self, entry: LogEntry) -> None:
        with open(self._path, "a") as f:
            f.write(entry.model_dump_json() + "\n")

    def _last_index_unsafe(self) -> int:
        return self._entries[-1].index if self._entries else -1

    @property
    def last_index(self) -> int:
        with self._lock:
            return self._last_index_unsafe()

    def append(self, operation: str, key: str, value: Optional[str] = None) -> LogEntry:
        with self._lock:
            index = len(self._entries)
            entry = LogEntry(index=index, operation=operation, key=key, value=value)
            self._persist(entry)
            self._entries.append(entry)
            return entry

    def append_entry(self, entry: LogEntry) -> None:
        with self._lock:
            if entry.index <= self._last_index_unsafe():
                return
            self._persist(entry)
            self._entries.append(entry)

    def entries_after(self, after_index: int) -> List[LogEntry]:
        with self._lock:
            return [e for e in self._entries if e.index > after_index]

    def all_entries(self) -> List[LogEntry]:
        with self._lock:
            return list(self._entries)


class KVStore:

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
                    "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", (key, value)
                )

    def delete(self, key: str) -> bool:
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))
                return cursor.rowcount > 0

    def apply_entry(self, entry: LogEntry) -> None:
        if entry.operation == "put":
            self.put(entry.key, entry.value or "")
        elif entry.operation == "delete":
            self.delete(entry.key)


class StorageEngine:

    def __init__(self, data_dir: str) -> None:
        self.wal = WriteAheadLog(data_dir)
        self.kv = KVStore(data_dir)
        self._replay_wal()

    def _replay_wal(self) -> None:
        for entry in self.wal.all_entries():
            self.kv.apply_entry(entry)

    def write(self, operation: str, key: str, value: Optional[str] = None) -> LogEntry:
        entry = self.wal.append(operation, key, value)
        self.kv.apply_entry(entry)
        return entry

    def apply_replicated_entry(self, entry: LogEntry) -> None:
        self.wal.append_entry(entry)
        self.kv.apply_entry(entry)

    def read(self, key: str) -> Optional[str]:
        return self.kv.get(key)

    @property
    def log_index(self) -> int:
        return self.wal.last_index
