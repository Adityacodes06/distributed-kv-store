from __future__ import annotations
from typing import Any, Dict, Optional
from pydantic import BaseModel


class PutRequest(BaseModel):
    value: str


class KVResponse(BaseModel):
    key: str
    value: Optional[str] = None
    message: str


class HealthResponse(BaseModel):
    node_id: str
    role: str
    status: str


class MetricsResponse(BaseModel):
    node_id: str
    role: str
    total_reads: int
    total_writes: int
    total_deletes: int
    replication_success_count: int
    replication_failure_count: int
    log_index: int


class LogEntry(BaseModel):
    index: int
    operation: str
    key: str
    value: Optional[str] = None


class ReplicateRequest(BaseModel):
    entries: list[LogEntry]


class SyncResponse(BaseModel):
    entries: list[LogEntry]
    leader_log_index: int
