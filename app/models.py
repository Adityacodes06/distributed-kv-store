"""
Pydantic models for API request/response payloads.

These models enforce schema validation at the API boundary, ensuring
that every PUT payload includes a `value` field and every response
has a predictable structure.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel


# ── Request Models ──────────────────────────────────────────

class PutRequest(BaseModel):
    """Body for PUT /kv/{key}."""
    value: str


# ── Response Models ─────────────────────────────────────────

class KVResponse(BaseModel):
    """Standard response for key-value operations."""
    key: str
    value: Optional[str] = None
    message: str


class HealthResponse(BaseModel):
    """Response for GET /health."""
    node_id: str
    role: str
    status: str


class MetricsResponse(BaseModel):
    """Response for GET /metrics — observability data."""
    node_id: str
    role: str
    total_reads: int
    total_writes: int
    total_deletes: int
    replication_success_count: int
    replication_failure_count: int
    log_index: int


# ── Internal Replication Models ─────────────────────────────

class LogEntry(BaseModel):
    """
    A single entry in the write-ahead log.

    Distributed Systems Concept — Write-Ahead Logging (WAL):
      Before any mutation is applied to the key-value store, the
      operation is first persisted to an append-only log.  This
      guarantees durability: even if the process crashes mid-write,
      the log can be replayed to recover state.
    """
    index: int
    operation: str   # "put" | "delete"
    key: str
    value: Optional[str] = None


class ReplicateRequest(BaseModel):
    """Payload sent from leader → followers during replication."""
    entries: list[LogEntry]


class SyncResponse(BaseModel):
    """Response from POST /internal/sync."""
    entries: list[LogEntry]
    leader_log_index: int
