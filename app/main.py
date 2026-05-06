"""
main.py — FastAPI application for the Distributed Key-Value Store.

This is the entry point for each node.  It exposes:
  • Public API   — PUT / GET / DELETE /kv/{key}, /health, /metrics
  • Internal API — /internal/replicate, /internal/sync

Distributed Systems Concept — API Gateway Pattern:
  Every node exposes the same API surface.  Writes received by a
  follower are *forwarded* to the leader (write forwarding), while
  reads can be served locally for lower latency (at the cost of
  potential staleness on followers).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.models import (
    HealthResponse,
    KVResponse,
    LogEntry,
    MetricsResponse,
    PutRequest,
    ReplicateRequest,
    SyncResponse,
)
from app.replication import (
    replicate_to_cluster,
    replication_failure_count,
    replication_success_count,
)
from app.storage import StorageEngine
from app.cluster import cluster_status, sync_from_leader

# ── Logging setup ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("kvstore")


# ── Application state ──────────────────────────────────────
engine: Optional[StorageEngine] = None

# Simple in-memory counters for observability
total_reads: int = 0
total_writes: int = 0
total_deletes: int = 0


# ── Lifespan (startup / shutdown) ──────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialise the storage engine on startup and optionally
    trigger a follower sync from the leader.
    """
    global engine
    engine = StorageEngine(settings.data_dir)
    logger.info(
        "Node %s started as %s (port %d)",
        settings.node_id,
        settings.node_role,
        settings.node_port,
    )

    # If this node is a follower, attempt to catch up from the leader
    if not settings.is_leader:
        try:
            entries = await sync_from_leader(engine.log_index)
            for raw in entries:
                entry = LogEntry(**raw) if isinstance(raw, dict) else raw
                engine.apply_replicated_entry(entry)
            logger.info("Follower sync complete — log index is now %d", engine.log_index)
        except Exception as exc:
            logger.warning("Initial sync from leader failed (will retry): %s", exc)

    yield  # Application runs here

    logger.info("Node %s shutting down", settings.node_id)


app = FastAPI(
    title="Distributed Key-Value Store",
    description="A mini distributed KV database with leader-based quorum replication.",
    version="1.0.0",
    lifespan=lifespan,
)


# ═══════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════


@app.put("/kv/{key}", response_model=KVResponse)
async def put_key(key: str, body: PutRequest):
    """
    Store a key-value pair.

    • On the **leader**: write to local WAL → replicate to followers
      with quorum → return success.
    • On a **follower**: forward the request to the leader
      (write forwarding).

    Distributed Systems Concept — Write Forwarding:
      Clients can connect to any node.  Followers transparently
      forward writes to the leader so the client doesn't need to
      know which node is the leader.
    """
    global total_writes

    if not settings.is_leader:
        # Forward to leader
        return await _forward_write("PUT", key, body.value)

    # Leader path: WAL → replicate → respond
    entry = engine.write("put", key, body.value)

    quorum_ok = await replicate_to_cluster(entry)
    if not quorum_ok:
        raise HTTPException(
            status_code=503,
            detail="Write failed: could not reach quorum",
        )

    total_writes += 1
    return KVResponse(key=key, value=body.value, message="stored")


@app.get("/kv/{key}", response_model=KVResponse)
async def get_key(key: str):
    """
    Retrieve the value for a given key.

    Reads are served locally (from the node's own SQLite store).
    On followers this may return slightly stale data — see the
    consistency model in the README.
    """
    global total_reads
    total_reads += 1

    value = engine.read(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return KVResponse(key=key, value=value, message="found")


@app.delete("/kv/{key}", response_model=KVResponse)
async def delete_key(key: str):
    """
    Delete a key-value pair.

    Like PUT, deletes on followers are forwarded to the leader.
    """
    global total_deletes

    if not settings.is_leader:
        return await _forward_write("DELETE", key)

    entry = engine.write("delete", key)

    quorum_ok = await replicate_to_cluster(entry)
    if not quorum_ok:
        raise HTTPException(
            status_code=503,
            detail="Delete failed: could not reach quorum",
        )

    total_deletes += 1
    return KVResponse(key=key, value=None, message="deleted")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint — used by Docker and peer nodes."""
    return HealthResponse(
        node_id=settings.node_id,
        role=settings.node_role,
        status="healthy",
    )


@app.get("/metrics", response_model=MetricsResponse)
async def metrics():
    """Observability endpoint exposing key counters."""
    import app.replication as repl

    return MetricsResponse(
        node_id=settings.node_id,
        role=settings.node_role,
        total_reads=total_reads,
        total_writes=total_writes,
        total_deletes=total_deletes,
        replication_success_count=repl.replication_success_count,
        replication_failure_count=repl.replication_failure_count,
        log_index=engine.log_index,
    )


@app.get("/cluster")
async def cluster_info():
    """Return health status of all cluster peers."""
    return await cluster_status()


# ═══════════════════════════════════════════════════════════
#  INTERNAL API  (node-to-node communication)
# ═══════════════════════════════════════════════════════════


@app.post("/internal/replicate")
async def receive_replication(payload: ReplicateRequest):
    """
    Receive replicated log entries from the leader.

    Distributed Systems Concept — Log Shipping:
      The leader sends each new WAL entry to followers.  Followers
      append the entry to their own WAL and apply it to their KV
      store, keeping their state in sync with the leader.
    """
    for entry in payload.entries:
        engine.apply_replicated_entry(entry)
    return {"status": "ok", "log_index": engine.log_index}


class SyncRequest(BaseModel):
    after_index: int = -1


@app.post("/internal/sync", response_model=SyncResponse)
async def handle_sync(body: SyncRequest):
    """
    Return all WAL entries after a given index.

    Called by followers during recovery to catch up on missed writes.
    """
    entries = engine.wal.entries_after(body.after_index)
    return SyncResponse(entries=entries, leader_log_index=engine.log_index)


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════


async def _forward_write(method: str, key: str, value: str | None = None):
    """Forward a write request from a follower to the leader."""
    try:
        async with httpx.AsyncClient() as client:
            if method == "PUT":
                resp = await client.put(
                    f"{settings.leader_url}/kv/{key}",
                    json={"value": value},
                    timeout=5.0,
                )
            else:  # DELETE
                resp = await client.delete(
                    f"{settings.leader_url}/kv/{key}",
                    timeout=5.0,
                )

            if resp.status_code >= 400:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Leader unreachable: {exc}",
        )


# ── Run with `python -m app.main` for local dev ────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.node_host,
        port=settings.node_port,
        reload=True,
    )
