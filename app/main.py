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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("kvstore")
engine: Optional[StorageEngine] = None
total_reads: int = 0
total_writes: int = 0
total_deletes: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    engine = StorageEngine(settings.data_dir)
    logger.info(
        "Node %s started as %s (port %d)",
        settings.node_id,
        settings.node_role,
        settings.node_port,
    )
    if not settings.is_leader:
        try:
            entries = await sync_from_leader(engine.log_index)
            for raw in entries:
                entry = LogEntry(**raw) if isinstance(raw, dict) else raw
                engine.apply_replicated_entry(entry)
            logger.info(
                "Follower sync complete — log index is now %d", engine.log_index
            )
        except Exception as exc:
            logger.warning("Initial sync from leader failed (will retry): %s", exc)
    yield
    logger.info("Node %s shutting down", settings.node_id)


app = FastAPI(
    title="Distributed Key-Value Store",
    description="A mini distributed KV database with leader-based quorum replication.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.put("/kv/{key}", response_model=KVResponse)
async def put_key(key: str, body: PutRequest):
    global total_writes
    if not settings.is_leader:
        return await _forward_write("PUT", key, body.value)
    entry = engine.write("put", key, body.value)
    quorum_ok = await replicate_to_cluster(entry)
    if not quorum_ok:
        raise HTTPException(
            status_code=503, detail="Write failed: could not reach quorum"
        )
    total_writes += 1
    return KVResponse(key=key, value=body.value, message="stored")


@app.get("/kv/{key}", response_model=KVResponse)
async def get_key(key: str):
    global total_reads
    total_reads += 1
    value = engine.read(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return KVResponse(key=key, value=value, message="found")


@app.delete("/kv/{key}", response_model=KVResponse)
async def delete_key(key: str):
    global total_deletes
    if not settings.is_leader:
        return await _forward_write("DELETE", key)
    entry = engine.write("delete", key)
    quorum_ok = await replicate_to_cluster(entry)
    if not quorum_ok:
        raise HTTPException(
            status_code=503, detail="Delete failed: could not reach quorum"
        )
    total_deletes += 1
    return KVResponse(key=key, value=None, message="deleted")


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        node_id=settings.node_id, role=settings.node_role, status="healthy"
    )


@app.get("/metrics", response_model=MetricsResponse)
async def metrics():
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
    return await cluster_status()


@app.post("/internal/replicate")
async def receive_replication(payload: ReplicateRequest):
    for entry in payload.entries:
        engine.apply_replicated_entry(entry)
    return {"status": "ok", "log_index": engine.log_index}


class SyncRequest(BaseModel):
    after_index: int = -1


@app.post("/internal/sync", response_model=SyncResponse)
async def handle_sync(body: SyncRequest):
    entries = engine.wal.entries_after(body.after_index)
    return SyncResponse(entries=entries, leader_log_index=engine.log_index)


async def _forward_write(method: str, key: str, value: str | None = None):
    try:
        async with httpx.AsyncClient() as client:
            if method == "PUT":
                resp = await client.put(
                    f"{settings.leader_url}/kv/{key}",
                    json={"value": value},
                    timeout=5.0,
                )
            else:
                resp = await client.delete(
                    f"{settings.leader_url}/kv/{key}", timeout=5.0
                )
            if resp.status_code >= 400:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail=f"Leader unreachable: {exc}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app", host=settings.node_host, port=settings.node_port, reload=True
    )
