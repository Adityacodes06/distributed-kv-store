from __future__ import annotations
import asyncio
import logging
from typing import List, Tuple
import httpx
from app.config import settings
from app.models import LogEntry, ReplicateRequest

logger = logging.getLogger("replication")
replication_success_count: int = 0
replication_failure_count: int = 0


async def replicate_to_follower(
    peer_url: str, entry: LogEntry, timeout: float = 2.0
) -> bool:
    global replication_success_count, replication_failure_count
    url = f"{peer_url}/internal/replicate"
    payload = ReplicateRequest(entries=[entry])
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload.model_dump(), timeout=timeout)
            if resp.status_code == 200:
                replication_success_count += 1
                logger.info("Replicated index %d to %s", entry.index, peer_url)
                return True
            else:
                replication_failure_count += 1
                logger.warning(
                    "Replication to %s returned %d", peer_url, resp.status_code
                )
                return False
    except Exception as exc:
        replication_failure_count += 1
        logger.warning("Replication to %s failed: %s", peer_url, exc)
        return False


async def replicate_to_cluster(entry: LogEntry) -> bool:
    peers = settings.peers
    total_nodes = len(peers) + 1
    quorum = total_nodes // 2 + 1
    required_follower_acks = quorum - 1
    if not peers:
        return True
    tasks = [replicate_to_follower(peer, entry) for peer in peers]
    results: List[bool] = await asyncio.gather(*tasks)
    ack_count = sum((1 for r in results if r))
    logger.info(
        "Quorum check: %d/%d follower ACKs (need %d)",
        ack_count,
        len(peers),
        required_follower_acks,
    )
    return ack_count >= required_follower_acks
