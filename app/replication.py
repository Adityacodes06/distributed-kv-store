"""
Replication module — leader-to-follower log shipping with quorum writes.

Distributed Systems Concepts:

  1. **Leader-based replication**
     All writes are accepted by the leader, which then ships the new
     log entry to every follower.  This serialises writes and avoids
     conflicts.

  2. **Quorum acknowledgement**
     A write is considered *committed* only when a majority of nodes
     (including the leader) acknowledge it.  For a 3-node cluster,
     quorum = 2 (leader + at least 1 follower).

     Formula:  quorum = (N // 2) + 1

  3. **Async fan-out**
     The leader sends replication requests to all followers in
     parallel (using asyncio + httpx) and waits for the quorum
     threshold, not for every follower.  This keeps latency low
     even when one node is slow or down.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Tuple

import httpx

from app.config import settings
from app.models import LogEntry, ReplicateRequest

logger = logging.getLogger("replication")


# ── Metrics counters (updated by the replication layer) ─────
replication_success_count: int = 0
replication_failure_count: int = 0


async def replicate_to_follower(
    peer_url: str,
    entry: LogEntry,
    timeout: float = 2.0,
) -> bool:
    """
    Send a single log entry to one follower.

    Returns True if the follower acknowledged the write.
    """
    global replication_success_count, replication_failure_count

    url = f"{peer_url}/internal/replicate"
    payload = ReplicateRequest(entries=[entry])

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                url,
                json=payload.model_dump(),
                timeout=timeout,
            )
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
    """
    Replicate a log entry to all followers and wait for quorum.

    Quorum = (total_nodes // 2) + 1.  The leader itself counts as
    one acknowledgement, so we need (quorum - 1) follower ACKs.

    Returns True if quorum was reached, False otherwise.

    Distributed Systems Concept — Quorum Writes:
      By requiring a majority of nodes to acknowledge a write before
      reporting success to the client, we ensure that the data
      survives even if a minority of nodes fail.  This is the same
      principle used by Raft, Paxos, and ZooKeeper.
    """
    peers = settings.peers
    total_nodes = len(peers) + 1          # peers + leader
    quorum = (total_nodes // 2) + 1       # majority
    required_follower_acks = quorum - 1   # leader already counts as 1

    if not peers:
        # Single-node cluster — quorum is trivially satisfied
        return True

    # Fan out replication requests in parallel
    tasks = [replicate_to_follower(peer, entry) for peer in peers]
    results: List[bool] = await asyncio.gather(*tasks)

    ack_count = sum(1 for r in results if r)
    logger.info(
        "Quorum check: %d/%d follower ACKs (need %d)",
        ack_count,
        len(peers),
        required_follower_acks,
    )

    return ack_count >= required_follower_acks
