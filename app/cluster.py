from __future__ import annotations
import logging
from typing import Dict, List
import httpx
from app.config import settings

logger = logging.getLogger("cluster")


async def check_peer_health(peer_url: str, timeout: float = 1.5) -> Dict:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{peer_url}/health", timeout=timeout)
            data = resp.json()
            return {"peer": peer_url, "status": "healthy", "detail": data}
    except Exception as exc:
        logger.warning("Peer %s is unreachable: %s", peer_url, exc)
        return {"peer": peer_url, "status": "unreachable", "detail": str(exc)}


async def cluster_status() -> List[Dict]:
    import asyncio

    tasks = [check_peer_health(p) for p in settings.peers]
    return list(await asyncio.gather(*tasks))


async def sync_from_leader(last_index: int) -> list:
    url = f"{settings.leader_url}/internal/sync"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json={"after_index": last_index}, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                logger.info(
                    "Sync: received %d entries from leader", len(data["entries"])
                )
                return data["entries"]
            else:
                logger.error("Sync failed with status %d", resp.status_code)
                return []
    except Exception as exc:
        logger.error("Sync from leader failed: %s", exc)
        return []
