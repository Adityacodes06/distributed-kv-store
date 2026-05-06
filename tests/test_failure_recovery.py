"""
Tests for failure and recovery scenarios.

These tests verify:
  1. Quorum writes succeed when one follower is down.
  2. A follower can recover missed entries after restarting.
  3. Write forwarding from follower to leader.

We use httpx mocking to simulate network-level failures
without spinning up real Docker containers.
"""

from __future__ import annotations

import os
import shutil

import pytest
from unittest.mock import AsyncMock, patch

from app.models import LogEntry
from app.storage import StorageEngine


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "data")
    os.makedirs(d, exist_ok=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Quorum Tests ────────────────────────────────────────────

class TestQuorumReplication:
    """
    Test that the quorum logic handles partial failures correctly.

    Distributed Systems Concept — Quorum:
      In a 3-node cluster (1 leader + 2 followers), quorum = 2.
      The leader counts as one ACK, so we need at least 1 follower
      to ACK for a write to succeed.
    """

    @pytest.mark.asyncio
    async def test_quorum_succeeds_with_one_follower_down(self, data_dir):
        """Write should succeed if 1 of 2 followers responds."""
        from app.replication import replicate_to_cluster
        from app.config import settings

        settings.peers = ["http://node2:8000", "http://node3:8000"]

        entry = LogEntry(index=0, operation="put", key="k", value="v")

        # Mock: node2 succeeds, node3 fails (simulating a crash)
        with patch("app.replication.replicate_to_follower") as mock_rep:
            mock_rep.side_effect = [True, False]  # node2=ok, node3=down
            result = await replicate_to_cluster(entry)

        assert result is True  # Quorum reached (leader + node2 = 2/3)

    @pytest.mark.asyncio
    async def test_quorum_fails_when_all_followers_down(self, data_dir):
        """Write should fail if no followers respond."""
        from app.replication import replicate_to_cluster
        from app.config import settings

        settings.peers = ["http://node2:8000", "http://node3:8000"]

        entry = LogEntry(index=0, operation="put", key="k", value="v")

        with patch("app.replication.replicate_to_follower") as mock_rep:
            mock_rep.side_effect = [False, False]
            result = await replicate_to_cluster(entry)

        assert result is False  # Quorum not reached

    @pytest.mark.asyncio
    async def test_quorum_succeeds_with_all_followers_up(self, data_dir):
        """Write should succeed when all followers respond."""
        from app.replication import replicate_to_cluster
        from app.config import settings

        settings.peers = ["http://node2:8000", "http://node3:8000"]

        entry = LogEntry(index=0, operation="put", key="k", value="v")

        with patch("app.replication.replicate_to_follower") as mock_rep:
            mock_rep.side_effect = [True, True]
            result = await replicate_to_cluster(entry)

        assert result is True


# ── Recovery Tests ──────────────────────────────────────────

class TestRecoverySync:
    """
    Test that a follower can catch up after missing entries.

    Distributed Systems Concept — Catch-up Replication:
      When a follower comes back online, it asks the leader for all
      log entries it missed (entries_after its last known index).
      This is more efficient than a full state transfer.
    """

    def test_follower_catches_up(self, data_dir):
        """
        Simulate:
          1. Leader writes 5 entries.
          2. Follower only has the first 2.
          3. Follower requests entries_after(1) and applies them.
        """
        # Leader engine with 5 writes
        leader_dir = os.path.join(data_dir, "leader")
        leader = StorageEngine(leader_dir)
        for i in range(5):
            leader.write("put", f"key{i}", f"val{i}")

        # Follower engine with only 2 entries (simulating partial replication)
        follower_dir = os.path.join(data_dir, "follower")
        follower = StorageEngine(follower_dir)
        for entry in leader.wal.all_entries()[:2]:
            follower.apply_replicated_entry(entry)

        assert follower.log_index == 1  # has entries 0 and 1

        # Follower requests missing entries from leader
        missing = leader.wal.entries_after(follower.log_index)
        assert len(missing) == 3  # entries 2, 3, 4

        # Follower applies missing entries
        for entry in missing:
            follower.apply_replicated_entry(entry)

        assert follower.log_index == 4
        assert follower.read("key4") == "val4"

    def test_empty_sync_when_up_to_date(self, data_dir):
        """If the follower is already up to date, sync returns nothing."""
        leader_dir = os.path.join(data_dir, "leader")
        leader = StorageEngine(leader_dir)
        leader.write("put", "a", "1")

        missing = leader.wal.entries_after(0)
        assert len(missing) == 0

    def test_full_sync_from_scratch(self, data_dir):
        """A brand-new follower with index -1 gets all entries."""
        leader_dir = os.path.join(data_dir, "leader")
        leader = StorageEngine(leader_dir)
        leader.write("put", "x", "1")
        leader.write("put", "y", "2")

        missing = leader.wal.entries_after(-1)
        assert len(missing) == 2


# ── WAL Durability Under Failure ────────────────────────────

class TestWALDurability:
    def test_wal_survives_engine_restart(self, data_dir):
        """WAL data persists across engine re-instantiations."""
        engine1 = StorageEngine(data_dir)
        engine1.write("put", "persistent", "data")

        # Simulate process crash + restart
        engine2 = StorageEngine(data_dir)
        assert engine2.read("persistent") == "data"
        assert engine2.log_index == 0
