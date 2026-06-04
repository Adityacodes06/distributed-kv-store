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


class TestQuorumReplication:

    @pytest.mark.asyncio
    async def test_quorum_succeeds_with_one_follower_down(self, data_dir):
        from app.replication import replicate_to_cluster
        from app.config import settings

        settings.peers = ["http://node2:8000", "http://node3:8000"]
        entry = LogEntry(index=0, operation="put", key="k", value="v")
        with patch("app.replication.replicate_to_follower") as mock_rep:
            mock_rep.side_effect = [True, False]
            result = await replicate_to_cluster(entry)
        assert result is True

    @pytest.mark.asyncio
    async def test_quorum_fails_when_all_followers_down(self, data_dir):
        from app.replication import replicate_to_cluster
        from app.config import settings

        settings.peers = ["http://node2:8000", "http://node3:8000"]
        entry = LogEntry(index=0, operation="put", key="k", value="v")
        with patch("app.replication.replicate_to_follower") as mock_rep:
            mock_rep.side_effect = [False, False]
            result = await replicate_to_cluster(entry)
        assert result is False

    @pytest.mark.asyncio
    async def test_quorum_succeeds_with_all_followers_up(self, data_dir):
        from app.replication import replicate_to_cluster
        from app.config import settings

        settings.peers = ["http://node2:8000", "http://node3:8000"]
        entry = LogEntry(index=0, operation="put", key="k", value="v")
        with patch("app.replication.replicate_to_follower") as mock_rep:
            mock_rep.side_effect = [True, True]
            result = await replicate_to_cluster(entry)
        assert result is True


class TestRecoverySync:

    def test_follower_catches_up(self, data_dir):
        leader_dir = os.path.join(data_dir, "leader")
        leader = StorageEngine(leader_dir)
        for i in range(5):
            leader.write("put", f"key{i}", f"val{i}")
        follower_dir = os.path.join(data_dir, "follower")
        follower = StorageEngine(follower_dir)
        for entry in leader.wal.all_entries()[:2]:
            follower.apply_replicated_entry(entry)
        assert follower.log_index == 1
        missing = leader.wal.entries_after(follower.log_index)
        assert len(missing) == 3
        for entry in missing:
            follower.apply_replicated_entry(entry)
        assert follower.log_index == 4
        assert follower.read("key4") == "val4"

    def test_empty_sync_when_up_to_date(self, data_dir):
        leader_dir = os.path.join(data_dir, "leader")
        leader = StorageEngine(leader_dir)
        leader.write("put", "a", "1")
        missing = leader.wal.entries_after(0)
        assert len(missing) == 0

    def test_full_sync_from_scratch(self, data_dir):
        leader_dir = os.path.join(data_dir, "leader")
        leader = StorageEngine(leader_dir)
        leader.write("put", "x", "1")
        leader.write("put", "y", "2")
        missing = leader.wal.entries_after(-1)
        assert len(missing) == 2


class TestWALDurability:

    def test_wal_survives_engine_restart(self, data_dir):
        engine1 = StorageEngine(data_dir)
        engine1.write("put", "persistent", "data")
        engine2 = StorageEngine(data_dir)
        assert engine2.read("persistent") == "data"
        assert engine2.log_index == 0
