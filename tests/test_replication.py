from __future__ import annotations
import os
import shutil
import tempfile
import pytest
from app.models import LogEntry
from app.storage import StorageEngine, WriteAheadLog


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "data")
    os.makedirs(d, exist_ok=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


class TestWriteAheadLog:

    def test_append_and_read(self, data_dir):
        wal = WriteAheadLog(data_dir)
        entry = wal.append("put", "name", "Aditya")
        assert entry.index == 0
        assert entry.operation == "put"
        assert entry.key == "name"
        assert entry.value == "Aditya"

    def test_sequential_indices(self, data_dir):
        wal = WriteAheadLog(data_dir)
        e1 = wal.append("put", "a", "1")
        e2 = wal.append("put", "b", "2")
        e3 = wal.append("delete", "a")
        assert [e1.index, e2.index, e3.index] == [0, 1, 2]

    def test_last_index_empty(self, data_dir):
        wal = WriteAheadLog(data_dir)
        assert wal.last_index == -1

    def test_entries_after(self, data_dir):
        wal = WriteAheadLog(data_dir)
        wal.append("put", "a", "1")
        wal.append("put", "b", "2")
        wal.append("put", "c", "3")
        entries = wal.entries_after(0)
        assert len(entries) == 2
        assert entries[0].index == 1
        assert entries[1].index == 2

    def test_persistence_across_restarts(self, data_dir):
        wal1 = WriteAheadLog(data_dir)
        wal1.append("put", "x", "42")
        wal1.append("put", "y", "99")
        wal2 = WriteAheadLog(data_dir)
        assert wal2.last_index == 1
        entries = wal2.all_entries()
        assert len(entries) == 2
        assert entries[0].key == "x"
        assert entries[1].key == "y"

    def test_idempotent_append_entry(self, data_dir):
        wal = WriteAheadLog(data_dir)
        entry = LogEntry(index=0, operation="put", key="k", value="v")
        wal.append_entry(entry)
        wal.append_entry(entry)
        assert len(wal.all_entries()) == 1


class TestStorageEngine:

    def test_write_and_read(self, data_dir):
        engine = StorageEngine(data_dir)
        engine.write("put", "color", "blue")
        assert engine.read("color") == "blue"

    def test_delete(self, data_dir):
        engine = StorageEngine(data_dir)
        engine.write("put", "temp", "123")
        engine.write("delete", "temp")
        assert engine.read("temp") is None

    def test_log_index_tracks_writes(self, data_dir):
        engine = StorageEngine(data_dir)
        assert engine.log_index == -1
        engine.write("put", "a", "1")
        assert engine.log_index == 0
        engine.write("put", "b", "2")
        assert engine.log_index == 1

    def test_apply_replicated_entry(self, data_dir):
        engine = StorageEngine(data_dir)
        entry = LogEntry(index=0, operation="put", key="city", value="Mumbai")
        engine.apply_replicated_entry(entry)
        assert engine.read("city") == "Mumbai"
        assert engine.log_index == 0

    def test_crash_recovery(self, data_dir):
        engine1 = StorageEngine(data_dir)
        engine1.write("put", "hero", "Batman")
        engine1.write("put", "villain", "Joker")
        engine1.write("delete", "villain")
        engine2 = StorageEngine(data_dir)
        assert engine2.read("hero") == "Batman"
        assert engine2.read("villain") is None
        assert engine2.log_index == 2
