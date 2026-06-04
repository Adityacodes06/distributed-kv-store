from __future__ import annotations
import os
import shutil
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _setup_env(tmp_path):
    data_dir = str(tmp_path / "data")
    os.environ["NODE_ID"] = "test-node"
    os.environ["NODE_ROLE"] = "leader"
    os.environ["NODE_HOST"] = "0.0.0.0"
    os.environ["NODE_PORT"] = "8000"
    os.environ["PEERS"] = ""
    os.environ["LEADER_URL"] = "http://localhost:8000"
    os.environ["DATA_DIR"] = data_dir
    import importlib
    import app.config

    importlib.reload(app.config)
    import app.replication

    importlib.reload(app.replication)
    import app.main as main_mod

    importlib.reload(main_mod)
    from app.storage import StorageEngine
    from app.config import settings

    main_mod.engine = StorageEngine(settings.data_dir)
    main_mod.total_reads = 0
    main_mod.total_writes = 0
    main_mod.total_deletes = 0
    yield
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


@pytest.fixture
def client():
    from app.main import app

    return TestClient(app, raise_server_exceptions=True)


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["node_id"] == "test-node"
    assert data["role"] == "leader"


def test_metrics_initial(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_reads"] == 0
    assert data["total_writes"] == 0
    assert data["log_index"] == -1


def test_put_and_get(client):
    resp = client.put("/kv/name", json={"value": "Aditya"})
    assert resp.status_code == 200
    assert resp.json()["message"] == "stored"
    resp = client.get("/kv/name")
    assert resp.status_code == 200
    assert resp.json()["value"] == "Aditya"


def test_get_missing_key(client):
    resp = client.get("/kv/nonexistent")
    assert resp.status_code == 404


def test_put_overwrite(client):
    client.put("/kv/color", json={"value": "red"})
    client.put("/kv/color", json={"value": "blue"})
    resp = client.get("/kv/color")
    assert resp.json()["value"] == "blue"


def test_delete(client):
    client.put("/kv/temp", json={"value": "123"})
    resp = client.delete("/kv/temp")
    assert resp.status_code == 200
    assert resp.json()["message"] == "deleted"
    resp = client.get("/kv/temp")
    assert resp.status_code == 404


def test_delete_missing_key(client):
    resp = client.delete("/kv/ghost")
    assert resp.status_code == 200


def test_metrics_after_operations(client):
    client.put("/kv/a", json={"value": "1"})
    client.put("/kv/b", json={"value": "2"})
    client.get("/kv/a")
    client.delete("/kv/b")
    resp = client.get("/metrics")
    data = resp.json()
    assert data["total_writes"] == 2
    assert data["total_reads"] >= 1
    assert data["total_deletes"] == 1
    assert data["log_index"] >= 2
