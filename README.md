# 🗄️ Distributed Key-Value Store

A production-quality mini distributed key-value database built with **Python**, **FastAPI**, and **Docker**. Demonstrates core distributed systems concepts including leader-based replication, quorum writes, write-ahead logging, fault tolerance, and automatic follower recovery.

---

## 📐 Architecture

```
                          ┌─────────────────────┐
                          │      Client / CLI    │
                          │  python client.py …  │
                          └─────────┬───────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             ┌────────────┐  ┌────────────┐  ┌────────────┐
             │   Node 1   │  │   Node 2   │  │   Node 3   │
             │  (LEADER)  │  │ (FOLLOWER) │  │ (FOLLOWER) │
             │  :8001     │  │  :8002     │  │  :8003     │
             └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
                   │               │               │
             ┌─────┴──────┐  ┌─────┴──────┐  ┌─────┴──────┐
             │  WAL + KV  │  │  WAL + KV  │  │  WAL + KV  │
             │  (SQLite)  │  │  (SQLite)  │  │  (SQLite)  │
             └────────────┘  └────────────┘  └────────────┘

  Write Flow:
    Client ──PUT──► Leader ──WAL──► Replicate to followers ──► Quorum ACK ──► 200 OK

  Read Flow:
    Client ──GET──► Any Node ──► Local SQLite ──► 200 OK
```

---

## 🧠 Distributed Systems Concepts Used

| Concept | How It's Implemented |
|---|---|
| **Leader-based replication** | All writes are serialised through the leader node |
| **Write-ahead logging (WAL)** | Every mutation is appended to a durable log before applying to KV |
| **Quorum writes** | Writes succeed only when a majority (2/3) of nodes acknowledge |
| **Write forwarding** | Followers transparently forward writes to the leader |
| **Follower recovery** | On restart, followers sync missed log entries from the leader |
| **Crash recovery** | WAL replay rebuilds in-memory + SQLite state after a crash |
| **Idempotent replication** | Duplicate log entries are safely ignored by followers |

---

## 🔌 API Reference

### Public Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `PUT` | `/kv/{key}` | Store a key-value pair |
| `GET` | `/kv/{key}` | Retrieve value by key |
| `DELETE` | `/kv/{key}` | Delete a key |
| `GET` | `/health` | Node health check |
| `GET` | `/metrics` | Observability metrics |
| `GET` | `/cluster` | Cluster peer status |

### Internal Endpoints (node-to-node)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/internal/replicate` | Receive replicated log entries from leader |
| `POST` | `/internal/sync` | Return WAL entries for follower recovery |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- pip

### Run with Docker Compose (recommended)

```bash
# Build and start the 3-node cluster
docker compose up --build

# The cluster is now running:
#   Leader   → http://localhost:8001
#   Follower → http://localhost:8002
#   Follower → http://localhost:8003
```

### Run Locally (single node, for development)

```bash
# Install dependencies
pip install -r requirements.txt

# Start a single leader node
NODE_ID=node1 NODE_ROLE=leader NODE_PORT=8000 PEERS="" DATA_DIR=./data \
  python -m app.main
```

---

## 📡 API Examples with curl

### Store a value
```bash
curl -X PUT http://localhost:8001/kv/name \
  -H "Content-Type: application/json" \
  -d '{"value": "Aditya"}'
# {"key":"name","value":"Aditya","message":"stored"}
```

### Read a value (from any node)
```bash
curl http://localhost:8001/kv/name
# {"key":"name","value":"Aditya","message":"found"}

# Read from a follower (may be slightly stale)
curl http://localhost:8002/kv/name
# {"key":"name","value":"Aditya","message":"found"}
```

### Delete a value
```bash
curl -X DELETE http://localhost:8001/kv/name
# {"key":"name","value":null,"message":"deleted"}
```

### Health check
```bash
curl http://localhost:8001/health
# {"node_id":"node1","role":"leader","status":"healthy"}
```

### Metrics
```bash
curl http://localhost:8001/metrics
# {
#   "node_id": "node1",
#   "role": "leader",
#   "total_reads": 5,
#   "total_writes": 3,
#   "total_deletes": 1,
#   "replication_success_count": 6,
#   "replication_failure_count": 0,
#   "log_index": 3
# }
```

### Cluster status
```bash
curl http://localhost:8001/cluster
# [
#   {"peer": "http://node2:8000", "status": "healthy", ...},
#   {"peer": "http://node3:8000", "status": "healthy", ...}
# ]
```

---

## 💻 CLI Client

```bash
# Store a value
python client.py put name Aditya

# Retrieve a value
python client.py get name

# Delete a value
python client.py delete name

# Health check
python client.py health

# Metrics
python client.py metrics

# Cluster status
python client.py cluster

# Connect to a specific node
python client.py --url http://localhost:8002 get name
```

---

## 🧪 Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run specific test files
pytest tests/test_kv_api.py -v
pytest tests/test_replication.py -v
pytest tests/test_failure_recovery.py -v
```

### Test Coverage

| Test File | What It Covers |
|---|---|
| `test_kv_api.py` | PUT, GET, DELETE, health, metrics endpoints |
| `test_replication.py` | WAL append, persistence, crash recovery, idempotent replication |
| `test_failure_recovery.py` | Quorum with follower failures, catch-up sync, WAL durability |

---

## 💥 Failure Simulation

### Scenario 1: Kill a follower — writes should still succeed

```bash
# Start the cluster
docker compose up --build -d

# Write a value (succeeds with all 3 nodes)
curl -X PUT http://localhost:8001/kv/test -d '{"value":"hello"}' -H "Content-Type: application/json"

# Kill node3
docker compose stop node3

# Write another value (still succeeds — quorum is 2/3)
curl -X PUT http://localhost:8001/kv/test2 -d '{"value":"world"}' -H "Content-Type: application/json"

# Verify the value exists on node2
curl http://localhost:8002/kv/test2
```

### Scenario 2: Bring follower back — it should sync

```bash
# Restart node3
docker compose start node3

# Wait a few seconds for sync, then verify
sleep 3
curl http://localhost:8003/kv/test2
# Should return: {"key":"test2","value":"world","message":"found"}
```

### Scenario 3: Kill both followers — writes should fail

```bash
docker compose stop node2 node3

curl -X PUT http://localhost:8001/kv/fail -d '{"value":"nope"}' -H "Content-Type: application/json"
# Should return: 503 "Write failed: could not reach quorum"
```

---

## 📊 Observability

Each node exposes a `/metrics` endpoint with:

| Metric | Description |
|---|---|
| `node_id` | Unique identifier for this node |
| `role` | `leader` or `follower` |
| `total_reads` | Number of GET requests served |
| `total_writes` | Number of successful PUT operations |
| `total_deletes` | Number of successful DELETE operations |
| `replication_success_count` | Successful replication RPCs |
| `replication_failure_count` | Failed replication RPCs |
| `log_index` | Latest WAL entry index |

---

## 🔒 Consistency Model

- **Writes**: **Strong consistency** via leader-based quorum replication. A write is only acknowledged after a majority of nodes have persisted it.
- **Reads from leader**: Always consistent (the leader has the latest data).
- **Reads from followers**: May be **slightly stale** if a follower hasn't received the latest replicated entry yet. For strong read consistency, always read from the leader or implement read forwarding.

---

## 📁 Project Structure

```
distributed-kv-store/
├── app/
│   ├── __init__.py        # Package marker
│   ├── main.py            # FastAPI application & all endpoints
│   ├── storage.py         # WAL + SQLite KV engine
│   ├── replication.py     # Leader→follower replication with quorum
│   ├── cluster.py         # Peer health checks & follower sync
│   ├── config.py          # Environment-based configuration
│   └── models.py          # Pydantic request/response models
├── tests/
│   ├── __init__.py
│   ├── test_kv_api.py     # API endpoint tests
│   ├── test_replication.py # WAL & storage engine tests
│   └── test_failure_recovery.py  # Quorum & recovery tests
├── client.py              # CLI client
├── docker-compose.yml     # 3-node cluster definition
├── Dockerfile             # Container image
├── requirements.txt       # Python dependencies
├── .env.example           # Example environment variables
├── .gitignore
└── README.md              # This file
```

---

## 🏗️ How to Push to GitHub

```bash
cd "distributed key value store"

# Initialize git
git init
git add .
git commit -m "feat: distributed key-value store with quorum replication"

# Create a repo on GitHub, then:
git remote add origin https://github.com/<your-username>/distributed-kv-store.git
git branch -M main
git push -u origin main
```

This is a python Project, implementation of this in GoLang will be available in some time.
