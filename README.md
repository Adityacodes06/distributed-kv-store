# 🗄️ Distributed Key-Value Store (Go + RPC)

A production-quality mini distributed key-value database rewritten in **Go** using Go's built-in **RPC** for internal replication/sync and custom client CLI, alongside a standard **HTTP/REST** API for public compatibility. Demonstrates core distributed systems concepts including leader-based replication, quorum writes, write-ahead logging, fault tolerance, and automatic follower recovery.

---

## 📐 Architecture

```
                          ┌─────────────────────┐
                          │    Go CLI Client    │
                          │  go run client.go … │
                          └─────────┬───────────┘
                                    │
                                   RPC
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
    Client ──PUT (RPC)──► Leader ──WAL──► Replicate to followers (RPC) ──► Quorum ACK ──► Success

  Read Flow:
    Client ──GET (RPC)──► Any Node ──► Local SQLite ──► Value
```

---

## 🧠 Distributed Systems Concepts Used

| Concept | How It's Implemented |
|---|---|
| **Leader-based replication** | All writes are serialised through the leader node |
| **Write-ahead logging (WAL)** | Every mutation is appended to a durable JSONL log before applying to the database |
| **Quorum writes** | Writes succeed only when a majority (2/3) of nodes acknowledge |
| **Write forwarding** | Followers transparently forward writes to the leader via Go RPC |
| **Follower recovery** | On restart, followers catch up missed WAL entries from the leader using Go RPC |
| **Crash recovery** | WAL replay rebuilds SQLite state automatically after a crash or restart |
| **Idempotent replication** | Duplicate log entries are safely ignored by followers |

---

## 🔌 API & RPC Reference

### Public HTTP/REST Endpoints (Backward Compatible)

| Method | Endpoint | Description |
|---|---|---|
| `PUT` | `/kv/{key}` | Store a key-value pair |
| `GET` | `/kv/{key}` | Retrieve value by key |
| `DELETE` | `/kv/{key}` | Delete a key |
| `GET` | `/health` | Node health check |
| `GET` | `/metrics` | Observability metrics |
| `GET` | `/cluster` | Cluster peer status |

### RPC Interface (`KVNode` Service)

The Go client uses `net/rpc` over TCP to invoke the following methods:
- `KVNode.Put(args models.PutArgs, reply *models.PutReply)`
- `KVNode.Get(args models.GetArgs, reply *models.GetReply)`
- `KVNode.Delete(args models.DeleteArgs, reply *models.DeleteReply)`
- `KVNode.Health(args models.HealthArgs, reply *models.HealthReply)`
- `KVNode.Metrics(args models.MetricsArgs, reply *models.MetricsReply)`
- `KVNode.Cluster(args models.ClusterArgs, reply *models.ClusterReply)`
- `KVNode.Replicate(args models.ReplicateArgs, reply *models.ReplicateReply)` (Internal)
- `KVNode.Sync(args models.SyncArgs, reply *models.SyncReply)` (Internal)

---

## 🚀 Quick Start

### Prerequisites

- Go 1.22+
- Docker & Docker Compose (optional)

### Run with Docker Compose (recommended)

```bash
# Build and start the 3-node cluster
docker compose up --build

# The cluster is now running:
#   Leader   → http://localhost:8001 (RPC / HTTP)
#   Follower → http://localhost:8002 (RPC / HTTP)
#   Follower → http://localhost:8003 (RPC / HTTP)
```

### Run Locally (single node, for development)

```bash
# Start a single leader node
NODE_ID=node1 NODE_ROLE=leader NODE_PORT=8000 PEERS="" DATA_DIR=./data \
  go run main.go
```

---

## 📡 API Examples with curl (HTTP)

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

---

## 💻 Go CLI Client (RPC)

We provide a native Go client that communicates with the nodes via Go RPC:

```bash
# Compile the client
go build -o kvclient client/client.go

# Store a value
./kvclient put name Aditya

# Retrieve a value
./kvclient get name

# Delete a value
./kvclient delete name

# Health check
./kvclient health

# Metrics
./kvclient metrics

# Cluster status
./kvclient cluster

# Connect to a specific follower node
./kvclient --url localhost:8002 get name
```

---

## 🧪 Running Tests

Go unit and integration tests cover key-value API, storage engine, crash recovery, and quorum replication:

```bash
# Run all tests
go test -v ./...
```

---

## 💥 Failure Simulation

### Scenario 1: Kill a follower — writes should still succeed

```bash
# Start the cluster
docker compose up --build -d

# Write a value (succeeds with all 3 nodes)
./kvclient --url localhost:8001 put test hello

# Kill node3
docker compose stop node3

# Write another value (still succeeds — quorum is 2/3)
./kvclient --url localhost:8001 put test2 world

# Verify the value exists on node2
./kvclient --url localhost:8002 get test2
```

### Scenario 2: Bring follower back — it should catch up sync

```bash
# Restart node3
docker compose start node3

# Wait a few seconds for sync, then verify
sleep 3
./kvclient --url localhost:8003 get test2
# Should return: {"key":"test2","value":"world","message":"found"}
```

### Scenario 3: Kill both followers — writes should fail

```bash
docker compose stop node2 node3

./kvclient --url localhost:8001 put fail nope
# Should return: ❌ Error: Write failed: could not reach quorum
```

---

## 📁 Project Structure

```
distributed-kv-store/
├── main.go               # Server entry point, configuration & RPC listener
├── client/
│   └── client.go         # Go CLI client using Go RPC
├── app/
│   ├── config/
│   │   └── config.go     # Loads config from environment variables
│   ├── models/
│   │   └── models.go     # Shared RPC & HTTP request/response models
│   ├── storage/
│   │   └── storage.go    # WAL (jsonl) + SQLite engine logic
│   ├── replication/
│   │   └── replication.go # Leader quorum replication over Go RPC
│   └── cluster/
│       └── cluster.go    # Peer health checks (HTTP) & catch-up sync (RPC)
├── tests/
│   ├── kv_api_test.go    # HTTP endpoint unit tests
│   ├── replication_test.go # WAL & storage engine unit tests
│   └── storage_test.go   # Failure & recovery unit tests
├── docker-compose.yml     # 3-node cluster definition
├── Dockerfile             # Multi-stage Docker build for a lightweight Go binary
├── .env.example           # Example environment variables
├── .gitignore
└── README.md              # This file
```
