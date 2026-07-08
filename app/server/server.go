package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"strings"
	"sync/atomic"

	"distributed-kv-store/app/config"
	"distributed-kv-store/app/cluster"
	"distributed-kv-store/app/models"
	"distributed-kv-store/app/replication"
	"distributed-kv-store/app/storage"
)

type KVNode struct {
	Cfg          *config.Config
	Engine       *storage.StorageEngine
	TotalReads   int64
	TotalWrites  int64
	TotalDeletes int64
}

// NewKVNode creates a new KVNode instance
func NewKVNode(cfg *config.Config, engine *storage.StorageEngine) *KVNode {
	return &KVNode{
		Cfg:    cfg,
		Engine: engine,
	}
}

// --- RPC Methods ---

func (n *KVNode) Put(args *models.PutArgs, reply *models.PutReply) error {
	if !n.Cfg.IsLeader() {
		return n.forwardPutRPC(args, reply)
	}

	entry, err := n.Engine.Write("put", args.Key, args.Value)
	if err != nil {
		return fmt.Errorf("storage write failed: %w", err)
	}

	quorumOK, err := replication.ReplicateToCluster(entry, n.Cfg.Peers)
	if err != nil || !quorumOK {
		return errors.New("Write failed: could not reach quorum")
	}

	atomic.AddInt64(&n.TotalWrites, 1)
	reply.Key = args.Key
	reply.Value = args.Value
	reply.Message = "stored"
	return nil
}

func (n *KVNode) Get(args *models.GetArgs, reply *models.GetReply) error {
	atomic.AddInt64(&n.TotalReads, 1)
	value, ok, err := n.Engine.Read(args.Key)
	if err != nil {
		return fmt.Errorf("storage read failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("Key '%s' not found", args.Key)
	}

	reply.Key = args.Key
	reply.Value = value
	reply.Message = "found"
	return nil
}

func (n *KVNode) Delete(args *models.DeleteArgs, reply *models.DeleteReply) error {
	if !n.Cfg.IsLeader() {
		return n.forwardDeleteRPC(args, reply)
	}

	entry, err := n.Engine.Write("delete", args.Key, "")
	if err != nil {
		return fmt.Errorf("storage delete failed: %w", err)
	}

	quorumOK, err := replication.ReplicateToCluster(entry, n.Cfg.Peers)
	if err != nil || !quorumOK {
		return errors.New("Delete failed: could not reach quorum")
	}

	atomic.AddInt64(&n.TotalDeletes, 1)
	reply.Key = args.Key
	reply.Value = nil
	reply.Message = "deleted"
	return nil
}

func (n *KVNode) Health(args *models.HealthArgs, reply *models.HealthReply) error {
	reply.NodeID = n.Cfg.NodeID
	reply.Role = n.Cfg.NodeRole
	reply.Status = "healthy"
	return nil
}

func (n *KVNode) Metrics(args *models.MetricsArgs, reply *models.MetricsReply) error {
	reply.NodeID = n.Cfg.NodeID
	reply.Role = n.Cfg.NodeRole
	reply.TotalReads = int(atomic.LoadInt64(&n.TotalReads))
	reply.TotalWrites = int(atomic.LoadInt64(&n.TotalWrites))
	reply.TotalDeletes = int(atomic.LoadInt64(&n.TotalDeletes))
	reply.ReplicationSuccessCount = int(atomic.LoadInt64(&replication.ReplicationSuccessCount))
	reply.ReplicationFailureCount = int(atomic.LoadInt64(&replication.ReplicationFailureCount))
	reply.LogIndex = n.Engine.LogIndex()
	return nil
}

func (n *KVNode) Cluster(args *models.ClusterArgs, reply *models.ClusterReply) error {
	*reply = models.ClusterReply(cluster.ClusterStatus(n.Cfg.RawPeers))
	return nil
}

func (n *KVNode) Replicate(args *models.ReplicateArgs, reply *models.ReplicateReply) error {
	for _, entry := range args.Entries {
		if err := n.Engine.ApplyReplicatedEntry(entry); err != nil {
			log.Printf("Replication apply failed for index %d: %v", entry.Index, err)
			reply.Status = "error"
			reply.LogIndex = n.Engine.LogIndex()
			return err
		}
	}
	reply.Status = "ok"
	reply.LogIndex = n.Engine.LogIndex()
	return nil
}

func (n *KVNode) Sync(args *models.SyncArgs, reply *models.SyncReply) error {
	reply.Entries = n.Engine.GetWAL().EntriesAfter(args.AfterIndex)
	reply.LeaderLogIndex = n.Engine.LogIndex()
	return nil
}

// --- Forwarding RPC Helpers ---

func (n *KVNode) forwardPutRPC(args *models.PutArgs, reply *models.PutReply) error {
	leaderAddr := n.Cfg.LeaderURL
	leaderAddr = strings.TrimPrefix(leaderAddr, "http://")
	leaderAddr = strings.TrimPrefix(leaderAddr, "https://")

	client, err := rpc.DialHTTP("tcp", leaderAddr)
	if err != nil {
		return fmt.Errorf("Leader unreachable: %w", err)
	}
	defer client.Close()

	return client.Call("KVNode.Put", args, reply)
}

func (n *KVNode) forwardDeleteRPC(args *models.DeleteArgs, reply *models.DeleteReply) error {
	leaderAddr := n.Cfg.LeaderURL
	leaderAddr = strings.TrimPrefix(leaderAddr, "http://")
	leaderAddr = strings.TrimPrefix(leaderAddr, "https://")

	client, err := rpc.DialHTTP("tcp", leaderAddr)
	if err != nil {
		return fmt.Errorf("Leader unreachable: %w", err)
	}
	defer client.Close()

	return client.Call("KVNode.Delete", args, reply)
}

// --- HTTP Handler Wrappers (Public for Testing) ---

func (n *KVNode) HandlePutHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, `{"detail":"Key cannot be empty"}`, http.StatusBadRequest)
		return
	}

	var req struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"detail":"Invalid request body"}`, http.StatusBadRequest)
		return
	}

	args := models.PutArgs{
		Key:   key,
		Value: req.Value,
	}
	var reply models.PutReply

	err := n.Put(&args, &reply)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(err.Error(), "could not reach quorum") {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"detail": err.Error()})
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"detail": err.Error()})
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleGetHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, `{"detail":"Key cannot be empty"}`, http.StatusBadRequest)
		return
	}

	args := models.GetArgs{Key: key}
	var reply models.GetReply

	err := n.Get(&args, &reply)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"detail": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleDeleteHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, `{"detail":"Key cannot be empty"}`, http.StatusBadRequest)
		return
	}

	args := models.DeleteArgs{Key: key}
	var reply models.DeleteReply

	err := n.Delete(&args, &reply)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"detail": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleHealthHTTP(w http.ResponseWriter, r *http.Request) {
	var reply models.HealthReply
	_ = n.Health(&models.HealthArgs{}, &reply)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleMetricsHTTP(w http.ResponseWriter, r *http.Request) {
	var reply models.MetricsReply
	_ = n.Metrics(&models.MetricsArgs{}, &reply)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleClusterHTTP(w http.ResponseWriter, r *http.Request) {
	var reply models.ClusterReply
	_ = n.Cluster(&models.ClusterArgs{}, &reply)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleReplicateHTTP(w http.ResponseWriter, r *http.Request) {
	var req models.ReplicateArgs
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"status":"error","detail":"Invalid request body"}`, http.StatusBadRequest)
		return
	}

	var reply models.ReplicateReply
	err := n.Replicate(&req, &reply)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(reply)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

func (n *KVNode) HandleSyncHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AfterIndex int `json:"after_index"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"detail":"Invalid request body"}`, http.StatusBadRequest)
		return
	}

	args := models.SyncArgs{AfterIndex: req.AfterIndex}
	var reply models.SyncReply
	_ = n.Sync(&args, &reply)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reply)
}

// SetupMux configures the routes on a serve mux
func (n *KVNode) SetupMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Mount default RPC handler
	mux.Handle(rpc.DefaultRPCPath, rpc.DefaultServer)
	mux.Handle(rpc.DefaultDebugPath, rpc.DefaultServer)

	// Register HTTP Routes (Go 1.22 routing)
	mux.HandleFunc("PUT /kv/{key}", n.HandlePutHTTP)
	mux.HandleFunc("GET /kv/{key}", n.HandleGetHTTP)
	mux.HandleFunc("DELETE /kv/{key}", n.HandleDeleteHTTP)
	mux.HandleFunc("GET /health", n.HandleHealthHTTP)
	mux.HandleFunc("GET /metrics", n.HandleMetricsHTTP)
	mux.HandleFunc("GET /cluster", n.HandleClusterHTTP)

	// Internal endpoints for HTTP replication/sync backup
	mux.HandleFunc("POST /internal/replicate", n.HandleReplicateHTTP)
	mux.HandleFunc("POST /internal/sync", n.HandleSyncHTTP)

	return mux
}
