package tests

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"testing"

	"distributed-kv-store/app/models"
	"distributed-kv-store/app/replication"
	"distributed-kv-store/app/storage"
)

// MockFollower represents a mock RPC receiver for testing replication
type MockFollower struct {
	mu           sync.Mutex
	shouldFail   bool
	receivedLogs []models.LogEntry
}

func (m *MockFollower) Replicate(args *models.ReplicateArgs, reply *models.ReplicateReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFail {
		reply.Status = "error"
		return errors.New("injected replication error")
	}

	m.receivedLogs = append(m.receivedLogs, args.Entries...)
	reply.Status = "ok"
	reply.LogIndex = 100 // mock index
	return nil
}

func startMockRPCServer(t *testing.T, follower *MockFollower) (string, func()) {
	t.Helper()
	server := rpc.NewServer()
	err := server.RegisterName("KVNode", follower)
	if err != nil {
		t.Fatalf("failed to register mock RPC: %v", err)
	}

	// Use random free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	addr := listener.Addr().String()

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)

	httpServer := &http.Server{
		Handler: mux,
	}

	go func() {
		_ = httpServer.Serve(listener)
	}()

	cleanup := func() {
		_ = httpServer.Close()
		_ = listener.Close()
	}

	return addr, cleanup
}

func TestQuorumReplication_AllFollowersUp(t *testing.T) {
	f1 := &MockFollower{}
	f2 := &MockFollower{}

	addr1, cleanup1 := startMockRPCServer(t, f1)
	defer cleanup1()
	addr2, cleanup2 := startMockRPCServer(t, f2)
	defer cleanup2()

	peers := []string{addr1, addr2}
	entry := models.LogEntry{Index: 0, Operation: "put", Key: "k", Value: "v"}

	success, err := replication.ReplicateToCluster(entry, peers)
	if err != nil {
		t.Fatalf("replication failed: %v", err)
	}
	if !success {
		t.Error("expected quorum to succeed")
	}

	if len(f1.receivedLogs) != 1 || len(f2.receivedLogs) != 1 {
		t.Errorf("expected both followers to receive log, got f1=%d, f2=%d", len(f1.receivedLogs), len(f2.receivedLogs))
	}
}

func TestQuorumReplication_OneFollowerDown(t *testing.T) {
	f1 := &MockFollower{}
	f2 := &MockFollower{shouldFail: true} // Simulates failure response or offline node

	addr1, cleanup1 := startMockRPCServer(t, f1)
	defer cleanup1()
	addr2, cleanup2 := startMockRPCServer(t, f2)
	defer cleanup2()

	peers := []string{addr1, addr2}
	entry := models.LogEntry{Index: 0, Operation: "put", Key: "k", Value: "v"}

	// Quorum is (2 peers + 1 leader)/2 + 1 = 2 nodes.
	// Leader itself counts as 1. We need 1 follower ACK.
	// Since f1 succeeds and f2 fails, we should still reach quorum (1 follower ack + 1 leader ack = 2).
	success, err := replication.ReplicateToCluster(entry, peers)
	if err != nil {
		t.Fatalf("replication unexpectedly returned error: %v", err)
	}
	if !success {
		t.Error("expected quorum to succeed with one follower down")
	}

	if len(f1.receivedLogs) != 1 {
		t.Errorf("expected healthy follower to receive log, got %d", len(f1.receivedLogs))
	}
}

func TestQuorumReplication_AllFollowersDown(t *testing.T) {
	f1 := &MockFollower{shouldFail: true}
	f2 := &MockFollower{shouldFail: true}

	addr1, cleanup1 := startMockRPCServer(t, f1)
	defer cleanup1()
	addr2, cleanup2 := startMockRPCServer(t, f2)
	defer cleanup2()

	peers := []string{addr1, addr2}
	entry := models.LogEntry{Index: 0, Operation: "put", Key: "k", Value: "v"}

	success, err := replication.ReplicateToCluster(entry, peers)
	if err == nil {
		t.Error("expected replication to return error when quorum is not met")
	}
	if success {
		t.Error("expected quorum to fail with all followers down")
	}
}

type SyncNode struct {
	engine *storage.StorageEngine
}

func TestFollowerSync_CatchesUp(t *testing.T) {
	// Test that sync returns missing entries
	leaderDir, err := os.MkdirTemp("", "leader-data")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(leaderDir)

	leaderEngine, err := storage.NewStorageEngine(leaderDir)
	if err != nil {
		t.Fatalf("failed to create leader storage: %v", err)
	}
	defer leaderEngine.Close()

	// Write entries to leader
	for i := 0; i < 5; i++ {
		_, _ = leaderEngine.Write("put", fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// Spin up leader RPC server
	sn := &SyncNode{engine: leaderEngine}
	server := rpc.NewServer()
	// Register name "KVNode" with Sync method
	err = server.RegisterName("KVNode", &struct {
		*SyncNode
	}{sn})
	if err != nil {
		t.Fatalf("failed to register SyncNode: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := listener.Addr().String()

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)
	httpServer := &http.Server{Handler: mux}
	go func() { _ = httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		_ = listener.Close()
	}()

	// Simulate follower calling SyncFromLeader
	// Follower has index 1 (meaning it has index 0 and 1, needs 2, 3, 4)
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	args := models.SyncArgs{AfterIndex: 1}
	var reply models.SyncReply
	err = client.Call("KVNode.Sync", &args, &reply)
	if err != nil {
		t.Fatalf("Sync RPC failed: %v", err)
	}

	if len(reply.Entries) != 3 {
		t.Fatalf("expected 3 missing entries, got %d", len(reply.Entries))
	}
	if reply.Entries[0].Index != 2 || reply.Entries[2].Index != 4 {
		t.Errorf("expected missing entries indices [2, 3, 4], got [%d, ..., %d]", reply.Entries[0].Index, reply.Entries[len(reply.Entries)-1].Index)
	}
	if reply.LeaderLogIndex != 4 {
		t.Errorf("expected leader log index 4, got %d", reply.LeaderLogIndex)
	}
}

// Implement mock Sync method for KVNode structure registered in testing
func (s *SyncNode) Sync(args *models.SyncArgs, reply *models.SyncReply) error {
	reply.Entries = s.engine.GetWAL().EntriesAfter(args.AfterIndex)
	reply.LeaderLogIndex = s.engine.LogIndex()
	return nil
}
