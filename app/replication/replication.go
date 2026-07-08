package replication

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv-store/app/models"
)

var (
	ReplicationSuccessCount int64
	ReplicationFailureCount int64

	clientsMu sync.Mutex
	clients   = make(map[string]*rpc.Client)
)

// getRPCClient gets a cached RPC client or dials a new connection with timeout
func getRPCClient(peer string) (*rpc.Client, error) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	if client, ok := clients[peer]; ok {
		return client, nil
	}

	// Dial with a 2-second timeout
	conn, err := net.DialTimeout("tcp", peer, 2*time.Second)
	if err != nil {
		return nil, err
	}

	// Perform HTTP CONNECT handshake for Go net/rpc
	_, err = io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
	if err != nil {
		conn.Close()
		return nil, err
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil {
		conn.Close()
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		client := rpc.NewClient(conn)
		clients[peer] = client
		return client, nil
	}

	conn.Close()
	return nil, fmt.Errorf("unexpected status during RPC handshake: %s", resp.Status)
}

// markClientFailed clears a failed client from the cache so we redial next time
func markClientFailed(peer string) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	if client, exists := clients[peer]; exists {
		client.Close()
		delete(clients, peer)
	}
}

// ReplicateToFollower sends a LogEntry to a peer follower via RPC
func ReplicateToFollower(peer string, entry models.LogEntry) bool {
	client, err := getRPCClient(peer)
	if err != nil {
		atomic.AddInt64(&ReplicationFailureCount, 1)
		log.Printf("Replication to %s failed (dial): %v", peer, err)
		return false
	}

	args := models.ReplicateArgs{
		Entries: []models.LogEntry{entry},
	}
	var reply models.ReplicateReply

	// Set a deadline on the call by doing it asynchronously
	done := make(chan error, 1)
	go func() {
		err := client.Call("KVNode.Replicate", &args, &reply)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			atomic.AddInt64(&ReplicationFailureCount, 1)
			log.Printf("Replication to %s failed (call): %v", peer, err)
			// Remove failed client to force a reconnect next time
			markClientFailed(peer)
			return false
		}
		if reply.Status == "ok" {
			atomic.AddInt64(&ReplicationSuccessCount, 1)
			log.Printf("Replicated index %d to %s", entry.Index, peer)
			return true
		}
		atomic.AddInt64(&ReplicationFailureCount, 1)
		log.Printf("Replication to %s returned status: %s", peer, reply.Status)
		return false

	case <-time.After(2 * time.Second):
		atomic.AddInt64(&ReplicationFailureCount, 1)
		log.Printf("Replication to %s timed out after 2s", peer)
		markClientFailed(peer)
		return false
	}
}

// ReplicateToCluster sends the entry to all peers and checks if quorum is reached
func ReplicateToCluster(entry models.LogEntry, peers []string) (bool, error) {
	if len(peers) == 0 {
		return true, nil
	}

	totalNodes := len(peers) + 1
	quorum := totalNodes/2 + 1
	requiredFollowerAcks := quorum - 1

	var wg sync.WaitGroup
	acksChan := make(chan bool, len(peers))

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			acksChan <- ReplicateToFollower(p, entry)
		}(peer)
	}

	wg.Wait()
	close(acksChan)

	ackCount := 0
	for ack := range acksChan {
		if ack {
			ackCount++
		}
	}

	log.Printf("Quorum check: %d/%d follower ACKs (need %d)", ackCount, len(peers), requiredFollowerAcks)
	if ackCount >= requiredFollowerAcks {
		return true, nil
	}
	return false, errors.New("write failed: could not reach quorum")
}
