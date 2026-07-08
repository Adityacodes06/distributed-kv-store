package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"distributed-kv-store/app/models"
)

// CheckPeerHealth checks the health of a peer node using HTTP
func CheckPeerHealth(peerURL string, timeout time.Duration) models.PeerStatus {
	client := http.Client{
		Timeout: timeout,
	}

	url := fmt.Sprintf("%s/health", strings.TrimSuffix(peerURL, "/"))
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Peer %s is unreachable: %v", peerURL, err)
		return models.PeerStatus{
			Peer:   peerURL,
			Status: "unreachable",
			Error:  err.Error(),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Peer %s is unreachable: status code %d", peerURL, resp.StatusCode)
		return models.PeerStatus{
			Peer:   peerURL,
			Status: "unreachable",
			Error:  fmt.Sprintf("HTTP status %d", resp.StatusCode),
		}
	}

	var healthReply models.HealthReply
	if err := json.NewDecoder(resp.Body).Decode(&healthReply); err != nil {
		log.Printf("Peer %s returned invalid health data: %v", peerURL, err)
		return models.PeerStatus{
			Peer:   peerURL,
			Status: "unreachable",
			Error:  fmt.Sprintf("invalid JSON: %v", err),
		}
	}

	return models.PeerStatus{
		Peer:   peerURL,
		Status: "healthy",
		Detail: healthReply,
	}
}

// ClusterStatus retrieves health details of all raw peers in parallel
func ClusterStatus(rawPeers []string) models.ClusterReply {
	var wg sync.WaitGroup
	statuses := make([]models.PeerStatus, len(rawPeers))

	for i, peer := range rawPeers {
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()
			statuses[idx] = CheckPeerHealth(p, 1500*time.Millisecond)
		}(i, peer)
	}

	wg.Wait()
	return statuses
}

// SyncFromLeader connects to the leader using RPC and retrieves missing log entries
func SyncFromLeader(leaderURL string, lastIndex int) ([]models.LogEntry, error) {
	// Extract host:port from the HTTP URL for RPC dialing
	rpcAddr := leaderURL
	rpcAddr = strings.TrimPrefix(rpcAddr, "http://")
	rpcAddr = strings.TrimPrefix(rpcAddr, "https://")

	client, err := rpc.DialHTTP("tcp", rpcAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial leader RPC %s: %w", rpcAddr, err)
	}
	defer client.Close()

	args := models.SyncArgs{
		AfterIndex: lastIndex,
	}
	var reply models.SyncReply

	err = client.Call("KVNode.Sync", &args, &reply)
	if err != nil {
		return nil, fmt.Errorf("sync RPC call failed: %w", err)
	}

	log.Printf("Sync: received %d entries from leader", len(reply.Entries))
	return reply.Entries, nil
}
