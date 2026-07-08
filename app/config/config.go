package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	NodeID    string
	NodeRole  string // "leader" or "follower"
	NodeHost  string
	NodePort  int
	Peers     []string // stripped of http:// for easy RPC dialing
	RawPeers  []string // unmodified peers with http:// prefix
	LeaderURL string
	DataDir   string
}

func LoadConfig() *Config {
	nodeID := getEnv("NODE_ID", "node1")
	nodeRole := getEnv("NODE_ROLE", "leader")
	nodeHost := getEnv("NODE_HOST", "0.0.0.0")
	nodePortStr := getEnv("NODE_PORT", "8000")
	nodePort, err := strconv.Atoi(nodePortStr)
	if err != nil {
		nodePort = 8000
	}
	peersRaw := getEnv("PEERS", "")
	var peers []string
	var rawPeers []string
	if peersRaw != "" {
		parts := strings.Split(peersRaw, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				rawPeers = append(rawPeers, trimmed)
				// Strip HTTP prefix for RPC dialing
				rpcAddr := trimmed
				rpcAddr = strings.TrimPrefix(rpcAddr, "http://")
				rpcAddr = strings.TrimPrefix(rpcAddr, "https://")
				peers = append(peers, rpcAddr)
			}
		}
	}
	leaderURL := getEnv("LEADER_URL", "http://localhost:8000")
	dataDir := getEnv("DATA_DIR", "./data")

	return &Config{
		NodeID:    nodeID,
		NodeRole:  nodeRole,
		NodeHost:  nodeHost,
		NodePort:  nodePort,
		Peers:     peers,
		RawPeers:  rawPeers,
		LeaderURL: leaderURL,
		DataDir:   dataDir,
	}
}

func (c *Config) IsLeader() bool {
	return c.NodeRole == "leader"
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
