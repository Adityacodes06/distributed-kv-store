package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"

	"distributed-kv-store/app/config"
	"distributed-kv-store/app/cluster"
	"distributed-kv-store/app/server"
	"distributed-kv-store/app/storage"
)

func main() {
	cfg := config.LoadConfig()
	log.Printf("Starting Node %s as %s (port %d)", cfg.NodeID, cfg.NodeRole, cfg.NodePort)

	engine, err := storage.NewStorageEngine(cfg.DataDir)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}
	defer engine.Close()

	node := server.NewKVNode(cfg, engine)

	// Trigger initial catch-up sync from leader if follower
	if !cfg.IsLeader() {
		go func() {
			// Trigger follower sync
			entries, err := cluster.SyncFromLeader(cfg.LeaderURL, engine.LogIndex())
			if err != nil {
				log.Printf("Initial sync from leader failed (will retry): %v", err)
			} else {
				for _, entry := range entries {
					if err := engine.ApplyReplicatedEntry(entry); err != nil {
						log.Printf("Failed to apply replicated entry index %d during sync: %v", entry.Index, err)
					}
				}
				log.Printf("Follower sync complete — log index is now %d", engine.LogIndex())
			}
		}()
	}

	// Register RPC Server
	err = rpc.Register(node)
	if err != nil {
		log.Fatalf("Failed to register RPC: %v", err)
	}

	// Setup serve mux with all HTTP and RPC handlers
	mux := node.SetupMux()

	addr := fmt.Sprintf("%s:%d", cfg.NodeHost, cfg.NodePort)
	log.Printf("Listening for HTTP and RPC on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
