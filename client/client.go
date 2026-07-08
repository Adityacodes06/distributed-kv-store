package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"

	"distributed-kv-store/app/models"
)

const defaultURL = "localhost:8001"

func main() {
	// Parse general flags
	urlFlag := flag.String("url", defaultURL, "Base URL/address of the KV node (e.g. localhost:8001 or http://localhost:8001)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]

	// Format address for RPC DialHTTP (strip http prefix if present)
	addr := *urlFlag
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	if !strings.Contains(addr, ":") {
		// Default to port 8000 if not specified
		addr += ":8000"
	}

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		fmt.Printf("❌ Cannot connect to %s — is the node running? (Error: %v)\n", addr, err)
		os.Exit(1)
	}
	defer client.Close()

	var result interface{}

	switch command {
	case "put":
		if len(args) < 3 {
			log.Fatalf("Usage: client put <key> <value>")
		}
		putArgs := models.PutArgs{Key: args[1], Value: args[2]}
		var reply models.PutReply
		err = client.Call("KVNode.Put", &putArgs, &reply)
		result = reply

	case "get":
		if len(args) < 2 {
			log.Fatalf("Usage: client get <key>")
		}
		getArgs := models.GetArgs{Key: args[1]}
		var reply models.GetReply
		err = client.Call("KVNode.Get", &getArgs, &reply)
		result = reply

	case "delete":
		if len(args) < 2 {
			log.Fatalf("Usage: client delete <key>")
		}
		delArgs := models.DeleteArgs{Key: args[1]}
		var reply models.DeleteReply
		err = client.Call("KVNode.Delete", &delArgs, &reply)
		result = reply

	case "health":
		healthArgs := models.HealthArgs{}
		var reply models.HealthReply
		err = client.Call("KVNode.Health", &healthArgs, &reply)
		result = reply

	case "metrics":
		metricsArgs := models.MetricsArgs{}
		var reply models.MetricsReply
		err = client.Call("KVNode.Metrics", &metricsArgs, &reply)
		result = reply

	case "cluster":
		clusterArgs := models.ClusterArgs{}
		var reply models.ClusterReply
		err = client.Call("KVNode.Cluster", &clusterArgs, &reply)
		result = reply

	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}

	// Output formatted JSON
	prettyJSON, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatalf("Failed to format output: %v", err)
	}
	fmt.Println(string(prettyJSON))
}

func printUsage() {
	fmt.Println("CLI client for the Distributed Key-Value Store")
	fmt.Println("\nUsage:")
	fmt.Println("  go run client/client.go [flags] <command> [arguments]")
	fmt.Println("\nFlags:")
	fmt.Println("  --url string     Address of the KV node (default \"localhost:8001\")")
	fmt.Println("\nCommands:")
	fmt.Println("  put <key> <val>  Store a key-value pair")
	fmt.Println("  get <key>        Retrieve a value")
	fmt.Println("  delete <key>     Delete a key")
	fmt.Println("  health           Check node health")
	fmt.Println("  metrics          View node metrics")
	fmt.Println("  cluster          View cluster peer status")
}
