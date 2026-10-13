package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"raft-kv-store/internal/raft"
)

const clusterSize = 5

func peerAddress(peerID int) string {
	// Individual overrides are useful outside Docker, for example:
	// RAFT_NODE_2_ADDR=10.0.0.12:5002
	key := fmt.Sprintf("RAFT_NODE_%d_ADDR", peerID)
	if value := os.Getenv(key); value != "" {
		return value
	}

	if strings.EqualFold(os.Getenv("RAFT_DOCKER"), "true") {
		return fmt.Sprintf("node%d:%d", peerID, 5000+peerID)
	}

	return fmt.Sprintf("localhost:%d", 5000+peerID)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run ./cmd/node <nodeID>")
		return
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil || nodeID < 1 || nodeID > clusterSize {
		fmt.Printf("Node ID must be between 1 and %d\n", clusterSize)
		return
	}

	peers := make([]int, 0, clusterSize-1)
	for id := 1; id <= clusterSize; id++ {
		if id != nodeID {
			peers = append(peers, id)
		}
	}

	node := raft.NewNode(nodeID, peers)
	if value := os.Getenv("RAFT_SNAPSHOT_THRESHOLD"); value != "" {
		threshold, err := strconv.Atoi(value)
		if err != nil || threshold <= 0 {
			fmt.Println("RAFT_SNAPSHOT_THRESHOLD must be a positive integer")
			return
		}
		node.SnapshotThreshold = threshold
	}

	for _, peerID := range peers {
		node.SetPeerAddress(peerID, peerAddress(peerID))
	}

	address := fmt.Sprintf(":%d", 5000+nodeID)
	if configured := os.Getenv("RAFT_LISTEN_ADDR"); configured != "" {
		address = configured
	}

	if err := node.LoadState(); err != nil {
		fmt.Printf(
			"Node %d failed to load persistent state: %v\n",
			node.ID,
			err,
		)
		return
	}

	go func() {
		if err := node.StartGRPCServer(address); err != nil {
			fmt.Printf(
				"Node %d gRPC server error: %v\n",
				node.ID,
				err,
			)
		}
	}()

	// Give the server a moment to start before election activity begins.
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Node %d started\n", node.ID)
	node.StartElectionTimer()

	select {}
}
