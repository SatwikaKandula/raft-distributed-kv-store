package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	raftpb "raft-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Println(
			"Usage: go run ./cmd/admin <address> <block|unblock> <peerID>",
		)
		return
	}

	address := os.Args[1]
	action := strings.ToLower(os.Args[2])

	peerID, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("Invalid peer ID")
		return
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		fmt.Println("Connection error:", err)
		return
	}

	defer conn.Close()

	client := raftpb.NewAdminServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	switch action {

	case "block":

		response, err := client.BlockPeer(
			ctx,
			&raftpb.BlockPeerRequest{
				PeerId: int32(peerID),
			},
		)

		if err != nil {
			fmt.Println("Block RPC error:", err)
			return
		}

		fmt.Println(response.Message)

	case "unblock":

		response, err := client.UnblockPeer(
			ctx,
			&raftpb.UnblockPeerRequest{
				PeerId: int32(peerID),
			},
		)

		if err != nil {
			fmt.Println("Unblock RPC error:", err)
			return
		}

		fmt.Println(response.Message)

	default:
		fmt.Println("Action must be block or unblock")
	}
}
