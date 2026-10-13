package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	raftpb "raft-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func connect(address string) (*grpc.ClientConn, raftpb.KVServiceClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		return nil, nil, err
	}

	client := raftpb.NewKVServiceClient(conn)

	return conn, client, nil
}
func main() {

	if len(os.Args) < 4 {
		fmt.Println("Usage:")
		fmt.Println("PUT:    go run ./cmd/client <address> PUT <key> <value>")
		fmt.Println("GET:    go run ./cmd/client <address> GET <key>")
		fmt.Println("DELETE: go run ./cmd/client <address> DELETE <key>")
		return
	}

	address := os.Args[1]
	command := strings.ToUpper(os.Args[2])
	key := os.Args[3]

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		fmt.Println("Could not create gRPC client:", err)
		return
	}

	defer conn.Close()

	client := raftpb.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	switch command {

	case "PUT":

		if len(os.Args) < 5 {
			fmt.Println("PUT requires a value")
			return
		}

		value := os.Args[4]

		response, err := client.Put(
			ctx,
			&raftpb.PutRequest{
				Key:   key,
				Value: value,
			},
		)

		if err != nil {
			fmt.Println("PUT RPC error:", err)
			return
		}

		// We contacted a follower.
		if !response.Success && response.LeaderAddress != "" {

			fmt.Println(
				"Redirecting to leader:",
				response.LeaderAddress,
			)

			leaderConn, leaderClient, err :=
				connect(response.LeaderAddress)

			if err != nil {
				fmt.Println("Could not connect to leader:", err)
				return
			}

			defer leaderConn.Close()

			leaderCtx, leaderCancel :=
				context.WithTimeout(
					context.Background(),
					3*time.Second,
				)

			defer leaderCancel()

			response, err = leaderClient.Put(
				leaderCtx,
				&raftpb.PutRequest{
					Key:   key,
					Value: value,
				},
			)

			if err != nil {
				fmt.Println("PUT RPC error:", err)
				return
			}
		}

		fmt.Println(response.Message)

	case "GET":

		response, err := client.Get(
			ctx,
			&raftpb.GetRequest{
				Key: key,
			},
		)

		if err != nil {
			fmt.Println("GET RPC error:", err)
			return
		}

		// We contacted a follower.
		if !response.Success && response.LeaderAddress != "" {

			fmt.Println(
				"Redirecting to leader:",
				response.LeaderAddress,
			)

			leaderConn, leaderClient, err :=
				connect(response.LeaderAddress)

			if err != nil {
				fmt.Println("Could not connect to leader:", err)
				return
			}

			defer leaderConn.Close()

			leaderCtx, leaderCancel :=
				context.WithTimeout(
					context.Background(),
					3*time.Second,
				)

			defer leaderCancel()

			response, err = leaderClient.Get(
				leaderCtx,
				&raftpb.GetRequest{
					Key: key,
				},
			)

			if err != nil {
				fmt.Println("GET RPC error:", err)
				return
			}
		}

		if response.Success {
			fmt.Println("Value:", response.Value)
		} else {
			fmt.Println(response.Message)
		}

	case "DELETE":

		response, err := client.Delete(
			ctx,
			&raftpb.DeleteRequest{
				Key: key,
			},
		)

		if err != nil {
			fmt.Println("DELETE RPC error:", err)
			return
		}

		// We contacted a follower.
		if !response.Success && response.LeaderAddress != "" {

			fmt.Println(
				"Redirecting to leader:",
				response.LeaderAddress,
			)

			leaderConn, leaderClient, err :=
				connect(response.LeaderAddress)

			if err != nil {
				fmt.Println("Could not connect to leader:", err)
				return
			}

			defer leaderConn.Close()

			leaderCtx, leaderCancel :=
				context.WithTimeout(
					context.Background(),
					3*time.Second,
				)

			defer leaderCancel()

			response, err = leaderClient.Delete(
				leaderCtx,
				&raftpb.DeleteRequest{
					Key: key,
				},
			)

			if err != nil {
				fmt.Println("DELETE RPC error:", err)
				return
			}
		}

		fmt.Println(response.Message)
	}
}
