package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	raftpb "raft-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run ./cmd/benchmark <leader-address> [requests] [concurrency]")
		fmt.Println("Example: go run ./cmd/benchmark localhost:5001 1000 20")
		return
	}

	address := os.Args[1]
	requests := parsePositiveArg(os.Args, 2, 1000)
	concurrency := parsePositiveArg(os.Args, 3, 20)
	if concurrency > requests {
		concurrency = requests
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("Could not create gRPC client:", err)
		return
	}
	defer conn.Close()

	client := raftpb.NewKVServiceClient(conn)

	jobs := make(chan int)
	latencies := make(chan time.Duration, requests)
	var successCount atomic.Int64
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for id := range jobs {
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			response, err := client.Put(ctx, &raftpb.PutRequest{
				Key:   fmt.Sprintf("bench-%d", id),
				Value: strconv.Itoa(id),
			})
			cancel()
			latencies <- time.Since(started)

			if err == nil && response.Success {
				successCount.Add(1)
			}
		}
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker()
	}

	started := time.Now()
	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(started)
	close(latencies)

	all := make([]time.Duration, 0, requests)
	for latency := range latencies {
		all = append(all, latency)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	successes := successCount.Load()
	throughput := float64(successes) / elapsed.Seconds()

	fmt.Printf("Requests:    %d\n", requests)
	fmt.Printf("Successful:  %d\n", successes)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Elapsed:     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput:  %.2f successful requests/sec\n", throughput)
	if len(all) > 0 {
		fmt.Printf("p50 latency: %s\n", percentile(all, 0.50))
		fmt.Printf("p95 latency: %s\n", percentile(all, 0.95))
		fmt.Printf("p99 latency: %s\n", percentile(all, 0.99))
	}
}

func parsePositiveArg(args []string, index, fallback int) int {
	if len(args) <= index {
		return fallback
	}
	value, err := strconv.Atoi(args[index])
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * p)
	return values[index].Round(time.Microsecond)
}
