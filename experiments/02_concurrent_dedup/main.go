// Experiment 02: Concurrent Clients + Deduplication Correctness
//
// WHAT IT MEASURES:
//   N clients concurrently process DIFFERENT input datasets that share a
//   significant fraction of values (controlled by -overlap), and all write
//   their discovered primes to the SAME output file. We measure:
//     - total wall-clock time for all N clients to commit
//     - whether the final output file has ZERO duplicate primes
//       (this is the core correctness guarantee of the system)
//     - the dedup ratio: how many primes were filtered server-side because
//       another client had already written them
//
// WHY OVERLAPPING DATASETS:
//   In a real deployment, multiple workers might process overlapping shards
//   of a dataset (e.g. redundant scanning for fault tolerance, or sliding
//   windows). The server's FilePrimeSet must ensure the shared output file
//   never contains the same prime twice, regardless of write order or
//   timing — this is what we're testing under real concurrency.
//
// HOW TO RUN:
//   go run experiments/02_concurrent_dedup/main.go -clients=4 -size=5000 -overlap=0.5
//
// Pre-requisite: input files input/concurrent_<i>.txt must exist on the
// server for i in [0, clients). Generate with -generate flag first:
//   go run experiments/02_concurrent_dedup/main.go -generate -clients=4 -size=5000 -overlap=0.5
// then copy seed_data/concurrent_*.txt into each server's storage/input/.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	ikkat "distributed-system-ikkat"
	common "distributed-system-ikkat/experiments/common"
)

func main() {
	numClients := flag.Int("clients", 4, "number of concurrent clients")
	size := flag.Int("size", 5000, "numbers per client dataset")
	overlap := flag.Float64("overlap", 0.5, "fraction of values shared across all client datasets")
	maxVal := flag.Uint64("max", 100_000, "max value of generated integers")
	generate := flag.Bool("generate", false, "generate seed datasets and exit")
	flag.Parse()

	if *generate {
		generateOverlappingDatasets(*numClients, *size, *maxVal, *overlap)
		return
	}

	servers := []string{"localhost:5001", "localhost:5002", "localhost:5003"}
	outputName := "output/concurrent_dedup_test.txt"

	// Clean slate: delete the shared output file if it exists from a prior run.
	{
		c, conn, err := ikkat.DialClient(servers)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			c.Delete(ctx, outputName) // ignore error — may not exist
			cancel()
			conn.Close()
		}
	}

	var wg sync.WaitGroup
	var primesWrittenTotal int64
	var mu sync.Mutex

	start := time.Now()
	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := runWorker(idx, outputName, servers)
			if err != nil {
				log.Printf("worker %d failed: %v", idx, err)
				return
			}
			mu.Lock()
			primesWrittenTotal += int64(n)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	total := time.Since(start)

	// Verify dedup correctness by reading the final output file's line count
	// via a fresh client (server-side file, not local cache).
	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		log.Fatalf("dial for verification failed: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.Open(ctx, outputName, 0, "verifier"); err != nil {
		log.Fatalf("open output for verification failed: %v", err)
	}
	data, err := c.Read(ctx, outputName, "verifier")
	if err != nil {
		log.Fatalf("read output for verification failed: %v", err)
	}

	lines := strings.Fields(string(data))
	seen := make(map[string]bool)
	dupCount := 0
	for _, l := range lines {
		if seen[l] {
			dupCount++
		}
		seen[l] = true
	}

	result := common.NewResult("concurrent_dedup")
	result.SetInt("num_clients", *numClients)
	result.SetInt("dataset_size_per_client", *size)
	result.SetFloat("overlap_fraction", *overlap)
	result.SetDuration("total_time_s", total)
	result.SetInt64("primes_sent_total", primesWrittenTotal)
	result.SetInt("primes_in_final_file", len(seen))
	result.SetInt("duplicate_lines_found", dupCount)
	dedupPct := 0.0
	if primesWrittenTotal > 0 {
		dedupPct = 100.0 * (1.0 - float64(len(seen))/float64(primesWrittenTotal))
	}
	result.SetFloat("server_side_dedup_pct", dedupPct)
	result.Set("zero_duplicates_in_output", fmt.Sprintf("%v", dupCount == 0))

	result.PrintTable()
	if err := result.WriteCSV(); err != nil {
		log.Printf("write csv failed: %v", err)
	}
}

// runWorker simulates one client: read its input dataset, filter primes,
// append to the shared output file, close (commit). Returns the count of
// primes this worker attempted to write (before server-side dedup).
func runWorker(idx int, outputName string, servers []string) (int, error) {
	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	clientID := fmt.Sprintf("concurrent-worker-%d", idx)
	inputName := fmt.Sprintf("input/concurrent_%d.txt", idx)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := c.Open(ctx, inputName, 0, clientID); err != nil {
		return 0, fmt.Errorf("open input: %w", err)
	}
	raw, err := c.Read(ctx, inputName, clientID)
	if err != nil {
		return 0, fmt.Errorf("read input: %w", err)
	}

	var primes []string
	for _, s := range strings.Fields(string(raw)) {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			continue
		}
		if ikkat.IsPrime(n) {
			primes = append(primes, s)
		}
	}

	// Create the shared output file (only the first worker to arrive succeeds;
	// others get AlreadyExists, which is fine — they just open it).
	c.Create(ctx, outputName, clientID) // ignore AlreadyExists

	if _, err := c.Open(ctx, outputName, 1, clientID); err != nil {
		return 0, fmt.Errorf("open output: %w", err)
	}
	if err := c.AppendFile(outputName, []byte(strings.Join(primes, "\n")+"\n")); err != nil {
		return 0, fmt.Errorf("append: %w", err)
	}
	if err := c.Close(ctx, outputName, clientID); err != nil {
		return 0, fmt.Errorf("close (commit): %w", err)
	}

	return len(primes), nil
}

// generateOverlappingDatasets writes N input files where `overlap` fraction
// of the values in each file come from a shared pool, and the remainder are
// unique to that file. This guarantees the server WILL see duplicate primes
// across clients, so server_side_dedup_pct should be > 0.
func generateOverlappingDatasets(numClients, size int, maxVal uint64, overlap float64) {
	sharedPoolSize := int(float64(size) * overlap)
	sharedPool := make([]uint64, sharedPoolSize)
	for i := range sharedPool {
		sharedPool[i] = uint64(rand.Int63n(int64(maxVal))) + 1
	}

	for i := 0; i < numClients; i++ {
		path := fmt.Sprintf("seed_data/concurrent_%d.txt", i)
		var sb strings.Builder
		for j := 0; j < size; j++ {
			var n uint64
			if j < sharedPoolSize {
				n = sharedPool[j] // shared across all clients
			} else {
				n = uint64(rand.Int63n(int64(maxVal))) + 1 // unique-ish
			}
			sb.WriteString(strconv.FormatUint(n, 10))
			sb.WriteByte('\n')
		}
		if err := writeFile(path, sb.String()); err != nil {
			log.Fatalf("write %s: %v", path, err)
		}
		fmt.Printf("Generated %s (%d numbers, %d shared)\n", path, size, sharedPoolSize)
	}
	fmt.Println("Copy seed_data/concurrent_*.txt into each server's storage/input/ before running without -generate.")
}

func writeFile(path, content string) error {
	return common.WriteTextFile(path, content)
}
