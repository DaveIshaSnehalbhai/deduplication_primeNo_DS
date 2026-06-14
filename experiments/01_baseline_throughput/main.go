// Experiment 01: Baseline Throughput
//
// WHAT IT MEASURES:
//   A single client reads an input file, filters primes locally, writes the
//   result to a fresh output file, and commits via Close(). We measure:
//     - end-to-end wall clock time
//     - server-side write throughput (bytes/sec of the committed output)
//     - primes found / numbers processed
//
// WHY THIS IS THE BASELINE:
//   Every other experiment (concurrent, replication, snapshot) is compared
//   against this number. If concurrent writes from N clients take roughly
//   the same wall-clock time as N sequential single-client runs, replication
//   overhead is the dominant cost. If it's much higher, lock contention on
//   the output FileEntry is the bottleneck.
//
// HOW TO RUN:
//   Start a 3-node cluster first (see README "Quick Start"), then:
//     go run experiments/01_baseline_throughput/main.go -size=1000
//     go run experiments/01_baseline_throughput/main.go -size=10000
//     go run experiments/01_baseline_throughput/main.go -size=100000
//
// Each run appends one row to experiments/results/baseline_throughput.csv.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	ikkat "distributed-system-ikkat"
	common "distributed-system-ikkat/experiments/common"
)

func main() {
	size := flag.Int("size", 10000, "number of integers in the generated dataset")
	maxVal := flag.Uint64("max", 1_000_000, "max value of generated integers")
	flag.Parse()

	servers := []string{"localhost:5001", "localhost:5002", "localhost:5003"}

	inputName := fmt.Sprintf("input/bench_input_%d.txt", *size)
	outputName := fmt.Sprintf("output/bench_output_%d.txt", *size)
	clientID := fmt.Sprintf("bench-baseline-%d", *size)

	// 1. Generate dataset locally, then seed it into the server's input dir.
	// NOTE: this experiment assumes the input file already exists on the
	// server (input/ is read-only and pre-populated). If it doesn't exist,
	// generate it once with:
	//   common.GenerateDataset("storage/input/bench_input_<size>.txt", size, max, 0.0)
	// and copy storage/ into each server's storage/ directory before starting them.
	localInputPath := fmt.Sprintf("seed_data/bench_input_%d.txt", *size)
	if err := common.GenerateDataset(localInputPath, *size, *maxVal, 0.0); err != nil {
		log.Fatalf("generate dataset: %v", err)
	}
	fmt.Printf("Generated %d numbers at %s (copy to each server's storage/%s before running)\n",
		*size, localInputPath, inputName)

	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	result := common.NewResult("baseline_throughput")
	result.SetInt("dataset_size", *size)

	start := time.Now()

	// Step 1: open + read input
	if _, err := c.Open(ctx, inputName, 0, clientID); err != nil {
		log.Fatalf("open input failed: %v", err)
	}
	rawContent, err := c.Read(ctx, inputName, clientID)
	if err != nil {
		log.Fatalf("read input failed: %v", err)
	}
	readDone := time.Now()

	// Step 2: local prime filter (CPU-bound, measured separately from I/O)
	nums := strings.Fields(string(rawContent))
	var primes []string
	for _, s := range nums {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			continue
		}
		if ikkat.IsPrime(n) {
			primes = append(primes, s)
		}
	}
	primeResult := strings.Join(primes, "\n")
	filterDone := time.Now()

	// Step 3: create + open + write + close output (commit)
	if _, err := c.Create(ctx, outputName, clientID); err != nil {
		log.Printf("create output (may already exist): %v", err)
	}
	if _, err := c.Open(ctx, outputName, 1, clientID); err != nil {
		log.Fatalf("open output failed: %v", err)
	}
	if err := c.AppendFile(outputName, []byte(primeResult)); err != nil {
		log.Fatalf("append failed: %v", err)
	}
	commitStart := time.Now()
	if err := c.Close(ctx, outputName, clientID); err != nil {
		log.Fatalf("close (commit) failed: %v", err)
	}
	commitDone := time.Now()

	total := commitDone.Sub(start)

	result.SetInt("numbers_processed", len(nums))
	result.SetInt("primes_found", len(primes))
	result.SetDuration("read_time_s", readDone.Sub(start))
	result.SetDuration("filter_time_s", filterDone.Sub(readDone))
	result.SetDuration("commit_time_s", commitDone.Sub(commitStart))
	result.SetDuration("total_time_s", total)
	throughput := float64(len(nums)) / total.Seconds()
	result.SetFloat("throughput_nums_per_sec", throughput)

	result.PrintTable()
	if err := result.WriteCSV(); err != nil {
		log.Printf("write csv failed: %v", err)
	}
}
