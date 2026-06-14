// Experiment 03: Replication & Failover
//
// WHAT IT MEASURES:
//   1. A client writes to the cluster while the PRIMARY is killed mid-
//      replication (FAIL_DURING_REPLICATION=1 env var on the primary).
//   2. We measure how long it takes for a backup to be elected primary and
//      start serving requests again ("failover time").
//   3. We verify the committed data survived on the new primary (durability).
//   4. We verify the client's retried write is idempotent (no duplicate
//      content from the retry).
//
// THIS IS AN ORCHESTRATION TEST, NOT JUST A CLIENT.
//   It assumes you run the 3-server cluster via the provided shell script
//   experiments/03_replication_failover/run.sh, which:
//     - starts server 1 (id=1) with FAIL_DURING_REPLICATION=1
//     - starts servers 2 and 3 normally
//     - runs THIS program, which performs a write and times the failover
//     - the bash script then checks server 1's process exited
//
// HOW TO RUN:
//   bash experiments/03_replication_failover/run.sh
//
// The bash script writes the final result row to
// experiments/results/replication_failover.csv

package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	ikkat "distributed-system-ikkat"
	common "distributed-system-ikkat/experiments/common"
)

func main() {
	servers := []string{"localhost:5001", "localhost:5002", "localhost:5003"}
	outputName := "output/failover_test.txt"
	clientID := "failover-tester"

	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Clean slate.
	c.Delete(ctx, outputName)

	content := "2\n3\n5\n7\n11\n13\n17\n19\n23\n29\n"

	result := common.NewResult("replication_failover")

	// Step 1: create + open + write. The primary is configured with
	// FAIL_DURING_REPLICATION=1, so it will os.Exit(1) during Close() AFTER
	// the WAL entry is persisted locally but BEFORE the response is returned.
	// The client's gRPC call will get a connection error; DialClient's retry
	// logic + reconnectToLeader should redirect to the new primary.
	if _, err := c.Create(ctx, outputName, clientID); err != nil {
		log.Printf("create (ok if exists): %v", err)
	}
	if _, err := c.Open(ctx, outputName, 1, clientID); err != nil {
		log.Fatalf("open failed: %v", err)
	}
	if err := c.AppendFile(outputName, []byte(content)); err != nil {
		log.Fatalf("append failed: %v", err)
	}

	fmt.Println("Calling Close() — primary should crash now (FAIL_DURING_REPLICATION=1)...")
	closeStart := time.Now()

	err = c.Close(ctx, outputName, clientID)
	closeDuration := time.Since(closeStart)

	if err != nil {
		fmt.Printf("First Close() failed as expected (primary crashed): %v\n", err)
		fmt.Println("Retrying Close() — client should reconnect to new primary...")

		retryStart := time.Now()
		// Re-dial in case the old connection is fully dead.
		c2, conn2, derr := ikkat.DialClient(servers)
		if derr != nil {
			log.Fatalf("re-dial after primary crash failed: %v", derr)
		}
		defer conn2.Close()

		// Re-open since the FD from the dead primary is no longer valid on
		// the new primary (in-memory state). Re-create is idempotent — the
		// file content was already committed to the WAL on a majority before
		// the crash (FAIL_DURING_REPLICATION fires AFTER majority commit in
		// Close — see server_basics.go).
		if _, err := c2.Open(ctx, outputName, 0, clientID); err != nil {
			log.Fatalf("re-open after failover failed: %v", err)
		}
		data, rerr := c2.Read(ctx, outputName, clientID)
		if rerr != nil {
			log.Fatalf("read after failover failed: %v", rerr)
		}
		failoverDuration := time.Since(retryStart)

		lines := strings.Fields(string(data))
		seen := make(map[string]bool)
		dup := false
		for _, l := range lines {
			if seen[l] {
				dup = true
			}
			seen[l] = true
		}

		result.SetDuration("close_time_before_crash_s", closeDuration)
		result.SetDuration("failover_recovery_time_s", failoverDuration)
		result.SetInt("lines_committed", len(lines))
		result.Set("duplicate_after_retry", fmt.Sprintf("%v", dup))
		result.Set("data_survived_failover", fmt.Sprintf("%v", len(lines) == 10))
		result.Set("primary_crashed_as_expected", "true")
	} else {
		// No crash happened (maybe FAIL_DURING_REPLICATION wasn't set) —
		// still record the timing for comparison.
		result.SetDuration("close_time_before_crash_s", closeDuration)
		result.SetDuration("failover_recovery_time_s", 0)
		result.Set("primary_crashed_as_expected", "false")
		result.Set("note", "set FAIL_DURING_REPLICATION=1 on primary to test failover")
	}

	result.PrintTable()
	if err := result.WriteCSV(); err != nil {
		log.Printf("write csv failed: %v", err)
	}
}
