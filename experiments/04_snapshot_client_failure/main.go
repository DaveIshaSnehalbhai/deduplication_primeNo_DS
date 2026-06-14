// Experiment 04: Chandy-Lamport Snapshot for Client Failure Detection
//
// WHAT IT MEASURES:
//   1. A "worker" client opens an output file in WRITE mode and holds it
//      open WITHOUT committing (simulating a worker mid-processing).
//   2. While the worker is "stuck", we call InitiateSnapshot on the primary.
//   3. We measure snapshot completion time (all backups ACKed).
//   4. We load the resulting snapshot JSON and verify it captured the
//      worker's open FD with Mode=WRITE — i.e. CheckClientInSnapshot
//      correctly identifies that this client had uncommitted work.
//   5. We simulate the worker crashing (never calls Close) and verify the
//      lease eventually expires, releasing the write lock for other clients.
//
// WHY THIS MATTERS:
//   This is the core test for the client-failure recovery story: given a
//   snapshot taken while a client was mid-write, can an operator determine
//   "did this client's work make it to disk or not?" The answer here should
//   be NO (the file lock was held but Close was never called, so no WAL
//   entry exists for this client's data) — which is exactly what
//   CheckClientInSnapshot(snap, workerID) should report via Mode==WRITE.
//
// HOW TO RUN:
//   go run experiments/04_snapshot_client_failure/main.go
//
// Requires the SnapshotService to be registered on all 3 servers (see
// connection.go RegisterSnapshotServiceServer) and a clean cluster.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	ikkat "distributed-system-ikkat"
	common "distributed-system-ikkat/experiments/common"
	pb "distributed-system-ikkat/filesystem"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	servers := []string{"localhost:5001", "localhost:5002", "localhost:5003"}
	outputName := "output/snapshot_test.txt"
	workerID := "snapshot-worker-42"

	result := common.NewResult("snapshot_client_failure")

	// ── Step 1: worker opens file for writing and does NOT close it ───────
	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c.Delete(ctx, outputName) // clean slate (ignore error)

	if _, err := c.Create(ctx, outputName, workerID); err != nil {
		log.Printf("create (ok if exists): %v", err)
	}
	if _, err := c.Open(ctx, outputName, 1, workerID); err != nil {
		log.Fatalf("open for write failed: %v", err)
	}
	// Write local data but do NOT call Close — this is the "stuck/crashed
	// mid-processing" state. The server's FileEntry now has activeWriter=true
	// for this file, and meta.clients[workerID] has Mode=WRITE.
	if err := c.AppendFile(outputName, []byte("999983\n")); err != nil {
		log.Fatalf("append failed: %v", err)
	}
	fmt.Println("Worker has opened the file in WRITE mode and NOT committed (simulating crash).")

	// ── Step 2: initiate a Chandy-Lamport snapshot on the primary ─────────
	leaderConn, leaderAddr, err := dialLeader(servers)
	if err != nil {
		log.Fatalf("find leader failed: %v", err)
	}
	defer leaderConn.Close()
	fmt.Printf("Primary is at %s\n", leaderAddr)

	snapClient := pb.NewSnapshotServiceClient(leaderConn)
	snapID := fmt.Sprintf("exp04-%d", time.Now().Unix())

	snapStart := time.Now()
	snapCtx, snapCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer snapCancel()
	resp, err := snapClient.InitiateSnapshot(snapCtx, &pb.SnapshotRequest{SnapshotId: snapID})
	snapDuration := time.Since(snapStart)
	if err != nil {
		log.Fatalf("InitiateSnapshot failed: %v", err)
	}

	result.SetDuration("snapshot_completion_time_s", snapDuration)
	result.Set("snapshot_success", fmt.Sprintf("%v", resp.Success))
	result.Set("snapshot_id", snapID)

	// ── Step 3: load the snapshot file written by the primary ─────────────
	// The primary writes to ./snapshot_<serverID>_<snapID>.json in its CWD.
	// We don't know the primary's server ID from here directly, so try 1..3.
	var snap *ikkat.GlobalSnapshot
	var loadedFrom string
	for _, sid := range []string{"1", "2", "3"} {
		path := fmt.Sprintf("snapshot_%s_%s.json", sid, snapID)
		s, err := ikkat.LoadSnapshot(path)
		if err == nil {
			snap = s
			loadedFrom = path
			break
		}
	}
	if snap == nil {
		log.Fatalf("could not load any snapshot_*_%s.json file — check server CWDs", snapID)
	}
	fmt.Printf("Loaded snapshot from %s\n", loadedFrom)

	// ── Step 4: verify the worker's open write-mode FD was captured ───────
	dirty, details := ikkat.CheckClientInSnapshot(snap, workerID)
	result.Set("worker_captured_as_dirty", fmt.Sprintf("%v", dirty))
	result.SetInt("open_fds_for_worker", len(details))
	for _, d := range details {
		fmt.Printf("  Snapshot entry: client=%s file=%s fd=%d mode=%v version=%d\n",
			d.ClientID, d.Filename, d.FD, d.Mode, d.Version)
	}

	// ── Step 5: simulate crash — close the connection without calling Close ──
	conn.Close() // worker "dies" — connection drops, file never closed

	fmt.Println("Worker connection closed without committing (simulated crash).")
	fmt.Println("NOTE: full lease-expiry verification takes LeaseTimeout (10 min) —")
	fmt.Println("      skipping the wait in this experiment. To test manually, wait")
	fmt.Println("      10 minutes then have another client Open the file in WRITE mode.")
	result.Set("lease_expiry_tested", "false")
	result.Set("lease_timeout_constant", "10m")

	result.PrintTable()
	if err := result.WriteCSV(); err != nil {
		log.Printf("write csv failed: %v", err)
	}
}

// dialLeader connects to the cluster, asks for the current leader, and
// returns a direct connection to the leader along with its address.
func dialLeader(servers []string) (*grpc.ClientConn, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, addr := range servers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		fc := pb.NewFileServiceClient(conn)
		leaderResp, err := fc.GetLeader(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		if leaderResp.Address == addr {
			return conn, addr, nil
		}
		conn.Close()
		leaderConn, err := grpc.NewClient(leaderResp.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, "", err
		}
		return leaderConn, leaderResp.Address, nil
	}
	return nil, "", fmt.Errorf("no server responded to GetLeader")
}
