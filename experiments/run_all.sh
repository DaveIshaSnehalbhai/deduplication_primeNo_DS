#!/usr/bin/env bash
# Runs all four experiments against a running 3-node cluster.
#
# PREREQUISITES:
#   - A 3-node cluster running on localhost:5001-5003
#     (start with: go run test_server_1/simple_server.go -id=1 -port=5001, etc.
#      OR use experiments/03_replication_failover/run.sh for experiment 3,
#      which manages its own cluster lifecycle)
#
# This script runs experiments 1, 2, and 4 against an already-running cluster,
# then runs experiment 3 separately (it manages its own servers).
#
# Results land in experiments/results/*.csv — run collect_results.sh
# afterwards to render them into README.md.

set -e
cd "$(dirname "$0")/.."  # repo root

mkdir -p experiments/results seed_data

echo "============================================"
echo "Experiment 01: Baseline Throughput"
echo "============================================"
for size in 1000 10000 100000; do
  echo "--- size=$size ---"
  go run experiments/01_baseline_throughput/main.go -size=$size || true
done

echo
echo "============================================"
echo "Experiment 02: Concurrent Clients + Dedup"
echo "============================================"
for clients in 1 2 4 8; do
  echo "--- clients=$clients ---"
  go run experiments/02_concurrent_dedup/main.go -generate -clients=$clients -size=5000 -overlap=0.5
  echo "  (copy seed_data/concurrent_*.txt to each server's storage/input/ now)"
  read -p "  Press enter once copied and servers restarted with new input files..." _
  go run experiments/02_concurrent_dedup/main.go -clients=$clients -size=5000 -overlap=0.5 || true
done

echo
echo "============================================"
echo "Experiment 04: Chandy-Lamport Snapshot"
echo "============================================"
go run experiments/04_snapshot_client_failure/main.go || true

echo
echo "============================================"
echo "Experiment 03: Replication Failover"
echo "============================================"
echo "This experiment manages its own server cluster."
bash experiments/03_replication_failover/run.sh || true

echo
echo "All experiments complete. Results in experiments/results/*.csv"
echo "Run: bash experiments/collect_results.sh  to render README tables."
