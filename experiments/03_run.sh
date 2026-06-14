#!/usr/bin/env bash
# Orchestrates the replication failover experiment.
#
# 1. Starts server 1 with FAIL_DURING_REPLICATION=1 (it will crash itself
#    during the first Close() that requires replication).
# 2. Starts servers 2 and 3 normally.
# 3. Waits for leader election to settle.
# 4. Runs the failover test client.
# 5. Verifies server 1's process actually exited.
# 6. Cleans up remaining server processes.

set -e
cd "$(dirname "$0")/.."  # repo root

mkdir -p experiments/results
LOGDIR="experiments/results/logs"
mkdir -p "$LOGDIR"

echo "Starting server 1 (id=1, will crash during replication)..."
FAIL_DURING_REPLICATION=1 go run test_server_1/simple_server.go -id=1 -port=5001 \
  > "$LOGDIR/server1.log" 2>&1 &
SERVER1_PID=$!

echo "Starting server 2 (id=2)..."
go run test_server_2/bu1_server.go -id=2 -port=5002 \
  > "$LOGDIR/server2.log" 2>&1 &
SERVER2_PID=$!

echo "Starting server 3 (id=3)..."
go run test_server_3/bu2_server.go -id=3 -port=5003 \
  > "$LOGDIR/server3.log" 2>&1 &
SERVER3_PID=$!

cleanup() {
  echo "Cleaning up server processes..."
  kill "$SERVER1_PID" 2>/dev/null || true
  kill "$SERVER2_PID" 2>/dev/null || true
  kill "$SERVER3_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting 6s for cluster to elect a leader..."
sleep 6

echo "Running failover test client..."

go run experiments/03_replication_failover/main.go


echo "Checking server 1 process status..."
if kill -0 "$SERVER1_PID" 2>/dev/null; then
  echo "WARNING: server 1 (pid $SERVER1_PID) is still running — FAIL_DURING_REPLICATION may not have triggered."
  echo "  Check $LOGDIR/server1.log for 'Simulating server crash during replication'"
else
  echo "OK: server 1 (pid $SERVER1_PID) exited as expected (crash injection worked)."
fi

echo "Done. Logs in $LOGDIR/"
