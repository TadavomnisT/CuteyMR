#!/usr/bin/env bash

# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (c) 2025 The CuteyMR Authors
# test_fault_tolerance.sh — kills one worker mid-job and proves the cluster
# still produces a 100% correct wordcount (tasks get re-queued automatically).
set -euo pipefail
cd "$(dirname "$0")/.."

LOGDIR=/tmp/cuteymr-ft
INPUT=$LOGDIR/input
PORT=${PORT:-8081}

echo "== CuteyMR fault tolerance test =="
make race >/dev/null

rm -rf "$LOGDIR" data/output
mkdir -p "$LOGDIR"

if [ ! -f "$INPUT/voyage_of_the_star_whale.txt" ]; then
    echo "generating big mock dataset (~30MB)…"
    bin/mr-mockgen --out "$INPUT" --scale 10
fi

bin/mr-master --port "$PORT" --workers 3 \
    --input "$INPUT" --output data/output \
    --chunk-size 256 --headless --exit-after-done 4s >"$LOGDIR/master.log" 2>&1 &
MASTER_PID=$!
sleep 0.7

PIDS=()
for i in 1 2 3; do
    bin/mr-worker --master "http://127.0.0.1:$PORT" \
        --file-port $((9200 + i)) \
        --data-dir "$LOGDIR/worker$i" \
        --headless >"$LOGDIR/worker$i.log" 2>&1 &
    PIDS+=($!)
done

cleanup() {
    kill "$MASTER_PID" "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "waiting until the map phase is well underway, then killing worker 2 with SIGKILL…"
sleep 1.2
kill -9 "${PIDS[1]}" || true
PIDS[1]=""
echo "worker 2 killed — master should notice (heartbeat timeout / data-plane probe) and re-queue its tasks"

DEADLINE=$((SECONDS + 300))
while [ $SECONDS -lt $DEADLINE ]; do
    if [ -f data/output/wordcount.json ]; then break; fi
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        echo "master exited early — log tail:"; tail -40 "$LOGDIR/master.log"; exit 1
    fi
    sleep 1
done
if [ ! -f data/output/wordcount.json ]; then
    echo "TIMEOUT: job did not finish"; tail -40 "$LOGDIR/master.log"; exit 1
fi
wait "$MASTER_PID" || true

echo
echo "== did the master notice the death? =="
grep -E "UNREACHABLE|FAILED|re-queue|rolling back" "$LOGDIR/master.log" | head -10 || echo "(no death events found!)"

echo
echo "== verification against sequential reference =="
bin/mr-wcseq --input "$INPUT" --check data/output/wordcount.json

echo
echo "RESULT: FAULT TOLERANCE PASS"
