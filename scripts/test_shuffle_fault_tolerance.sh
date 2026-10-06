#!/usr/bin/env bash

# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (c) 2025 The CuteyMR Authors
# test_shuffle_fault_tolerance.sh — kills one worker *during the shuffle
# phase*, exercising the hardest recovery path in CuteyMR:
#
#   1. a reduce task can no longer fetch intermediate files from the dead
#      worker -> reports the failed source -> master actively probes the
#      dead worker's data plane instead of waiting for the heartbeat timeout
#   2. completed map tasks whose files lived on the dead worker are
#      re-queued and the job ROLLS BACK to the map phase
#   3. the cluster re-processes the lost chunks and still produces a 100%
#      correct wordcount
set -euo pipefail
cd "$(dirname "$0")/.."

LOGDIR=/tmp/cuteymr-sft
INPUT=$LOGDIR/input
PORT=${PORT:-8082}

echo "== CuteyMR shuffle-phase fault tolerance test =="
make race >/dev/null

rm -rf "$LOGDIR" data/output
mkdir -p "$LOGDIR"

if [ ! -f "$INPUT/voyage_of_the_star_whale.txt" ]; then
    echo "generating big mock dataset (~30MB)…"
    bin/mr-mockgen --out "$INPUT" --scale 10
fi

bin/mr-master --port "$PORT" --workers 3 \
    --input "$INPUT" --output data/output \
    --chunk-size 256 --headless --exit-after-done 6s >"$LOGDIR/master.log" 2>&1 &
MASTER_PID=$!
sleep 0.7

PIDS=()
for i in 1 2 3; do
    bin/mr-worker --master "http://127.0.0.1:$PORT" \
        --file-port $((9300 + i)) \
        --data-dir "$LOGDIR/worker$i" \
        --headless >"$LOGDIR/worker$i.log" 2>&1 &
    PIDS+=($!)
done

cleanup() {
    kill "$MASTER_PID" "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "waiting until the SHUFFLE phase begins, then killing worker 2 with SIGKILL…"
KILLED=0
DEADLINE=$((SECONDS + 240))
while [ $SECONDS -lt $DEADLINE ]; do
    PHASE=$(curl -s "http://127.0.0.1:$PORT/api/status" | sed -n 's/.*"phase":"\([a-z]*\)".*/\1/p' | head -1)
    if [ "$PHASE" = "shuffle" ] || [ "$PHASE" = "reduce" ]; then
        # Kill IMMEDIATELY (fast poll above): workers discover reduce tasks via
        # ~300ms idle polls, so a kill within ~100ms of the phase flip lands
        # before the fetches have drained the dead worker's files — with the
        # parallel shuffle a 0.4s grace delay finishes the whole phase first.
        kill -9 "${PIDS[1]}" || true
        PIDS[1]=""
        KILLED=1
        echo "worker 2 killed during $PHASE — expecting probe + MAP rollback"
        break
    fi
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        echo "master exited early — log tail:"; tail -40 "$LOGDIR/master.log"; exit 1
    fi
    sleep 0.05
done

if [ $KILLED -eq 0 ]; then
    echo "FAIL: never reached the shuffle phase in time"
    tail -20 "$LOGDIR/master.log"
    exit 1
fi

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
echo "== recovery events =="
if grep -E "UNREACHABLE|FAILED|rolling back|data-plane" "$LOGDIR/master.log" | head -12; then
    :
else
    echo "FAIL: the kill did not exercise the recovery path (no probe/rollback events)"
    echo "      — the shuffle probably completed before the kill landed."
    exit 1
fi

echo
echo "== verification against sequential reference =="
bin/mr-wcseq --input "$INPUT" --check data/output/wordcount.json

echo
echo "RESULT: SHUFFLE FAULT TOLERANCE PASS"
