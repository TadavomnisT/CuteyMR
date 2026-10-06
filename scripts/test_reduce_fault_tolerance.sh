#!/usr/bin/env bash

# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (c) 2025 The CuteyMR Authors
# test_reduce_fault_tolerance.sh — kills one worker *during the reduce
# phase* (after the shuffle has fully drained), exercising the third and
# last failure window of the job:
#
#   1. nothing is mid-fetch anymore, so no failed shuffle fetch exists to
#      trigger the data-plane probe — the death is detected purely by the
#      HEARTBEAT TIMEOUT monitor
#   2. in-progress reduce tasks on the dead worker are re-queued
#   3. completed map tasks whose intermediate files lived on the dead
#      worker are re-queued and the job ROLLS BACK to the map phase
#   4. reduce partitions already completed by live workers are KEPT
#      (their results are still valid — only the lost work is redone)
#   5. the cluster still produces a byte-for-byte exact wordcount
#
# The standard mock books cannot exercise this window: their tiny
# vocabulary (~300 unique words) collapses under the combiner, so reduce
# tasks finish in milliseconds. This test therefore feeds a deterministic
# high-entropy document — WORDS unique sequence words w000000001… — which
# the combiner cannot collapse, forcing each reduce task to genuinely sum
# ~WORDS/4 unique words after its fetches drain.
#
# DETERMINISM: every worker additionally runs with --slow-reduce
# (default 900ms), the fault-injection aid that holds each reduce task
# in-progress before its fetches. The reduce phase therefore cannot end
# before the hold elapses, so the 30ms phase-poll below is guaranteed to
# catch it and the kill is guaranteed to land mid-reduce — no more racing
# millisecond sums. Set SLOW_REDUCE=0 to run the pure timing version.
#
# Together with test_fault_tolerance.sh (kill during MAP) and
# test_shuffle_fault_tolerance.sh (kill during SHUFFLE) this completes
# the failure-window trilogy: map, shuffle, reduce.
set -euo pipefail
cd "$(dirname "$0")/.."

LOGDIR=/tmp/cuteymr-rft
INPUT=/tmp/cuteymr-reduceinput   # deterministic; regenerated only if missing
WORDS=${WORDS:-600000}           # ~6.6MB — big enough to make reduce tasks
                                 # do real summing work, small enough to stay
                                 # a good RAM citizen next to other processes
PORT=${PORT:-8083}
SLOW_REDUCE=${SLOW_REDUCE:-900ms}    # --slow-reduce hold per reduce task; the
                                    # kill window becomes deterministic.
                                    # Set SLOW_REDUCE=0 to disable (timing race).

echo "== CuteyMR reduce-phase fault tolerance test =="
make race >/dev/null

rm -rf "$LOGDIR" data/output
mkdir -p "$LOGDIR" "$INPUT"

if [ ! -f "$INPUT/unique_words.txt" ] || [ "$(wc -l < "$INPUT/unique_words.txt")" -ne "$WORDS" ]; then
    echo "generating high-entropy input (${WORDS} unique sequence words, ~6.6MB)…"
    awk -v n="$WORDS" 'BEGIN { for (i = 1; i <= n; i++) printf "w%09d\n", i }' \
        > "$INPUT/unique_words.txt"
fi

# 4 partitions x ~WORDS/4 unique words each => reduce tasks do real summing
# work after the fetches drain; --slow-reduce below widens the kill window
# from "hope" to a guaranteed >=900ms hold.
SLOW_ARGS=()
if [ "$SLOW_REDUCE" != "0" ]; then
    SLOW_ARGS+=(--slow-reduce "$SLOW_REDUCE")
fi
bin/mr-master --port "$PORT" --workers 3 --partitions 4 \
    --heartbeat-timeout 3s \
    --input "$INPUT" --output data/output \
    --chunk-size 256 --headless --exit-after-done 6s >"$LOGDIR/master.log" 2>&1 &
MASTER_PID=$!
sleep 0.7

PIDS=()
for i in 1 2 3; do
    bin/mr-worker --master "http://127.0.0.1:$PORT" \
        --file-port $((9400 + i)) \
        --data-dir "$LOGDIR/worker$i" \
        "${SLOW_ARGS[@]}" \
        --headless >"$LOGDIR/worker$i.log" 2>&1 &
    PIDS+=($!)
done

cleanup() {
    kill "$MASTER_PID" "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "waiting until the REDUCE phase begins (shuffle fully drained), then killing worker 2 with SIGKILL…"
KILLED=0
DEADLINE=$((SECONDS + 240))
while [ $SECONDS -lt $DEADLINE ]; do
    PHASE=$(curl -s "http://127.0.0.1:$PORT/api/status" | sed -n 's/.*"phase":"\([a-z]*\)".*/\1/p' | head -1)
    if [ "$PHASE" = "reduce" ]; then
        # Kill IMMEDIATELY: the phase flipped because every partition file
        # was pulled, so no in-flight fetch exists to fail fast — only the
        # heartbeat timeout can discover this death. With --slow-reduce,
        # every reduce task is still inside its hold right now.
        kill -9 "${PIDS[1]}" || true
        PIDS[1]=""
        KILLED=1
        echo "worker 2 killed during REDUCE — expecting heartbeat timeout + reduce requeue + MAP rollback"
        break
    fi
    if [ "$PHASE" = "merge" ] || [ "$PHASE" = "done" ]; then
        echo "FAIL: reduce phase passed too quickly to kill into (reached $PHASE)"
        tail -20 "$LOGDIR/master.log"
        exit 1
    fi
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        echo "master exited early — log tail:"; tail -40 "$LOGDIR/master.log"; exit 1
    fi
    sleep 0.03
done

if [ $KILLED -eq 0 ]; then
    echo "FAIL: never reached the reduce phase in time"
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
echo "-- heartbeat detection + reduce requeue --"
grep -E "UNREACHABLE \(heartbeat timeout\)|reduce r[0-9]+ FAILED" "$LOGDIR/master.log" | head -8 || true
echo "-- map rollback --"
grep -E "map m[0-9]+ FAILED|rolling back to MAP" "$LOGDIR/master.log" | head -8 || true

if ! grep -q "UNREACHABLE (heartbeat timeout)" "$LOGDIR/master.log"; then
    echo "FAIL: the kill was not detected via heartbeat timeout"
    echo "      — with --slow-reduce the reduce phase cannot have finished;"
    echo "        check that bin/mr-worker supports --slow-reduce (rebuild)."
    exit 1
fi
if ! grep -q "rolling back to MAP phase" "$LOGDIR/master.log"; then
    echo "FAIL: no MAP rollback happened — the lost intermediate files were not re-processed."
    exit 1
fi

echo
echo "== verification against sequential reference =="
bin/mr-wcseq --input "$INPUT" --check data/output/wordcount.json

echo
echo "RESULT: REDUCE FAULT TOLERANCE PASS"
