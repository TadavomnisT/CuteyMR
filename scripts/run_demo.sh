#!/usr/bin/env bash

# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (c) 2025 The CuteyMR Authors
# run_demo.sh — fully automated end-to-end test of the CuteyMR cluster.
#
# It builds with the race detector, generates mock data, boots 1 master and
# 3 workers (headless), waits for the job to finish and verifies the final
# wordcount against the sequential reference implementation.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS=${WORKERS:-3}
PORT=${PORT:-8080}
LOGDIR=${LOGDIR:-/tmp/cuteymr-demo}
INPUT=${INPUT:-data/input}

echo "== CuteyMR end-to-end demo =="
echo "building (with race detector)…"
make race

if [ ! -f "$INPUT/voyage_of_the_star_whale.txt" ]; then
    echo "generating mock data…"
    bin/mr-mockgen --out "$INPUT"
fi

rm -rf data/output "$LOGDIR"
mkdir -p "$LOGDIR"

echo "starting master on :$PORT (waiting for $WORKERS workers)…"
bin/mr-master \
    --port "$PORT" --workers "$WORKERS" \
    --input "$INPUT" --output data/output \
    --headless --exit-after-done 4s >"$LOGDIR/master.log" 2>&1 &
MASTER_PID=$!

sleep 0.7
PIDS=()
for i in $(seq 1 "$WORKERS"); do
    PORT_FILE=$((9100 + i))
    bin/mr-worker \
        --master "http://127.0.0.1:$PORT" \
        --file-port "$PORT_FILE" \
        --data-dir "$LOGDIR/worker$i" \
        --headless >"$LOGDIR/worker$i.log" 2>&1 &
    PIDS+=($!)
done

cleanup() {
    kill "$MASTER_PID" "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "waiting for the job to complete…"
DEADLINE=$((SECONDS + 180))
while [ $SECONDS -lt $DEADLINE ]; do
    if [ -f data/output/wordcount.json ]; then break; fi
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        echo "master exited early — log tail:"
        tail -40 "$LOGDIR/master.log" || true
        exit 1
    fi
    sleep 1
done

if [ ! -f data/output/wordcount.json ]; then
    echo "TIMEOUT: job did not finish in ${DEADLINE}s"
    tail -40 "$LOGDIR/master.log" || true
    exit 1
fi

# give the master time to write summaries and shut the workers down
wait "$MASTER_PID" || true

echo
echo "== verification against sequential reference =="
bin/mr-wcseq --input "$INPUT" --check data/output/wordcount.json

echo
echo "== job summary =="
cat data/output/job-summary.json 2>/dev/null || echo "(no summary written)"

echo
echo "== top 12 words =="
head -13 data/output/top100.txt

echo
echo "all logs kept in $LOGDIR"
echo "RESULT: PASS"
