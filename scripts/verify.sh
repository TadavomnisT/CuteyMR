#!/usr/bin/env bash

# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (c) 2025 The CuteyMR Authors
# verify.sh — check an existing run's output (data/output/wordcount.json)
# against the sequential reference implementation.
set -euo pipefail
cd "$(dirname "$0")/.."

INPUT=${INPUT:-data/input}
OUT=${OUT:-data/output/wordcount.json}

if [ ! -f "$OUT" ]; then
    echo "no $OUT found — run scripts/run_demo.sh first"
    exit 1
fi

make build >/dev/null
bin/mr-wcseq --input "$INPUT" --check "$OUT"
