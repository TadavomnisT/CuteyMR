# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (c) 2025 The CuteyMR Authors

GO ?= go
BIN := bin

.PHONY: build race mock demo verify ft ftshuffle ftreduce test check fmt vet clean archive

build:
	$(GO) build -o $(BIN)/mr-master ./cmd/master
	$(GO) build -o $(BIN)/mr-worker ./cmd/worker
	$(GO) build -o $(BIN)/mr-mockgen ./cmd/mockgen
	$(GO) build -o $(BIN)/mr-wcseq ./cmd/wcseq

# static checks for CI: compile, vet, and assert no unformatted files
check:
	$(GO) build ./...
	$(GO) vet ./...
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
	        echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi; \
	echo "check: build + vet + gofmt all clean"

# build with the race detector (used by scripts/run_demo.sh)
race:
	$(GO) build -race -o $(BIN)/mr-master ./cmd/master
	$(GO) build -race -o $(BIN)/mr-worker ./cmd/worker
	$(GO) build -race -o $(BIN)/mr-mockgen ./cmd/mockgen
	$(GO) build -race -o $(BIN)/mr-wcseq ./cmd/wcseq

mock:
	$(GO) run ./cmd/mockgen --out data/input

demo:
	bash scripts/run_demo.sh

verify:
	bash scripts/verify.sh

# kill one worker during the map phase and verify the final wordcount
ft:
	bash scripts/test_fault_tolerance.sh

# kill one worker during the shuffle phase (probe + MAP rollback path)
ftshuffle:
	bash scripts/test_shuffle_fault_tolerance.sh

# kill one worker during the reduce phase (heartbeat timeout + MAP rollback path)
ftreduce:
	bash scripts/test_reduce_fault_tolerance.sh

# run the full test battery (all three failure windows: map, shuffle, reduce)
test: demo ft ftshuffle ftreduce

fmt:
	gofmt -l -w .

vet:
	$(GO) vet ./...

clean:
	rm -rf $(BIN) data/output

archive:
	zip -r cuteymr.zip . -x "bin/*" -x "data/output/*" -x "*.zip" -x "*/.DS_Store"
