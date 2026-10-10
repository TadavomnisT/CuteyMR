# CuteyMR — MapReduce WordCount in pure Go

A small but complete distributed **MapReduce** system that solves the classic
**word count** problem: one **master**, any number of **workers**, talking over
plain HTTP+JSON — with **zero external dependencies** (Go standard library only).

```
                     ┌─────────────────┐
                     │  ♛ MR-MASTER    │  :8080  (control plane + input)
                     └────────┬────────┘
        ┌─────────────────────┼─────────────────────┐
   ┌────┴─────┐         ┌────┴─────┐         ┌────┴─────┐
   │ worker 1 │◄───────►│ worker 2 │◄───────►│ worker 3 │   peer-to-peer
   │ 8 cores  │ shuffle │ 4 cores  │ shuffle │ 4 cores  │   data plane
   └──────────┘         └──────────┘         └──────────┘
```

- **Auto-start** — the job begins the moment the target number of workers is connected.
- **All-core parallelism** — every worker runs one goroutine per CPU core (map or reduce tasks).
- **Real shuffle** — intermediate partition files move peer-to-peer between workers; the master never relays bulk data.
- **Fault tolerant** — heartbeat timeouts, active data-plane probes, task re-queuing and MAP-phase rollback.
- **Workers are daemons** — like the master, workers stay online until you press `Ctrl+C`; they survive job completion, job failure and even master restarts (re-registering automatically to join the next job).
- **Live terminal dashboards** for master and every worker, including an animated cluster topology with moody worker faces `(>_<)`.
- **Verified** — results are compared word-for-word against a sequential reference implementation (and it passes, including `kill -9` chaos tests).

## Installation

```bash
git clone https://github.com/TadavomnisT/CuteyMR.git
cd CuteyMR
make build
```

## Quick start

```bash
make build      # builds bin/mr-master, bin/mr-worker, bin/mr-mockgen, bin/mr-wcseq
make mock       # generates ~3MB of mock documents into data/input

# terminal 1
bin/mr-master --port 8080 --workers 3 --input data/input --output data/output

# terminals 2..4 (file ports must differ when on one machine)
bin/mr-worker --master http://127.0.0.1:8080 --file-port 9101
bin/mr-worker --master http://127.0.0.1:8080 --file-port 9102
bin/mr-worker --master http://127.0.0.1:8080 --file-port 9103
```

Watch the dashboards. When the job completes the master parks on the results
screen and the workers idle online (stop them with `Ctrl+C` whenever you
like); the final word counts are stored in `data/output/`:

| file | contents |
|---|---|
| `wordcount.json` | `{"word": count, ...}` (sorted keys) |
| `wordcount.tsv` | `word\tcount`, sorted by count desc |
| `top100.txt` | pretty-printed top 100 with frequency bars |
| `job-summary.json` | phase timings and per-worker statistics |

## Multiple machines (same LAN)

```bash
# build per target once — Go cross-compiles trivially
GOOS=linux GOARCH=amd64 go build -o dist/linux-amd64/ ./cmd/...

# machine A (master)
./mr-master --port 8080 --workers 2 --input ~/books --output ~/results

# machines B, C
./mr-worker --master http://<A-IP>:8080 --file-port 9101 --advertise <THIS-IP>
```

Open TCP 8080 on the master and the workers' `--file-port` in your firewalls —
the shuffle is peer-to-peer. See `docs/MAPREDUCE-GUIDE.md` §10 (or the
illustrated `docs/MAPREDUCE-GUIDE.html`) for the full walkthrough.

## Testing

```bash
make demo                              # 3 workers + race detector, verifies result exactly
make ft                                # kill -9 a worker during MAP, still verifies exactly
make ftshuffle                         # kill -9 a worker during SHUFFLE (probe + MAP rollback)
make ftreduce                          # kill -9 a worker during REDUCE (heartbeat timeout + MAP rollback)
make test                              # the full battery: demo + ft + ftshuffle + ftreduce
make check                             # static checks: build + vet + gofmt clean
bash scripts/verify.sh                 # re-verify data/output any time
curl localhost:8080/api/status         # live JSON snapshot of the cluster
```

`ftreduce` is deterministic: every worker runs with `--slow-reduce 900ms`,
a fault-injection aid that holds each reduce task in-progress before its
fetches, so the kill is guaranteed to land mid-reduce instead of racing
millisecond sums (`SLOW_REDUCE=0 make ftreduce` restores the pure timing
version). The flag is also handy when reproducing failures by hand:

```bash
./mr-worker --master http://127.0.0.1:8080 --slow-reduce 2s   # widen the reduce window
```

Every scenario verifies the final wordcount **byte-for-byte** against
`mr-wcseq`, a sequential reference implementation sharing the same tokenizer.

Static checks (CI-friendly, zero config) — identical to `make check`:

```bash
go build ./... && go vet ./... && test -z "$(gofmt -l .)"
```

## Continuous integration

`.github/workflows/ci.yml` ships the whole battery as CI:

- **check** job — `make check` (build + vet + gofmt) on every push and PR.
- **test** job — `make test` (the demo plus all three fault-tolerance
  scenarios: kill during MAP, SHUFFLE and REDUCE) on demand
  (`workflow_dispatch`), weekly, and after every passing check on
  main/master. Logs and outputs are uploaded as artifacts on failure.

No external tools required — plain Go, make and bash, so the same workflow
works on any GitHub repo you push this to. The workflow assumes the
repository root is the Go module — the exact layout of `cuteymr.zip`; if
you nest `cuteymr/` inside a parent repo, add `working-directory: cuteymr`
to each run step (the file's header comment explains).

## Project layout

```
cmd/{master,worker,mockgen,wcseq}   small mains, one binary each
internal/common/                    wire protocol, tokenizer, FNV partitioner, gzip JSON
internal/termui/                    dependency-free ANSI terminal canvas
internal/master/                    scheduler, HTTP control plane, merge, dashboard
internal/worker/                    slot goroutines, map/reduce tasks, file server, dashboard
scripts/                            demo + fault-tolerance + verify scripts
docs/MAPREDUCE-GUIDE.md             the full guide in Markdown (GitHub renders it natively)
docs/MAPREDUCE-GUIDE.html           the same guide, fully illustrated (inline SVG + art)
docs/images/                        hero art, parable illustration, figure SVGs
```

The guide explains MapReduce from zero (with a library run by femboys),
walks a complete worked example with real hash values, documents the architecture,
protocol, fault tolerance and dashboards, and shows how to run everything.

## Flags

**mr-master**: `--port` (8080) · `--workers` (2) · `--input` (data/input, dir or files) ·
`--output` (data/output) · `--chunk-size` (512 KiB, word-aligned) · `--partitions`
(0 = auto: 2×workers, min 4) · `--heartbeat-timeout` (10s, worker liveness deadline)
· `--headless` · `--exit-after-done` (e.g. `5s`)

**mr-worker**: `--master` (http://127.0.0.1:8080) · `--file-port` (0 = ephemeral) ·
`--advertise` (auto) · `--data-dir` (fresh temp dir) · `--heartbeat` (1s, raise on
slow/WAN links) · `--slow-reduce` (0 = off; fault-injection hold per reduce task) ·
`--headless`

Requires Go 1.21+. No third-party packages — `go.mod` has no dependencies.

Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for the ground
rules (stdlib-only, `make check` green, every change verified against the
sequential reference) and how to add a fault-tolerance scenario.

## License

CuteyMR is free software: you can redistribute it and/or modify it under the
terms of the **GNU General Public License** as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version — see [LICENSE](LICENSE). Every source file carries an
`SPDX-License-Identifier: GPL-3.0-or-later` header saying the same thing.
