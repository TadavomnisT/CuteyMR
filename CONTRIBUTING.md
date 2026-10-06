# Contributing to CuteyMR

Thanks for digging in. This project is deliberately small and
dependency-free — the whole point is that a distributed system can be
read end-to-end in an afternoon. Keep it that way.

## Ground rules

- **Pure Go standard library.** No third-party packages, no codegen
  beyond `go build`. If a change needs an import outside the stdlib,
  that is a design smell, not a dependency to add.
- **`make check` must stay green.** It runs `go build`, `go vet` and
  `gofmt -l` (the last one *prints nothing* when clean). CI runs it on
  every push.
- **Every behavior change ends with a verified wordcount.** The
  reference implementation (`cmd/wcseq`) shares the tokenizer with the
  cluster; a scenario that cannot be verified byte-for-byte against it
  is not done.

## Setup

```bash
go version          # any modern Go; CI uses "stable"
make check          # build + vet + gofmt
make demo           # 3 workers, race detector on, exact verification
```

## Layout map

```
internal/common/    protocol types, tokenizer, FNV partitioner, gzip JSON
internal/master/    HTTP control plane, pull scheduler, phases, fault
                    tolerance (heartbeat timeout, data-plane probe,
                    task requeue, MAP rollback), final merge
internal/worker/    per-core slot goroutines, combiner, P2P shuffle
                    fetches, file server, dashboard
internal/termui/    ANSI canvas renderer shared by both dashboards
cmd/                master, worker, mockgen (mock data), wcseq (verifier)
scripts/            run_demo.sh, the three fault-tolerance scenarios
docs/               the illustrated guide (MAPREDUCE-GUIDE.html)
```

## Testing philosophy

The battery (`make test`) is four scenarios, and each one kills a
worker at a different phase boundary:

| target      | kill window    | what it exercises                          |
|-------------|----------------|--------------------------------------------|
| `make demo` | no kill        | happy path + race detector + exact result  |
| `make ft`   | during MAP     | in-progress requeue on the task pull path  |
| `make ftshuffle` | during SHUFFLE | fetch failure → data-plane probe → MAP rollback |
| `make ftreduce`  | during REDUCE | heartbeat-timeout detection (nothing fails fast) |

Two lessons baked into the scripts, worth knowing before you extend them:

1. **Small vocabularies hide bugs.** The mock books collapse under the
   combiner to ~300 unique words, so reduce work is near-instant. Any
   test that needs reduce tasks to *take time* must feed high-entropy
   input (see `ftreduce`'s sequence-word generator).
2. **Windows should be deterministic.** `ftreduce` starts every worker
   with `--slow-reduce 900ms` — the fault-injection hold that keeps
   each reduce task in-progress — so the kill lands mid-reduce by
   construction. If you add a scenario with a timing window, add a
   matching `--slow-*` flag to the worker instead of polling faster.
3. **Be a good RAM citizen.** The sandbox below 4 GB also runs other
   things; job sizes in scripts are chosen to coexist (600k words, not
   1.2M — see the OOM note in `test_reduce_fault_tolerance.sh`).

## Adding a fault-tolerance scenario

1. Pick the window and how the master should notice (failed RPC, failed
   fetch/probe, heartbeat timeout).
2. Add a `--slow-*` flag to `internal/worker` if the window is racy;
   wire it through `worker.Config`.
3. Copy the closest script in `scripts/`, adjust the poll + kill,
   assert on the *master log lines* that name the recovery path, and
   always finish with `mr-wcseq --check`.
4. Add a `make` target, chain it into `make test`, and document it in
   `README.md` §Testing.

## Style notes

- `gofmt` is the whole style guide; CI fails on drift.
- Comments explain *why*, especially around the fault paths — the
  rollback dance is the part future-you will thank present-you for.
- Terminal dashboards are plain ANSI; keep them working over `script`
  (pty) and with `TERM=dumb` via the `--headless` flag.

## CI

`.github/workflows/ci.yml` runs `make check` on every push/PR and the
full `make test` battery on demand + weekly. It assumes the repo root
is the Go module (the layout of `cuteymr.zip`); see its header comment
if you nest the project in a parent repo.
