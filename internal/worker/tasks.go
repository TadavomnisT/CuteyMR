// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package worker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"cuteymr/internal/common"
)

func (w *Worker) mapFilePath(taskID string, partition int) string {
	return filepath.Join(w.dataDir, "tasks", taskID, fmt.Sprintf("part-%04d.json.gz", partition))
}

func (w *Worker) reduceFilePath(partition int) string {
	return filepath.Join(w.dataDir, "results", fmt.Sprintf("r-%04d.json.gz", partition))
}

// reportDone tells the master about a task outcome, retrying on transient
// errors (the master deduplicates reports). Cancellation aborts in-flight
// reports and the retry sleep so worker shutdown never stalls here.
func (w *Worker) reportDone(ctx context.Context, a common.TaskAssignment, success bool, errMsg string,
	stats common.TaskStats, partitionSizes []int64, resultURL string, failedSource string) {
	req := common.TaskDoneRequest{
		WorkerID:       w.ID,
		TaskID:         a.TaskID,
		Type:           a.Type,
		Success:        success,
		Error:          errMsg,
		FailedSource:   failedSource,
		Stats:          stats,
		PartitionSizes: partitionSizes,
		ResultURL:      resultURL,
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		var resp common.TaskDoneResponse
		if err := common.PostJSONCtx(ctx, w.cfg.MasterURL+"/api/task/done", req, &resp); err == nil {
			return
		} else {
			lastErr = err
		}
		if !success {
			break // failure reports are best effort
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	w.bumpStats(func(s *workerStats) {
		s.Errors++
	})
	w.log.Push("could not report %s %s to master: %v", a.Type, a.TaskID, lastErr)
}

func (w *Worker) failTask(ctx context.Context, a common.TaskAssignment, errMsg string, failedSource string) {
	w.bumpStats(func(s *workerStats) {
		s.Errors++
	})
	w.log.Push("task %s %s FAILED: %s", a.Type, a.TaskID, errMsg)
	w.reportDone(ctx, a, false, errMsg, common.TaskStats{}, nil, "", failedSource)
}

// executeMap runs one map task: download the input chunk, tokenize it,
// combine counts locally (the classic wordcount combiner), split the counts
// into R hash partitions and write them as gzipped JSON files that peers
// will fetch during the shuffle.
func (w *Worker) executeMap(ctx context.Context, idx int, a common.TaskAssignment) {
	w.setSlot(idx, a.TaskID, common.TaskMap, "fetching input")
	start := time.Now()

	input, err := common.GetBytesCtx(ctx, w.cfg.MasterURL+a.InputURL)
	if err != nil {
		w.failTask(ctx, a, "input fetch: "+err.Error(), "")
		w.clearSlot(idx)
		return
	}

	counts := make(map[string]int64, 1<<14)
	var tokens int64
	lastPos := 0
	total := len(input)
	common.TokenizeStream(string(input), func(word string, endPos int) {
		counts[word]++
		tokens++
		if endPos-lastPos >= 32768 {
			lastPos = endPos
			w.updateSlotProgress(idx, float64(endPos)/float64(total), tokens,
				fmt.Sprintf("scan %d%%", endPos*100/max(total, 1)))
		}
	})
	w.updateSlotProgress(idx, 1, tokens, "partitioning")

	R := a.NumPartitions
	if R <= 0 {
		R = 1
	}
	parts := make([]map[string]int64, R)
	for word, c := range counts {
		p := common.PartitionOf(word, R)
		if parts[p] == nil {
			parts[p] = make(map[string]int64)
		}
		parts[p][word] += c
	}
	sizes := make([]int64, R)
	var written int64
	for p := 0; p < R; p++ {
		m := parts[p]
		if m == nil {
			m = map[string]int64{}
		}
		n, err := common.WriteGzJSON(w.mapFilePath(a.TaskID, p), m)
		if err != nil {
			w.failTask(ctx, a, fmt.Sprintf("write partition %d: %v", p, err), "")
			w.clearSlot(idx)
			return
		}
		sizes[p] = n
		written += n
	}

	stats := common.TaskStats{
		DurationMS:   time.Since(start).Milliseconds(),
		Words:        tokens,
		UniqueWords:  int64(len(counts)),
		BytesRead:    int64(total),
		BytesWritten: written,
	}
	w.reportDone(ctx, a, true, "", stats, sizes, "", "")
	w.bumpStats(func(s *workerStats) {
		s.MapDone++
		s.WordsEmitted += tokens
		s.BytesRead += int64(total)
		s.BytesWritten += written
	})
	w.log.Push("slot %d: map %s done in %.1fs — %s words (%s unique) → %d partitions (%s)",
		idx, a.TaskID, float64(stats.DurationMS)/1000, common.Humanize(tokens),
		common.Humanize(int64(len(counts))), R, common.HumanBytes(written))
	w.clearSlot(idx)
}

// maxFetchConcurrency bounds how many peers a reduce task fetches from at
// once during the shuffle. Sequential fetching wastes the data plane when a
// partition has many sources; a small bound keeps per-slot memory predictable
// (fetched files are buffered in RAM while decoding) and plays nice with the
// per-core slot design.
const maxFetchConcurrency = 4

// fetchResult is one completed (or failed) shuffle fetch.
type fetchResult struct {
	src  common.FileSource
	data []byte
	err  error
}

// fetchPasses is how many times a reduce task re-tries failed shuffle
// fetches (the initial pass plus retries) before giving up on the task.
// Every individual fetch already retries internally with backoff; the extra
// passes exist because a busy peer can still drop a connection during a
// huge shuffle, and re-fetching a handful of files is far cheaper than
// failing (and re-running) the entire reduce task.
const fetchPasses = 3

// fetchSources downloads every intermediate partition file for a reduce task,
// up to maxFetchConcurrency at a time, and merges the decoded word counts.
// Sources that fail a pass are retried in later passes; only when files are
// still missing after fetchPasses attempts does the task fail. It returns the
// merged map, the total bytes fetched, and — on failure — the worker whose
// file could not be retrieved (the master probes that worker's data plane
// before re-queuing work).
func (w *Worker) fetchSources(ctx context.Context, idx int, sources []common.FileSource) (
	map[string]int64, int64, string, error) {

	n := len(sources)
	result := make(map[string]int64, 1<<15)
	var (
		fetchBytes int64
		done       int
	)
	pending := sources
	var last passResult
	for pass := 0; pass < fetchPasses && len(pending) > 0; pass++ {
		if pass > 0 {
			w.log.Push("slot %d: retrying %d/%d shuffle sources (pass %d of %d)",
				idx, len(pending), n, pass+1, fetchPasses)
		}
		last = w.fetchPass(ctx, idx, pending, result, done, n, fetchBytes)
		fetchBytes = last.fetchBytes
		done = last.done
		if ctx.Err() != nil {
			// our own shutdown, not a peer's fault — nothing to probe
			return result, fetchBytes, "", ctx.Err()
		}
		pending = last.failed
	}
	if len(pending) > 0 {
		return result, fetchBytes, last.failedSrc, last.err
	}
	return result, fetchBytes, "", nil
}

// passResult summarizes one fetch pass.
type passResult struct {
	failed     []common.FileSource
	fetchBytes int64
	done       int
	err        error
	failedSrc  string
}

// fetchPass concurrently fetches one pass of sources (up to
// maxFetchConcurrency at a time), merging every successful file into acc and
// returning the sources that failed this pass. A failing fetch does not
// abort the pass: collecting the complete failure set lets the next pass
// retry exactly what is missing instead of re-fetching everything.
//
// Every goroutine sends exactly one result into a buffered channel of
// capacity len(sources), so no sender ever blocks and all of them are
// guaranteed to exit once the channel is drained.
func (w *Worker) fetchPass(ctx context.Context, idx int, sources []common.FileSource, acc map[string]int64,
	done, total int, fetchBytes int64) passResult {

	concurrency := maxFetchConcurrency
	if concurrency > len(sources) {
		concurrency = len(sources)
	}
	results := make(chan fetchResult, len(sources))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range sources {
		wg.Add(1)
		go func(src common.FileSource) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results <- fetchResult{src: src, err: ctx.Err()}
				return
			}
			defer func() { <-sem }()
			data, err := common.GetBytesCtx(ctx, src.URL)
			results <- fetchResult{src: src, data: data, err: err}
		}(sources[i])
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	var out passResult
	out.fetchBytes = fetchBytes
	out.done = done
	for r := range results {
		if r.err != nil {
			if out.err == nil {
				if errors.Is(r.err, context.Canceled) && ctx.Err() != nil {
					out.err = r.err // our own shutdown, not the peer's fault
				} else {
					out.err = fmt.Errorf("shuffle fetch from %s: %v", r.src.WorkerID, r.err)
					out.failedSrc = r.src.WorkerID
				}
			}
			out.failed = append(out.failed, r.src)
			continue
		}
		var m map[string]int64
		if err := common.ReadGzJSON(r.data, &m); err != nil {
			if out.err == nil {
				out.err = fmt.Errorf("corrupt intermediate file %s: %v", r.src.TaskID, err)
				out.failedSrc = r.src.WorkerID
			}
			out.failed = append(out.failed, r.src)
			continue
		}
		for k, v := range m {
			acc[k] += v
		}
		out.fetchBytes += int64(len(r.data))
		out.done++
		w.updateSlotFetch(idx, out.done, total, out.fetchBytes, fmt.Sprintf("fetch %d/%d", out.done, total))
	}
	return out
}

// executeReduce runs one reduce task: fetch all intermediate partition files
// from the map workers (the shuffle), sum them up, store the result and tell
// the master where to download it from.
func (w *Worker) executeReduce(ctx context.Context, idx int, a common.TaskAssignment) {
	w.setSlot(idx, a.TaskID, common.TaskReduce, "starting")
	start := time.Now()

	// Fault-injection aid (--slow-reduce): hold the task in-progress for a
	// deterministic while before doing real work, so kill-during-REDUCE
	// tests get a guaranteed window instead of racing millisecond sums.
	if d := w.cfg.SlowReduce; d > 0 {
		w.updateSlotProgress(idx, 0, 0, "slow-reduce hold (fault injection)")
		select {
		case <-time.After(d):
		case <-ctx.Done():
			w.clearSlot(idx)
			return
		}
	}

	result, fetchBytes, failedSrc, err := w.fetchSources(ctx, idx, a.Sources)
	if err != nil {
		w.failTask(ctx, a, err.Error(), failedSrc)
		w.clearSlot(idx)
		return
	}
	total := len(a.Sources)
	w.updateSlotProgress(idx, 1, 0, "aggregating")

	var merged int64
	for _, v := range result {
		merged += v
	}
	size, err := common.WriteGzJSON(w.reduceFilePath(a.Partition), result)
	if err != nil {
		w.failTask(ctx, a, "write result: "+err.Error(), "")
		w.clearSlot(idx)
		return
	}
	resultURL := fmt.Sprintf("http://%s/files/reduce/%d", w.fileAddr, a.Partition)
	stats := common.TaskStats{
		DurationMS:   time.Since(start).Milliseconds(),
		Words:        merged,
		UniqueWords:  int64(len(result)),
		BytesRead:    fetchBytes,
		BytesWritten: size,
	}
	w.reportDone(ctx, a, true, "", stats, nil, resultURL, "")
	w.bumpStats(func(s *workerStats) {
		s.ReduceDone++
		s.WordsMerged += merged
		s.BytesRead += fetchBytes
		s.BytesWritten += size
	})
	w.log.Push("slot %d: reduce %s done in %.1fs — %s unique words from %d files (%s shuffled in)",
		idx, a.TaskID, float64(stats.DurationMS)/1000, common.Humanize(int64(len(result))),
		total, common.HumanBytes(fetchBytes))
	w.clearSlot(idx)
}
