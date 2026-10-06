// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package master

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cuteymr/internal/common"
)

// mergeAndFinalize downloads every reduce result from the workers,
// merges the R partitions into one wordcount and writes the output files.
func (m *Master) mergeAndFinalize() {
	type item struct{ id, url string }
	m.mu.Lock()
	var items []item
	for _, t := range m.reduceTasks {
		items = append(items, item{t.ID, t.ResultURL})
	}
	outDir := ""
	if m.job != nil {
		outDir = m.job.OutputDir
	}
	m.mu.Unlock()

	final := make(map[string]int64, 1<<16)
	for _, it := range items {
		data, err := common.GetBytes(it.url)
		if err == nil {
			var part map[string]int64
			if err = common.ReadGzJSON(data, &part); err == nil {
				for k, v := range part {
					final[k] += v
				}
				m.mu.Lock()
				if rt := m.reduceTaskByID(it.id); rt != nil {
					rt.Consumed = true
				}
				m.mu.Unlock()
				continue
			}
		}
		// The result file is gone (worker died mid-merge): re-queue it.
		m.mu.Lock()
		if m.job != nil && m.job.Phase == common.PhaseMerge {
			if rt := m.reduceTaskByID(it.id); rt != nil && rt.State == common.TaskCompleted && !rt.Consumed {
				rt.State = common.TaskIdle
				rt.Holder = ""
				rt.ResultURL = ""
				m.job.Phase = common.PhaseReduce
				m.log.Push("merge: result of %s unavailable (%v) — re-queued", it.id, err)
			}
		}
		m.mu.Unlock()
		return
	}

	if err := m.writeOutputs(outDir, final); err != nil {
		m.mu.Lock()
		m.log.Push("FATAL: cannot write output: %v", err)
		m.markDoneLocked(common.PhaseFailed, err.Error())
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	var totalWords int64
	for _, c := range final {
		totalWords += c
	}
	m.job.TotalWords = totalWords
	m.job.UniqueWords = int64(len(final))
	m.job.MergeEndedAt = time.Now()
	m.topWords = topN(final, 8)
	dur := m.job.MergeEndedAt.Sub(m.job.StartedAt)
	m.markDoneLocked(common.PhaseDone, "")
	m.log.Push("────────────────────────────────────────────────────────────")
	m.log.Push("JOB COMPLETE in %s — %s total words · %s unique", termDur(dur),
		common.Humanize(totalWords), common.Humanize(m.job.UniqueWords))
	m.log.Push("final result stored in %s (wordcount.json / wordcount.tsv / top100.txt / job-summary.json)", outDir)
	m.log.Push("────────────────────────────────────────────────────────────")
}

func topN(final map[string]int64, n int) []topWord {
	type kv struct {
		w string
		c int64
	}
	arr := make([]kv, 0, len(final))
	for w, c := range final {
		arr = append(arr, kv{w, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].c != arr[j].c {
			return arr[i].c > arr[j].c
		}
		return arr[i].w < arr[j].w
	})
	if len(arr) > n {
		arr = arr[:n]
	}
	out := make([]topWord, len(arr))
	for i, e := range arr {
		out[i] = topWord{Word: e.w, Count: e.c}
	}
	return out
}

// writeOutputs stores the final wordcount in several formats.
func (m *Master) writeOutputs(outDir string, final map[string]int64) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	// 1) wordcount.json — map marshalled with sorted keys
	data, err := json.MarshalIndent(final, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "wordcount.json"), data, 0o644); err != nil {
		return err
	}

	// 2) wordcount.tsv + top100.txt — sorted by count desc, word asc
	sorted := topN(final, len(final))
	var tsv strings.Builder
	for _, e := range sorted {
		fmt.Fprintf(&tsv, "%s\t%d\n", e.Word, e.Count)
	}
	if err := os.WriteFile(filepath.Join(outDir, "wordcount.tsv"), []byte(tsv.String()), 0o644); err != nil {
		return err
	}

	var top strings.Builder
	var max int64
	limit := sorted
	if len(limit) > 100 {
		limit = limit[:100]
	}
	for _, e := range limit {
		if e.Count > max {
			max = e.Count
		}
	}
	fmt.Fprintf(&top, "TOP WORDS (of %s unique, %s total)\n", common.Humanize(int64(len(final))), common.Humanize(m.jobTotalWords(final)))
	fmt.Fprintf(&top, "%-18s %10s  %s\n", "WORD", "COUNT", "FREQUENCY")
	for _, e := range limit {
		barLen := 0
		if max > 0 {
			barLen = int(float64(e.Count) / float64(max) * 36)
		}
		fmt.Fprintf(&top, "%-18s %10d  %s\n", e.Word, e.Count, strings.Repeat("█", barLen))
	}
	if err := os.WriteFile(filepath.Join(outDir, "top100.txt"), []byte(top.String()), 0o644); err != nil {
		return err
	}
	return nil
}

func (m *Master) jobTotalWords(final map[string]int64) int64 {
	var n int64
	for _, c := range final {
		n += c
	}
	return n
}

// writeSummary stores a job report with timings and per-worker statistics.
// Called by the dashboard layer once the job is done.
func (m *Master) writeSummary() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.job == nil || m.job.Phase != common.PhaseDone {
		return
	}
	if _, err := os.Stat(filepath.Join(m.job.OutputDir, "job-summary.json")); err == nil {
		return // already written
	}
	type workerStat struct {
		ID         string  `json:"id"`
		Host       string  `json:"host"`
		Cores      int     `json:"cores"`
		TasksDone  int     `json:"tasks_done"`
		MapDone    int     `json:"map_done"`
		ReduceDone int     `json:"reduce_done"`
		Words      int64   `json:"words_counted"`
		Merged     int64   `json:"pairs_merged"`
		RatePeak   float64 `json:"rate_peak_words_per_sec"`
	}
	summary := struct {
		Job struct {
			Name        string `json:"name"`
			InputFiles  int    `json:"input_files"`
			InputBytes  int64  `json:"input_bytes"`
			MapTasks    int    `json:"map_tasks"`
			ReduceTasks int    `json:"reduce_tasks"`
			TotalWords  int64  `json:"total_words"`
			UniqueWords int64  `json:"unique_words"`
			OutputDir   string `json:"output_dir"`
		} `json:"job"`
		Timings struct {
			TotalMS   int64 `json:"total_ms"`
			MapMS     int64 `json:"map_ms"`
			ShuffleMS int64 `json:"shuffle_ms"`
			ReduceMS  int64 `json:"reduce_ms"`
			MergeMS   int64 `json:"merge_ms"`
		} `json:"timings"`
		Workers []workerStat `json:"workers"`
	}{}
	summary.Job.Name = m.job.Name
	summary.Job.InputFiles = len(m.job.InputFiles)
	summary.Job.InputBytes = m.job.TotalBytes
	summary.Job.MapTasks = m.job.M
	summary.Job.ReduceTasks = m.job.R
	summary.Job.TotalWords = m.job.TotalWords
	summary.Job.UniqueWords = m.job.UniqueWords
	summary.Job.OutputDir = m.job.OutputDir

	summary.Timings.TotalMS = m.job.MergeEndedAt.Sub(m.job.StartedAt).Milliseconds()
	if !m.job.MapEndedAt.IsZero() {
		summary.Timings.MapMS = m.job.MapEndedAt.Sub(m.job.StartedAt).Milliseconds()
	}
	if !m.job.ReduceStart.IsZero() && !m.job.ReduceEndedAt.IsZero() {
		summary.Timings.ShuffleMS = m.job.ReduceStart.Sub(m.job.MapEndedAt).Milliseconds()
		summary.Timings.ReduceMS = m.job.ReduceEndedAt.Sub(m.job.ReduceStart).Milliseconds()
	}
	if !m.job.MergeEndedAt.IsZero() {
		summary.Timings.MergeMS = m.job.MergeEndedAt.Sub(m.job.ReduceEndedAt).Milliseconds()
	}
	for _, id := range m.order {
		w := m.workers[id]
		peak := 0.0
		for _, v := range w.History {
			if v > peak {
				peak = v
			}
		}
		summary.Workers = append(summary.Workers, workerStat{
			ID: w.ID, Host: w.FileAddr, Cores: w.Cores, TasksDone: w.TasksDone,
			MapDone: w.MapDone, ReduceDone: w.ReduceDone, Words: w.WordsCounted,
			Merged: w.WordsMerged, RatePeak: peak,
		})
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(m.job.OutputDir, "job-summary.json"), data, 0o644)
}
