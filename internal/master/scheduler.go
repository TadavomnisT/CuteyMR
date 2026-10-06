// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package master

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cuteymr/internal/common"
)

func isSpaceByte(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	}
	return false
}

// gatherInputFiles expands the configured input paths (files and/or
// directories) into a sorted list of files.
func (m *Master) gatherInputFiles() ([]string, int64) {
	var files []string
	for _, p := range m.cfg.InputPaths {
		info, err := os.Stat(p)
		if err != nil {
			m.log.Push("warning: input path %s inaccessible (%v)", p, err)
			continue
		}
		if info.IsDir() {
			entries, err := os.ReadDir(p)
			if err != nil {
				m.log.Push("warning: cannot read dir %s (%v)", p, err)
				continue
			}
			var names []string
			for _, e := range entries {
				if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
					names = append(names, e.Name())
				}
			}
			sort.Strings(names)
			for _, n := range names {
				files = append(files, filepath.Join(p, n))
			}
		} else {
			files = append(files, p)
		}
	}
	var total int64
	for _, f := range files {
		if st, err := os.Stat(f); err == nil {
			total += st.Size()
		}
	}
	return files, total
}

// splitFileChunks cuts one file into chunks of ~chunkSize bytes, always at a
// whitespace boundary so no word is ever cut in half. The file is streamed
// (never loaded whole) so master memory stays bounded for any input size.
func splitFileChunks(path string, chunkSize int64) []Chunk {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		return nil
	}
	n := info.Size()
	name := filepath.Base(path)
	var chunks []Chunk
	var start int64
	// 64 KiB look-ahead window used to find the chunk boundary whitespace.
	const window = 64 << 10
	buf := make([]byte, window)
	for start < n {
		end := start + chunkSize
		if end >= n {
			end = n
		} else {
			// advance to the next whitespace so we never split inside a word
			off := end
			for off < n {
				toRead := int64(window)
				if off+toRead > n {
					toRead = n - off
				}
				r, rerr := f.ReadAt(buf[:toRead], off)
				if r == 0 {
					end = n
					break
				}
				found := false
				for i := 0; i < r; i++ {
					if isSpaceByte(buf[i]) {
						end = off + int64(i)
						found = true
						break
					}
				}
				if found || rerr != nil {
					break
				}
				off += int64(r)
			}
		}
		if end <= start {
			end = start + 1
		}
		chunks = append(chunks, Chunk{File: path, Name: name, Offset: start, Length: end - start})
		start = end
	}
	return chunks
}

func (m *Master) mapTaskByID(id string) *MapTask {
	for _, t := range m.mapTasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (m *Master) reduceTaskByID(id string) *ReduceTask {
	for _, t := range m.reduceTasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// Register handles worker registration (or re-registration after a restart).
func (m *Master) Register(req common.RegisterRequest) common.RegisterResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.workers[req.WorkerID]; ok {
		if old.Alive {
			return common.RegisterResponse{Accepted: false, Message: "worker id is already active"}
		}
		delete(m.workers, req.WorkerID)
		for i, id := range m.order {
			if id == req.WorkerID {
				m.order = append(m.order[:i], m.order[i+1:]...)
				break
			}
		}
	}
	fileAddr := fmt.Sprintf("%s:%d", req.Host, req.FilePort)
	w := &WorkerInfo{
		ID:           req.WorkerID,
		Hostname:     req.Hostname,
		Host:         req.Host,
		FilePort:     req.FilePort,
		FileAddr:     fileAddr,
		Cores:        req.Cores,
		MemoryMB:     req.MemoryMB,
		RegisteredAt: time.Now(),
		LastSeen:     time.Now(),
		Alive:        true,
		Active:       make(map[string]common.ActiveTaskInfo),
	}
	m.workers[req.WorkerID] = w
	m.order = append(m.order, req.WorkerID)
	alive := m.aliveCountLocked()
	m.log.Push("worker %s joined — %d cores, %s RAM, file server %s", req.WorkerID, req.Cores,
		common.HumanBytes(req.MemoryMB<<20), fileAddr)

	if m.job == nil && alive >= m.cfg.TargetWorkers {
		m.startJobLocked()
	}

	resp := common.RegisterResponse{
		Accepted:      true,
		TargetWorkers: m.cfg.TargetWorkers,
		AliveWorkers:  alive,
	}
	if m.job != nil {
		resp.NumPartitions = m.job.R
		resp.Phase = m.job.Phase
	} else {
		resp.Phase = common.PhaseWaiting
	}
	return resp
}

// startJobLocked kicks off the wordcount job once enough workers are online.
func (m *Master) startJobLocked() {
	files, total := m.gatherInputFiles()
	var chunks []Chunk
	for _, f := range files {
		chunks = append(chunks, splitFileChunks(f, m.cfg.ChunkSize)...)
	}
	alive := m.aliveCountLocked()
	R := m.cfg.NumPartitions
	if R <= 0 {
		R = 2 * alive
		if R < 4 {
			R = 4
		}
	}
	for i, ch := range chunks {
		m.mapTasks = append(m.mapTasks, &MapTask{ID: fmt.Sprintf("m%04d", i), Chunk: ch, State: common.TaskIdle})
	}
	for r := 0; r < R; r++ {
		m.reduceTasks = append(m.reduceTasks, &ReduceTask{ID: fmt.Sprintf("r%04d", r), Partition: r, State: common.TaskIdle})
	}
	m.job = &JobInfo{
		Name:       "wordcount",
		InputFiles: files,
		TotalBytes: total,
		M:          len(m.mapTasks),
		R:          R,
		Phase:      common.PhaseMap,
		StartedAt:  time.Now(),
		OutputDir:  m.cfg.OutputDir,
	}
	m.log.Push("────────────────────────────────────────────────────────────")
	m.log.Push("ALL %d WORKERS CONNECTED — starting wordcount job", alive)
	m.log.Push("input: %d files (%s) split into %d map tasks · %d reduce tasks (R=%d)",
		len(files), common.HumanBytes(total), len(m.mapTasks), R, R)
	m.log.Push("────────────────────────────────────────────────────────────")
	if len(m.mapTasks) == 0 {
		m.log.Push("no input data — jumping straight to shuffle/reduce")
		m.prepareShuffleLocked()
	}
}

// Heartbeat processes a worker heartbeat and returns cluster state.
func (m *Master) Heartbeat(req common.HeartbeatRequest) common.HeartbeatResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.workers[req.WorkerID]
	if w == nil {
		return common.HeartbeatResponse{OK: false, Message: "unknown worker — register again"}
	}
	now := time.Now()
	if !w.Alive {
		w.Alive = true
		w.prevSample = time.Time{}
		m.log.Push("worker %s is back online", w.ID)
	}
	w.LastSeen = now
	w.TasksDone = req.TasksCompleted
	w.MapDone = req.TasksMapDone
	w.ReduceDone = req.TasksReduceDone
	w.WordsEmitted = req.WordsEmitted
	w.WordsMerged = req.WordsMerged
	w.BytesRead = req.BytesRead
	w.BytesWritten = req.BytesWritten
	active := make(map[string]common.ActiveTaskInfo, len(req.Active))
	for _, a := range req.Active {
		active[a.TaskID] = a
	}
	w.Active = active

	if w.prevSample.IsZero() {
		w.prevSample = now
		w.prevWords = req.WordsEmitted
	} else if dt := now.Sub(w.prevSample).Seconds(); dt >= 0.5 {
		inst := float64(req.WordsEmitted-w.prevWords) / dt
		if inst < 0 {
			inst = 0
		}
		w.Rate = 0.55*inst + 0.45*w.Rate
		w.prevWords = req.WordsEmitted
		w.prevSample = now
		w.History = append(w.History, w.Rate)
		if len(w.History) > 80 {
			w.History = w.History[len(w.History)-80:]
		}
	}

	m.recomputeShuffleProgressLocked()
	m.maybeAdvanceShuffleLocked()

	resp := common.HeartbeatResponse{
		OK:            true,
		Phase:         common.PhaseWaiting,
		AliveWorkers:  m.aliveCountLocked(),
		TargetWorkers: m.cfg.TargetWorkers,
	}
	if m.job != nil {
		resp.Phase = m.job.Phase
		resp.Shutdown = m.job.Phase == common.PhaseDone || m.job.Phase == common.PhaseFailed
	}
	var clusterWords int64
	var clusterRate float64
	for _, ww := range m.workers {
		if ww.Alive {
			clusterWords += ww.WordsEmitted
			clusterRate += ww.Rate
		}
	}
	resp.ClusterWords = clusterWords
	resp.ClusterRate = clusterRate
	return resp
}

// recomputeShuffleProgressLocked sums fetch progress from live heartbeats
// and completed reduce tasks.
func (m *Master) recomputeShuffleProgressLocked() {
	done := 0
	total := 0
	var bytes int64
	for _, rt := range m.reduceTasks {
		total += len(rt.Sources)
		if rt.State == common.TaskCompleted {
			done += len(rt.Sources)
			bytes += rt.BytesRead
		}
	}
	for _, w := range m.workers {
		for _, a := range w.Active {
			if a.Type == common.TaskReduce {
				done += a.FetchDone
				bytes += a.FetchBytes
			}
		}
	}
	if done > total && total > 0 {
		done = total
	}
	m.fetchDone, m.fetchTotal, m.fetchBytes = done, total, bytes
}

// maybeAdvanceShuffleLocked flips shuffle -> reduce once all intermediate
// files have been pulled by the reduce workers.
func (m *Master) maybeAdvanceShuffleLocked() {
	if m.job == nil || m.job.Phase != common.PhaseShuffle {
		return
	}
	if m.fetchTotal == 0 || m.fetchDone >= m.fetchTotal {
		m.job.Phase = common.PhaseReduce
		if m.job.ReduceStart.IsZero() {
			m.job.ReduceStart = time.Now()
		}
		m.log.Push("SHUFFLE COMPLETE — all %d partition files pulled (%s moved)",
			m.fetchTotal, common.HumanBytes(m.fetchBytes))
	}
}

// NextTask hands the next pending task to a polling worker (pull model).
func (m *Master) NextTask(workerID string) common.TaskAssignment {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.workers[workerID]; !ok {
		return common.TaskAssignment{Type: common.TaskNone}
	}
	if m.job == nil {
		return common.TaskAssignment{Type: common.TaskNone}
	}
	switch m.job.Phase {
	case common.PhaseDone, common.PhaseFailed:
		return common.TaskAssignment{Type: common.TaskShutdown}
	case common.PhaseMap:
		for _, t := range m.mapTasks {
			if t.State == common.TaskIdle {
				return m.assignMapLocked(t, workerID)
			}
		}
		return common.TaskAssignment{Type: common.TaskNone}
	case common.PhaseShuffle, common.PhaseReduce:
		for _, t := range m.reduceTasks {
			if t.State == common.TaskIdle {
				return m.assignReduceLocked(t, workerID)
			}
		}
		return common.TaskAssignment{Type: common.TaskNone}
	}
	return common.TaskAssignment{Type: common.TaskNone}
}

func (m *Master) assignMapLocked(t *MapTask, workerID string) common.TaskAssignment {
	t.State = common.TaskInProgress
	t.AssignedTo = workerID
	t.Attempts++
	t.StartedAt = time.Now()
	return common.TaskAssignment{
		Type:          common.TaskMap,
		TaskID:        t.ID,
		InputURL:      "/api/input/" + t.ID,
		InputBytes:    t.Chunk.Length,
		SourceFile:    t.Chunk.Name,
		NumPartitions: m.job.R,
	}
}

func (m *Master) assignReduceLocked(t *ReduceTask, workerID string) common.TaskAssignment {
	t.State = common.TaskInProgress
	t.AssignedTo = workerID
	t.Attempts++
	t.StartedAt = time.Now()
	return common.TaskAssignment{
		Type:      common.TaskReduce,
		TaskID:    t.ID,
		Partition: t.Partition,
		Sources:   t.Sources,
	}
}

// TaskDone processes a task completion or failure report. It returns the ID
// of a worker that should be probed (data plane health check) because a
// reduce task could not fetch intermediate files from it.
func (m *Master) TaskDone(req common.TaskDoneRequest) (common.TaskDoneResponse, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	probeWorker := ""
	w := m.workers[req.WorkerID]
	if w == nil {
		return common.TaskDoneResponse{OK: false, Message: "unknown worker"}, ""
	}
	switch req.Type {
	case common.TaskMap:
		t := m.mapTaskByID(req.TaskID)
		if t == nil {
			return common.TaskDoneResponse{OK: false, Message: "no such map task"}, ""
		}
		if t.State == common.TaskCompleted {
			return common.TaskDoneResponse{OK: true, Message: "duplicate report ignored"}, ""
		}
		if t.State == common.TaskInProgress && t.AssignedTo != req.WorkerID {
			return common.TaskDoneResponse{OK: true, Message: "stale report ignored"}, ""
		}
		if !req.Success {
			m.failMapTaskLocked(t, req.WorkerID, req.Error)
			return common.TaskDoneResponse{OK: true}, ""
		}
		t.State = common.TaskCompleted
		t.Holder = req.WorkerID
		t.CompletedAt = time.Now()
		t.Words = req.Stats.Words
		t.UniqueWords = req.Stats.UniqueWords
		t.BytesRead = req.Stats.BytesRead
		t.PartitionSizes = req.PartitionSizes
		var mid int64
		for _, s := range req.PartitionSizes {
			mid += s
		}
		t.IntermediateBytes = mid
		if m.job != nil {
			m.job.intermediateBytes += mid
		}
		w.MapDone++
		w.TasksDone = w.MapDone + w.ReduceDone
		w.WordsCounted += req.Stats.Words
		dur := req.Stats.DurationMS
		m.log.Push("map %-6s done by %-18s %6.1fs · %s words · %s unique · %s intermediate",
			t.ID, w.ID, float64(dur)/1000, common.Humanize(t.Words), common.Humanize(t.UniqueWords), common.HumanBytes(mid))
		m.maybeAllMapsDoneLocked()
	case common.TaskReduce:
		t := m.reduceTaskByID(req.TaskID)
		if t == nil {
			return common.TaskDoneResponse{OK: false, Message: "no such reduce task"}, ""
		}
		if t.State == common.TaskCompleted {
			return common.TaskDoneResponse{OK: true, Message: "duplicate report ignored"}, ""
		}
		if t.State == common.TaskInProgress && t.AssignedTo != req.WorkerID {
			return common.TaskDoneResponse{OK: true, Message: "stale report ignored"}, ""
		}
		if !req.Success {
			m.failReduceTaskLocked(t, req.WorkerID, req.Error)
			if req.FailedSource != "" {
				if src := m.workers[req.FailedSource]; src != nil && src.Alive {
					probeWorker = req.FailedSource
				}
			}
			return common.TaskDoneResponse{OK: true}, probeWorker
		}
		t.State = common.TaskCompleted
		t.Holder = req.WorkerID
		t.CompletedAt = time.Now()
		t.ResultURL = req.ResultURL
		t.Words = req.Stats.Words
		t.UniqueWords = req.Stats.UniqueWords
		t.BytesRead = req.Stats.BytesRead
		w.ReduceDone++
		w.TasksDone = w.MapDone + w.ReduceDone
		w.WordsMerged += req.Stats.Words
		dur := req.Stats.DurationMS
		m.log.Push("reduce %-6s done by %-18s %6.1fs · %s unique words · %s input",
			t.ID, w.ID, float64(dur)/1000, common.Humanize(t.UniqueWords), common.HumanBytes(t.BytesRead))
		m.recomputeShuffleProgressLocked()
		m.maybeAdvanceShuffleLocked()
		m.maybeAllReduceDoneLocked()
	}
	return common.TaskDoneResponse{OK: true}, ""
}

func (m *Master) maybeAllMapsDoneLocked() {
	if m.job == nil || m.job.Phase != common.PhaseMap {
		return
	}
	for _, t := range m.mapTasks {
		if t.State != common.TaskCompleted {
			return
		}
	}
	m.prepareShuffleLocked()
}

// prepareShuffleLocked builds the source lists (which worker holds which
// intermediate file) and enters the shuffle phase.
func (m *Master) prepareShuffleLocked() {
	for _, rt := range m.reduceTasks {
		if rt.State == common.TaskCompleted {
			continue // its result is already computed
		}
		rt.Sources = nil
		for _, mt := range m.mapTasks {
			holder := m.workers[mt.Holder]
			if mt.State == common.TaskCompleted && (holder == nil || !holder.Alive) {
				// defensive: holder vanished between completion and now.
				// Reuse the standard failure path so intermediate-byte
				// accounting and rollback behaviour stay consistent.
				m.failMapTaskLocked(mt, mt.Holder, "files unavailable (holder gone)")
				m.job.Phase = common.PhaseMap
				return
			}
			if mt.State != common.TaskCompleted {
				m.log.Push("inconsistent map state during shuffle — staying in MAP")
				m.job.Phase = common.PhaseMap
				return
			}
			var bytes int64
			if mt.PartitionSizes != nil && rt.Partition < len(mt.PartitionSizes) {
				bytes = mt.PartitionSizes[rt.Partition]
			}
			rt.Sources = append(rt.Sources, common.FileSource{
				TaskID:   mt.ID,
				WorkerID: mt.Holder,
				URL:      fmt.Sprintf("http://%s/files/map/%s/%d", holder.FileAddr, mt.ID, rt.Partition),
				Bytes:    bytes,
			})
		}
	}
	m.job.Phase = common.PhaseShuffle
	m.job.MapEndedAt = time.Now()
	m.job.ShuffleStart = time.Now()
	m.recomputeShuffleProgressLocked()
	m.log.Push("MAP PHASE COMPLETE in %s — %s words across %d chunks",
		termDur(m.job.MapEndedAt.Sub(m.job.StartedAt)), common.Humanize(m.totalMapWordsLocked()), len(m.mapTasks))
	m.log.Push("SHUFFLE begins: %d reduce tasks will pull %d partition files (%s) from map workers",
		len(m.reduceTasks), m.fetchTotal, common.HumanBytes(m.job.intermediateBytes))
	m.maybeAdvanceShuffleLocked()
}

func (m *Master) totalMapWordsLocked() int64 {
	var n int64
	for _, t := range m.mapTasks {
		n += t.Words
	}
	return n
}

func (m *Master) maybeAllReduceDoneLocked() {
	if m.job == nil {
		return
	}
	if m.job.Phase != common.PhaseShuffle && m.job.Phase != common.PhaseReduce {
		return
	}
	for _, t := range m.reduceTasks {
		if t.State != common.TaskCompleted {
			return
		}
	}
	m.job.Phase = common.PhaseMerge
	m.job.ReduceEndedAt = time.Now()
	if m.job.ReduceStart.IsZero() {
		m.job.ReduceStart = m.job.ShuffleStart
	}
	m.log.Push("REDUCE PHASE COMPLETE in %s — merging %d partition results on the master",
		termDur(m.job.ReduceEndedAt.Sub(m.job.ReduceStart)), len(m.reduceTasks))
	go m.mergeAndFinalize()
}

// failMapTaskLocked requeues a failed (or orphaned) map task.
func (m *Master) failMapTaskLocked(t *MapTask, workerID, errMsg string) {
	t.Failures++
	wasCompleted := t.State == common.TaskCompleted
	t.State = common.TaskIdle
	t.AssignedTo = ""
	if wasCompleted {
		t.Holder = ""
		t.PartitionSizes = nil
		if m.job != nil {
			m.job.intermediateBytes -= t.IntermediateBytes
			t.IntermediateBytes = 0
		}
	}
	if errMsg == "" {
		errMsg = "worker lost"
	}
	m.log.Push("map %-6s FAILED on %s (%s) — attempt %d", t.ID, workerID, errMsg, t.Failures)
	if t.Failures >= m.cfg.MaxFailures {
		m.markDoneLocked(common.PhaseFailed, fmt.Sprintf("map task %s failed %d times", t.ID, t.Failures))
		return
	}
	// A completed map task whose files are lost invalidates pending reduce
	// work: roll the job back to the map phase so the chunk is re-processed.
	if wasCompleted && m.job != nil &&
		(m.job.Phase == common.PhaseShuffle || m.job.Phase == common.PhaseReduce) {
		for _, rt := range m.reduceTasks {
			if rt.State == common.TaskIdle {
				rt.Sources = nil
			}
		}
		m.job.Phase = common.PhaseMap
		// reset phase clocks so post-rollback timings reflect the new pass
		m.job.ShuffleStart = time.Time{}
		m.job.ReduceStart = time.Time{}
		m.log.Push("intermediate files lost — rolling back to MAP phase to re-process %s", t.ID)
	}
}

// failReduceTaskLocked requeues a failed (or orphaned) reduce task.
func (m *Master) failReduceTaskLocked(t *ReduceTask, workerID, errMsg string) {
	t.Failures++
	if errMsg == "" {
		errMsg = "worker lost"
	}
	m.log.Push("reduce %-6s FAILED on %s (%s) — attempt %d", t.ID, workerID, errMsg, t.Failures)
	if t.Failures >= m.cfg.MaxFailures {
		m.markDoneLocked(common.PhaseFailed, fmt.Sprintf("reduce task %s failed %d times", t.ID, t.Failures))
		return
	}
	if t.State == common.TaskCompleted {
		// result file lost before the master consumed it
		t.Holder = ""
		t.ResultURL = ""
	}
	t.State = common.TaskIdle
	t.AssignedTo = ""
}

// failWorkerLocked declares a worker dead and re-queues its tasks.
// reason explains why (heartbeat timeout, data-plane probe failure…).
func (m *Master) failWorkerLocked(id, reason string) {
	w := m.workers[id]
	if w == nil || !w.Alive {
		return
	}
	w.Alive = false
	m.log.Push("worker %s UNREACHABLE (%s) — re-queuing its tasks", id, reason)
	for _, t := range m.mapTasks {
		if t.AssignedTo == id && t.State == common.TaskInProgress {
			m.failMapTaskLocked(t, id, "")
		} else if t.Holder == id && t.State == common.TaskCompleted && m.job != nil &&
			m.job.Phase != common.PhaseMerge && m.job.Phase != common.PhaseDone {
			m.failMapTaskLocked(t, id, "")
		}
	}
	for _, t := range m.reduceTasks {
		if t.AssignedTo == id && t.State == common.TaskInProgress {
			m.failReduceTaskLocked(t, id, "")
		} else if t.Holder == id && t.State == common.TaskCompleted && !t.Consumed && m.job != nil &&
			m.job.Phase != common.PhaseMerge && m.job.Phase != common.PhaseDone {
			m.failReduceTaskLocked(t, id, "")
		}
	}
}

// MonitorWorkers periodically detects heartbeat timeouts.
func (m *Master) MonitorWorkers(stop <-chan struct{}) {
	// Sample fast enough for small timeouts (tests), but no busier than 1/s.
	tick := time.Second
	if m.cfg.WorkerTimeout < 4*time.Second {
		tick = m.cfg.WorkerTimeout / 4
	}
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		m.mu.Lock()
		// Once the job is DONE the cluster is winding down: workers
		// exit on their own as they learn the result, so a heartbeat
		// timeout after that is expected shutdown, not a failure.
		// Failing them would only spam "UNREACHABLE" lines into the
		// log during the master's exit grace period.
		if m.job != nil && m.job.Phase == common.PhaseDone {
			m.mu.Unlock()
			return
		}
		now := time.Now()
		for _, w := range m.workers {
			if w.Alive && now.Sub(w.LastSeen) > m.cfg.WorkerTimeout {
				m.failWorkerLocked(w.ID, "heartbeat timeout")
			}
		}
		m.mu.Unlock()
	}
}

func termDur(d time.Duration) string { return d.Truncate(time.Millisecond).String() }
