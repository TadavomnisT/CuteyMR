// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package master

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cuteymr/internal/common"
)

// Start launches the master HTTP control plane and the worker monitor.
func (m *Master) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", m.handleRoot)
	mux.HandleFunc("/api/register", m.handleRegister)
	mux.HandleFunc("/api/heartbeat", m.handleHeartbeat)
	mux.HandleFunc("/api/task/next", m.handleNextTask)
	mux.HandleFunc("/api/task/done", m.handleTaskDone)
	mux.HandleFunc("/api/input/", m.handleInput)
	mux.HandleFunc("/api/status", m.handleStatus)

	addr := fmt.Sprintf(":%d", m.cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	m.stopMonitor = make(chan struct{})
	go m.MonitorWorkers(m.stopMonitor)
	m.log.Push("master listening on %s — waiting for %d worker(s) to connect", addr, m.cfg.TargetWorkers)

	m.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = m.srv.Serve(ln) }()
	return nil
}

// Stop shuts the control plane down.
func (m *Master) Stop() {
	if m.stopMonitor != nil {
		close(m.stopMonitor)
	}
	if m.srv != nil {
		_ = m.srv.Close()
	}
}

func (m *Master) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	m.mu.Lock()
	phase := common.PhaseWaiting
	target := m.cfg.TargetWorkers
	alive := m.aliveCountLocked()
	var line2 string
	if m.job != nil {
		phase = m.job.Phase
		line2 = fmt.Sprintf("job: %s · map %d/%d · reduce %d/%d\n",
			m.job.Name, m.mapDoneLocked(), m.job.M, m.reduceDoneLocked(), m.job.R)
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "CuteyMR master on :%d\nphase: %s · workers: %d/%d\n%sGET /api/status for the full JSON snapshot\n",
		m.cfg.Port, phase, alive, target, line2)
}

func (m *Master) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		common.WriteJSON(w, http.StatusMethodNotAllowed, common.RegisterResponse{Message: "POST required"})
		return
	}
	var req common.RegisterRequest
	if err := common.ReadJSON(r, &req); err != nil {
		common.WriteJSON(w, http.StatusBadRequest, common.RegisterResponse{Message: err.Error()})
		return
	}
	if req.WorkerID == "" || req.FilePort <= 0 {
		common.WriteJSON(w, http.StatusBadRequest, common.RegisterResponse{Message: "worker_id and file_port required"})
		return
	}
	common.WriteJSON(w, http.StatusOK, m.Register(req))
}

func (m *Master) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		common.WriteJSON(w, http.StatusMethodNotAllowed, common.HeartbeatResponse{Message: "POST required"})
		return
	}
	var req common.HeartbeatRequest
	if err := common.ReadJSON(r, &req); err != nil {
		common.WriteJSON(w, http.StatusBadRequest, common.HeartbeatResponse{Message: err.Error()})
		return
	}
	common.WriteJSON(w, http.StatusOK, m.Heartbeat(req))
}

func (m *Master) handleNextTask(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	assign := m.NextTask(workerID)
	common.WriteJSON(w, http.StatusOK, assign)
}

func (m *Master) handleTaskDone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		common.WriteJSON(w, http.StatusMethodNotAllowed, common.TaskDoneResponse{Message: "POST required"})
		return
	}
	var req common.TaskDoneRequest
	if err := common.ReadJSON(r, &req); err != nil {
		common.WriteJSON(w, http.StatusBadRequest, common.TaskDoneResponse{Message: err.Error()})
		return
	}
	resp, probeWorker := m.TaskDone(req)
	common.WriteJSON(w, http.StatusOK, resp)
	// A reduce task could not fetch intermediate data from a peer worker:
	// actively probe that worker's data plane instead of waiting for the
	// heartbeat timeout, and mark it dead (re-queuing its tasks) if needed.
	if probeWorker != "" {
		go m.probeWorker(probeWorker)
	}
}

// probeWorker checks whether a worker's file server still responds and
// declares it dead if not.
func (m *Master) probeWorker(id string) {
	m.mu.Lock()
	w := m.workers[id]
	if w == nil || !w.Alive {
		m.mu.Unlock()
		return
	}
	addr := w.FileAddr
	m.mu.Unlock()

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/")
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return // data plane healthy — the fetch failure was transient
	}
	m.mu.Lock()
	m.failWorkerLocked(id, "data-plane probe failed: "+err.Error())
	m.mu.Unlock()
}

func (m *Master) handleInput(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/input/")
	m.mu.Lock()
	var chunk Chunk
	found := false
	for _, t := range m.mapTasks {
		if t.ID == id {
			chunk = t.Chunk
			found = true
			break
		}
	}
	m.mu.Unlock()
	if !found {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(chunk.File)
	if err != nil {
		http.Error(w, "input file unavailable: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	buf := make([]byte, chunk.Length)
	if _, err := f.ReadAt(buf, chunk.Offset); err != nil {
		http.Error(w, "read failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	_, _ = w.Write(buf)
}

// statusJSON is the /api/status payload used by tests and tooling.
type statusJSON struct {
	Job struct {
		Name        string `json:"name"`
		Phase       string `json:"phase"`
		InputFiles  int    `json:"input_files"`
		InputBytes  int64  `json:"input_bytes"`
		M           int    `json:"map_tasks"`
		R           int    `json:"reduce_tasks"`
		MapDone     int    `json:"map_done"`
		ReduceDone  int    `json:"reduce_done"`
		ShuffleDone int    `json:"shuffle_files_done"`
		ShuffleTot  int    `json:"shuffle_files_total"`
		TotalWords  int64  `json:"total_words"`
		UniqueWords int64  `json:"unique_words"`
		ElapsedMS   int64  `json:"elapsed_ms"`
		OutputDir   string `json:"output_dir"`
		FailReason  string `json:"fail_reason,omitempty"`
	} `json:"job"`
	Workers []struct {
		ID         string  `json:"id"`
		Host       string  `json:"host"`
		FileAddr   string  `json:"file_addr"`
		Cores      int     `json:"cores"`
		MemoryMB   int64   `json:"memory_mb"`
		Alive      bool    `json:"alive"`
		TasksDone  int     `json:"tasks_done"`
		MapDone    int     `json:"map_done"`
		ReduceDone int     `json:"reduce_done"`
		Words      int64   `json:"words_emitted"`
		Rate       float64 `json:"rate_words_per_sec"`
	} `json:"workers"`
	MapTasks    []taskStatus `json:"map_tasks"`
	ReduceTasks []taskStatus `json:"reduce_tasks"`
	Log         []string     `json:"log"`
}

type taskStatus struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	AssignedTo string `json:"assigned_to,omitempty"`
	Holder     string `json:"holder,omitempty"`
	Attempts   int    `json:"attempts"`
	Failures   int    `json:"failures"`
	Words      int64  `json:"words"`
}

func (m *Master) handleStatus(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	out := statusJSON{Log: m.log.Last(30)}
	if m.job != nil {
		out.Job.Name = m.job.Name
		out.Job.Phase = string(m.job.Phase)
		out.Job.InputFiles = len(m.job.InputFiles)
		out.Job.InputBytes = m.job.TotalBytes
		out.Job.M = m.job.M
		out.Job.R = m.job.R
		out.Job.MapDone = m.mapDoneLocked()
		out.Job.ReduceDone = m.reduceDoneLocked()
		out.Job.ShuffleDone = m.fetchDone
		out.Job.ShuffleTot = m.fetchTotal
		out.Job.TotalWords = m.job.TotalWords
		out.Job.UniqueWords = m.job.UniqueWords
		out.Job.ElapsedMS = time.Since(m.job.StartedAt).Milliseconds()
		out.Job.OutputDir = m.job.OutputDir
		out.Job.FailReason = m.job.FailReason
	}
	for _, id := range m.order {
		w := m.workers[id]
		out.Workers = append(out.Workers, struct {
			ID         string  `json:"id"`
			Host       string  `json:"host"`
			FileAddr   string  `json:"file_addr"`
			Cores      int     `json:"cores"`
			MemoryMB   int64   `json:"memory_mb"`
			Alive      bool    `json:"alive"`
			TasksDone  int     `json:"tasks_done"`
			MapDone    int     `json:"map_done"`
			ReduceDone int     `json:"reduce_done"`
			Words      int64   `json:"words_emitted"`
			Rate       float64 `json:"rate_words_per_sec"`
		}{
			ID: w.ID, Host: w.Host, FileAddr: w.FileAddr, Cores: w.Cores,
			MemoryMB: w.MemoryMB, Alive: w.Alive, TasksDone: w.TasksDone,
			MapDone: w.MapDone, ReduceDone: w.ReduceDone, Words: w.WordsCounted, Rate: w.Rate,
		})
	}
	for _, t := range m.mapTasks {
		out.MapTasks = append(out.MapTasks, taskStatus{
			ID: t.ID, State: string(t.State), AssignedTo: t.AssignedTo, Holder: t.Holder,
			Attempts: t.Attempts, Failures: t.Failures, Words: t.Words,
		})
	}
	for _, t := range m.reduceTasks {
		out.ReduceTasks = append(out.ReduceTasks, taskStatus{
			ID: t.ID, State: string(t.State), AssignedTo: t.AssignedTo, Holder: t.Holder,
			Attempts: t.Attempts, Failures: t.Failures, Words: t.Words,
		})
	}
	m.mu.Unlock()
	common.WriteJSON(w, http.StatusOK, out)
}

func (m *Master) mapDoneLocked() int {
	n := 0
	for _, t := range m.mapTasks {
		if t.State == common.TaskCompleted {
			n++
		}
	}
	return n
}

func (m *Master) reduceDoneLocked() int {
	n := 0
	for _, t := range m.reduceTasks {
		if t.State == common.TaskCompleted {
			n++
		}
	}
	return n
}
