// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Package worker implements a CuteyMR worker: it registers with the master,
// runs map/reduce tasks on one goroutine per CPU core, serves its
// intermediate files to peers over HTTP and renders a terminal dashboard.
package worker

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"cuteymr/internal/common"
)

// Config holds worker startup options.
type Config struct {
	MasterURL         string
	FilePort          int           // 0 = ephemeral
	Advertise         string        // "" = auto detect
	DataDir           string        // "" = temp dir
	HeartbeatInterval time.Duration // 0 = common.DefaultHeartbeatInterval
	Headless          bool
	SlowReduce        time.Duration // >0: fault-injection aid — sleep this long at the start of every reduce task
}

// SlotView is the live state of one task slot (rendered on the dashboard
// and shipped inside heartbeats).
type SlotView struct {
	Index      int
	TaskID     string
	Type       string
	Detail     string
	Progress   float64
	Words      int64
	Rate       float64
	FetchDone  int
	FetchTotal int
	FetchBytes int64
	Started    time.Time

	lastWords int64
	lastTime  time.Time
}

// workerStats is the mutable counter block of a worker. Access it only
// through bumpStats (mutex-guarded).
type workerStats struct {
	MapDone      int
	ReduceDone   int
	WordsEmitted int64
	WordsMerged  int64
	BytesRead    int64
	BytesWritten int64
	Errors       int
}

// Worker is one worker process.
type Worker struct {
	cfg Config
	ID  string

	mu      sync.Mutex
	slots   []*SlotView
	stats   workerStats
	cluster struct {
		Phase        common.Phase
		Alive        int
		Target       int
		ClusterWords int64
		ClusterRate  float64
		Updated      time.Time
	}

	log            *common.EventLog
	dataDir        string
	advertise      string
	fileAddr       string
	filePort       int
	httpSrv        *http.Server
	shutdownCh     chan struct{}
	shutdownOnce   sync.Once
	slotsWG        sync.WaitGroup
	startedAt      time.Time
	heartbeatFails int
	rateHistory    []float64 // combined slot rate history for the sparkline
}

var adjectives = []string{
	"swift", "bold", "calm", "brave", "cosmic", "dusty", "eager", "fancy",
	"gentle", "happy", "jolly", "keen", "lucky", "merry", "noble", "proud",
	"quiet", "rapid", "sunny", "tidy", "vivid", "warm", "zesty", "opal",
}

var animals = []string{
	"otter", "falcon", "panda", "tiger", "lemur", "cobra", "koala", "heron",
	"lynx", "marmot", "narwhal", "osprey", "quokka", "raven", "tapir",
	"urchin", "viper", "walrus", "ibex", "jackal", "kiwi", "zebu",
}

func genWorkerID() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("w-%s-%s-%04x", adjectives[r.Intn(len(adjectives))], animals[r.Intn(len(animals))], r.Intn(0x10000))
}

func defaultAdvertise(masterURL string) string {
	u, err := url.Parse(masterURL)
	if err == nil {
		h := strings.ToLower(u.Hostname())
		if h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "" {
			return "127.0.0.1"
		}
	}
	hn, err := os.Hostname()
	if err != nil || hn == "" {
		return "127.0.0.1"
	}
	return hn
}

// New creates a worker (call Start afterwards).
func New(cfg Config) (*Worker, error) {
	if cfg.MasterURL == "" {
		cfg.MasterURL = "http://127.0.0.1:8080"
	}
	cfg.MasterURL = strings.TrimRight(cfg.MasterURL, "/")
	if !strings.HasPrefix(cfg.MasterURL, "http://") && !strings.HasPrefix(cfg.MasterURL, "https://") {
		cfg.MasterURL = "http://" + cfg.MasterURL
	}
	advertise := cfg.Advertise
	if advertise == "" {
		advertise = defaultAdvertise(cfg.MasterURL)
	}
	id := genWorkerID()
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "cuteymr", fmt.Sprintf("%s-%d", id, os.Getpid()))
	}
	// start from a clean slate
	_ = os.RemoveAll(dataDir)
	if err := os.MkdirAll(filepath.Join(dataDir, "tasks"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "results"), 0o755); err != nil {
		return nil, err
	}
	cores := runtime.NumCPU()
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = common.DefaultHeartbeatInterval
	}
	w := &Worker{
		cfg:        cfg,
		ID:         id,
		log:        common.NewEventLog(300),
		dataDir:    dataDir,
		advertise:  advertise,
		shutdownCh: make(chan struct{}),
		startedAt:  time.Now(),
	}
	for i := 0; i < cores; i++ {
		w.slots = append(w.slots, &SlotView{Index: i})
	}
	hostname, _ := os.Hostname()
	w.log.Push("worker %s starting on %s — %d cores, %s RAM, data in %s",
		id, hostname, cores, common.HumanBytes(common.TotalMemoryMB()<<20), dataDir)
	return w, nil
}

// ShutdownCh is closed when the worker should exit (job finished / master lost).
func (w *Worker) ShutdownCh() <-chan struct{} { return w.shutdownCh }

// Log exposes the event log.
func (w *Worker) Log() *common.EventLog { return w.log }

// ID returns the worker id.
func (w *Worker) WorkerID() string { return w.ID }

// FileAddr returns host:port of this worker's file server.
func (w *Worker) FileAddr() string { return w.fileAddr }

func (w *Worker) triggerShutdown() {
	w.shutdownOnce.Do(func() {
		close(w.shutdownCh)
		w.log.Push("shutting down — goodbye! (^_^)/")
	})
}

// StartFileServer brings up the peer-to-peer data plane.
func (w *Worker) StartFileServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/files/map/", w.handleMapFile)
	mux.HandleFunc("/files/reduce/", w.handleReduceFile)
	mux.HandleFunc("/", w.handleRoot)
	addr := fmt.Sprintf(":%d", w.cfg.FilePort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	w.filePort = ln.Addr().(*net.TCPAddr).Port
	w.fileAddr = fmt.Sprintf("%s:%d", w.advertise, w.filePort)
	w.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = w.httpSrv.Serve(ln) }()
	w.log.Push("file server listening on %s (serving intermediate data to peers)", w.fileAddr)
	return nil
}

// Register introduces the worker to the master, retrying until accepted.
func (w *Worker) Register(ctx context.Context) error {
	hostname, _ := os.Hostname()
	req := common.RegisterRequest{
		WorkerID: w.ID,
		Host:     w.advertise,
		FilePort: w.filePort,
		Cores:    len(w.slots),
		MemoryMB: common.TotalMemoryMB(),
		Hostname: hostname,
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var resp common.RegisterResponse
		err := common.PostJSONCtx(ctx, w.cfg.MasterURL+"/api/register", req, &resp)
		if err == nil && resp.Accepted {
			w.log.Push("registered with master %s (cluster %d/%d, R=%d)",
				w.cfg.MasterURL, resp.AliveWorkers, resp.TargetWorkers, resp.NumPartitions)
			return nil
		}
		if err == nil && !resp.Accepted {
			return fmt.Errorf("master rejected registration: %s", resp.Message)
		}
		if attempt == 0 {
			w.log.Push("master not reachable yet (%v) — retrying until it comes up…", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Run starts heartbeat, slot and dashboard loops and blocks until the worker
// is asked to stop (signal, master shutdown, or master loss).
func (w *Worker) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	var dashWG sync.WaitGroup
	go w.HeartbeatLoop(ctx)
	dashWG.Add(1)
	go w.RunDashboard(ctx, &dashWG)
	for i := range w.slots {
		w.slotsWG.Add(1)
		go func(idx int) {
			defer w.slotsWG.Done()
			w.SlotLoop(ctx, idx)
		}(i)
	}

	select {
	case <-sigCh:
		w.log.Push("interrupt received — finishing current tasks")
	case <-w.shutdownCh:
	}

	cancel()
	done := make(chan struct{})
	go func() {
		w.slotsWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
	}
	// give the dashboard a moment to render the final state, then stop it
	time.Sleep(300 * time.Millisecond)
	cancel()
	dashWG.Wait()
}

// HeartbeatLoop periodically reports liveness and live stats to the master.
// If the master stays unreachable for ~15 heartbeat intervals (never less
// than 10s — PostJSON retries can stall a tick for much longer than the
// interval itself), the worker gives up and exits.
func (w *Worker) HeartbeatLoop(ctx context.Context) {
	interval := w.cfg.HeartbeatInterval
	giveUpAfter := 15 * interval
	if giveUpAfter < 10*time.Second {
		giveUpAfter = 10 * time.Second
	}
	lastOK := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		req := w.buildHeartbeat()
		var resp common.HeartbeatResponse
		err := common.PostJSONCtx(ctx, w.cfg.MasterURL+"/api/heartbeat", req, &resp)
		if err == nil {
			lastOK = time.Now()
		}
		w.mu.Lock()
		if err != nil {
			w.heartbeatFails++
			giveUp := time.Since(lastOK) > giveUpAfter
			w.mu.Unlock()
			if giveUp {
				w.log.Push("master unreachable for %s — exiting", time.Since(lastOK).Truncate(time.Second))
				w.triggerShutdown()
				return
			}
			continue
		}
		w.heartbeatFails = 0
		w.cluster.Phase = resp.Phase
		w.cluster.Alive = resp.AliveWorkers
		w.cluster.Target = resp.TargetWorkers
		w.cluster.ClusterWords = resp.ClusterWords
		w.cluster.ClusterRate = resp.ClusterRate
		w.cluster.Updated = time.Now()
		shutdown := resp.Shutdown
		unknown := !resp.OK
		w.mu.Unlock()
		if unknown {
			w.log.Push("master lost our registration — re-registering")
			if err := w.Register(ctx); err != nil {
				return
			}
			continue
		}
		if shutdown {
			w.triggerShutdown()
			return
		}
	}
}

func (w *Worker) buildHeartbeat() common.HeartbeatRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	req := common.HeartbeatRequest{
		WorkerID:        w.ID,
		TasksCompleted:  w.stats.MapDone + w.stats.ReduceDone,
		TasksMapDone:    w.stats.MapDone,
		TasksReduceDone: w.stats.ReduceDone,
		WordsEmitted:    w.stats.WordsEmitted,
		WordsMerged:     w.stats.WordsMerged,
		BytesRead:       w.stats.BytesRead,
		BytesWritten:    w.stats.BytesWritten,
	}
	for _, s := range w.slots {
		if s.TaskID != "" {
			req.Active = append(req.Active, common.ActiveTaskInfo{
				TaskID:     s.TaskID,
				Type:       s.Type,
				Progress:   s.Progress,
				Detail:     s.Detail,
				Words:      s.Words,
				FetchDone:  s.FetchDone,
				FetchTotal: s.FetchTotal,
				FetchBytes: s.FetchBytes,
			})
		}
	}
	return req
}

// SlotLoop is the pull loop of one CPU slot: ask for work, do it, repeat.
func (w *Worker) SlotLoop(ctx context.Context, idx int) {
	errLogged := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var a common.TaskAssignment
		err := common.GetJSONCtx(ctx, fmt.Sprintf("%s/api/task/next?worker=%s", w.cfg.MasterURL, url.QueryEscape(w.ID)), &a)
		if err != nil {
			if !errLogged {
				w.log.Push("slot %d: cannot reach master for tasks: %v", idx, err)
				errLogged = true
			}
			w.sleepCtx(ctx, 500*time.Millisecond)
			continue
		}
		errLogged = false
		switch a.Type {
		case common.TaskNone:
			// poll a bit faster while this worker has not done any work yet,
			// so late joiners pick up tasks quickly
			idle := 300 * time.Millisecond
			w.mu.Lock()
			if w.stats.MapDone+w.stats.ReduceDone == 0 {
				idle = 150 * time.Millisecond
			}
			w.mu.Unlock()
			w.sleepCtx(ctx, idle+time.Duration(rand.Intn(150))*time.Millisecond)
		case common.TaskShutdown:
			w.log.Push("slot %d: master reports job finished — nothing more to do", idx)
			w.triggerShutdown()
			return
		case common.TaskMap:
			w.executeMap(ctx, idx, a)
		case common.TaskReduce:
			w.executeReduce(ctx, idx, a)
		default:
			w.sleepCtx(ctx, time.Second)
		}
	}
}

func (w *Worker) sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// slot helpers ------------------------------------------------------------

func (w *Worker) setSlot(idx int, taskID, typ, detail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.slots[idx]
	s.TaskID = taskID
	s.Type = typ
	s.Detail = detail
	s.Progress = 0
	s.Words = 0
	s.Rate = 0
	s.FetchDone = 0
	s.FetchTotal = 0
	s.FetchBytes = 0
	s.Started = time.Now()
	s.lastWords = 0
	s.lastTime = time.Now()
}

func (w *Worker) updateSlotProgress(idx int, progress float64, words int64, detail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.slots[idx]
	s.Progress = progress
	s.Words = words
	s.Detail = detail
	now := time.Now()
	if dt := now.Sub(s.lastTime).Seconds(); dt >= 0.4 {
		inst := float64(words-s.lastWords) / dt
		if inst < 0 {
			inst = 0
		}
		s.Rate = 0.5*inst + 0.5*s.Rate
		s.lastWords = words
		s.lastTime = now
	}
}

func (w *Worker) updateSlotFetch(idx int, done, total int, bytes int64, detail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.slots[idx]
	s.FetchDone = done
	s.FetchTotal = total
	s.FetchBytes = bytes
	s.Detail = detail
	s.Progress = float64(done) / float64(max(total, 1)) * 0.8
}

func (w *Worker) clearSlot(idx int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.slots[idx]
	s.TaskID = ""
	s.Type = ""
	s.Detail = ""
	s.Progress = 0
	s.Words = 0
	s.Rate = 0
}

func (w *Worker) bumpStats(fn func(s *workerStats)) {
	w.mu.Lock()
	fn(&w.stats)
	w.mu.Unlock()
}

// Summary prints a final plain-text summary of this worker's work.
func (w *Worker) Summary() string {
	w.mu.Lock()
	mapDone, redDone := w.stats.MapDone, w.stats.ReduceDone
	words, merged := w.stats.WordsEmitted, w.stats.WordsMerged
	br, bw, errs := w.stats.BytesRead, w.stats.BytesWritten, w.stats.Errors
	w.mu.Unlock()
	return fmt.Sprintf("%s finished: %d tasks (%d map, %d reduce) · %s words counted · %s pairs merged · read %s · wrote %s · %d errors",
		w.ID, mapDone+redDone, mapDone, redDone, common.Humanize(words), common.Humanize(merged),
		common.HumanBytes(br), common.HumanBytes(bw), errs)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
