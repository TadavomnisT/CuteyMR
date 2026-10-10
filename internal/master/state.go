// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Package master implements the CuteyMR master: task scheduling,
// the HTTP control plane, the final merge and a terminal dashboard.
package master

import (
	"net/http"
	"sync"
	"time"

	"cuteymr/internal/common"
)

// Config holds master startup options.
type Config struct {
	Port          int
	TargetWorkers int
	InputPaths    []string
	OutputDir     string
	ChunkSize     int64         // bytes; 0 -> default 512KiB
	NumPartitions int           // R; 0 -> auto (2*workers, min 4)
	MaxFailures   int           // per task; 0 -> common.MaxTaskFailures
	WorkerTimeout time.Duration // heartbeat timeout; 0 -> common.DefaultWorkerTimeout
	ExitAfterDone time.Duration
	Headless      bool
}

// Chunk is one input slice handed to a map task.
type Chunk struct {
	File   string // absolute-ish path
	Name   string // base name for display
	Offset int64
	Length int64
}

// MapTask is one chunk of input to be mapped.
type MapTask struct {
	ID          string
	Chunk       Chunk
	State       common.TaskState
	AssignedTo  string
	Holder      string // worker that completed it and serves its files
	Attempts    int    // times assigned
	Failures    int    // times failed / re-queued
	StartedAt   time.Time
	CompletedAt time.Time

	Words             int64
	UniqueWords       int64
	BytesRead         int64
	IntermediateBytes int64
	PartitionSizes    []int64
}

// ReduceTask aggregates partition r of the intermediate data.
type ReduceTask struct {
	ID          string
	Partition   int
	State       common.TaskState
	AssignedTo  string
	Holder      string // worker that completed it and serves the result
	Attempts    int
	Failures    int
	StartedAt   time.Time
	CompletedAt time.Time

	Sources     []common.FileSource
	ResultURL   string
	Consumed    bool // master has downloaded the result
	UniqueWords int64
	Words       int64
	BytesRead   int64 // bytes fetched during the shuffle
}

// WorkerInfo is the master's record of one registered worker.
type WorkerInfo struct {
	ID           string
	Hostname     string
	Host         string
	FilePort     int
	FileAddr     string // host:port of the worker file server
	Cores        int
	MemoryMB     int64
	RegisteredAt time.Time
	LastSeen     time.Time
	Alive        bool

	TasksDone    int
	MapDone      int
	ReduceDone   int
	WordsEmitted int64 // reported by heartbeats (live view)
	WordsCounted int64 // aggregated from task reports (accurate)
	WordsMerged  int64
	BytesRead    int64
	BytesWritten int64
	Active       map[string]common.ActiveTaskInfo

	Rate       float64 // words/s, EWMA from heartbeats
	prevWords  int64
	prevSample time.Time
	History    []float64
}

// JobInfo describes the single wordcount job the cluster is running.
type JobInfo struct {
	Name       string
	InputFiles []string
	TotalBytes int64
	M, R       int

	Phase         common.Phase
	StartedAt     time.Time
	MapEndedAt    time.Time
	ShuffleStart  time.Time
	ReduceStart   time.Time
	ReduceEndedAt time.Time
	MergeEndedAt  time.Time

	TotalWords  int64
	UniqueWords int64
	OutputDir   string
	FailReason  string

	intermediateBytes int64
}

// Master holds all cluster state behind one mutex.
type Master struct {
	cfg Config

	mu      sync.Mutex
	workers map[string]*WorkerInfo
	order   []string

	job         *JobInfo
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask

	log         *common.EventLog
	doneCh      chan struct{}
	doneOnce    sync.Once
	srv         *http.Server
	stopMonitor chan struct{}

	startedAt time.Time

	frame int64 // dashboard animation frame

	// probeFails counts consecutive failed data-plane probe rounds per
	// worker (reset on any probe success or re-registration). See
	// probeWorker: it takes a stale heartbeat OR 3 straight failed rounds
	// before a freshly-heartbeating worker is declared dead.
	probeFails map[string]int

	// aggregate shuffle progress (computed from heartbeats)
	fetchDone  int
	fetchTotal int
	fetchBytes int64

	topWords []topWord // for the results panel
}

type topWord struct {
	Word  string
	Count int64
}

// New creates a master.
func New(cfg Config) *Master {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 512 << 10
	}
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = common.MaxTaskFailures
	}
	if cfg.WorkerTimeout <= 0 {
		cfg.WorkerTimeout = common.DefaultWorkerTimeout
	}
	if cfg.TargetWorkers <= 0 {
		cfg.TargetWorkers = 1
	}
	m := &Master{
		cfg:        cfg,
		workers:    make(map[string]*WorkerInfo),
		probeFails: make(map[string]int),
		log:        common.NewEventLog(400),
		doneCh:     make(chan struct{}),
		startedAt:  time.Now(),
	}
	return m
}

// Log exposes the event log.
func (m *Master) Log() *common.EventLog { return m.log }

// Done is closed once the job reaches a terminal phase.
func (m *Master) Done() <-chan struct{} { return m.doneCh }

func (m *Master) closeDone() {
	m.doneOnce.Do(func() { close(m.doneCh) })
}

func (m *Master) aliveCountLocked() int {
	n := 0
	for _, w := range m.workers {
		if w.Alive {
			n++
		}
	}
	return n
}

func (m *Master) markDoneLocked(phase common.Phase, reason string) {
	if m.job == nil {
		return
	}
	m.job.Phase = phase
	if reason != "" {
		m.job.FailReason = reason
	}
	m.closeDone()
}
