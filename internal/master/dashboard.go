// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package master

import (
	"fmt"
	"os"
	"sync"
	"time"

	"cuteymr/internal/common"
	"cuteymr/internal/termui"
)

// Palette — the standard 16 terminal colors only, so any terminal (even
// one without 256-color support) renders them from its own classic Linux
// palette. No blue/indigo tones.
var (
	colBorder  = termui.Style{Fg: 8} // bright black: the classic gray border
	colTitle   = termui.Style{Fg: 15, Bold: true}
	colDim     = termui.Style{Fg: 7} // white: readable secondary text
	colText    = termui.Style{Fg: 7}
	colGood    = termui.Style{Fg: 10} // bright green
	colMap     = termui.Style{Fg: 10}
	colReduce  = termui.Style{Fg: 13} // bright magenta
	colShuffle = termui.Style{Fg: 5}  // magenta
	colNet     = termui.Style{Fg: 14} // bright cyan
	colYellow  = termui.Style{Fg: 11}
	colHot     = termui.Style{Fg: 9} // bright red
	colOrange  = termui.Style{Fg: 3} // yellow: amber sparks
)

// RunDashboard renders the live terminal dashboard until stop is closed.
func (m *Master) RunDashboard(stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	plain := m.cfg.Headless
	if !plain {
		if _, _, err := termui.TerminalSize(); err != nil {
			plain = true
		}
	}
	if !plain {
		termui.EnterAltScreen()
		defer termui.ExitAltScreen()
	}
	shotPath := os.Getenv("MR_SCREENSHOT")
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	frame := 0
	lastPlain := time.Time{}
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		if plain {
			if time.Since(lastPlain) >= 2*time.Second {
				lastPlain = time.Now()
				fmt.Println(m.plainLine())
			}
			if m.isDone() {
				m.writeSummary()
			}
			if shotPath != "" {
				c := termui.NewCanvas(110, 44)
				m.mu.Lock()
				m.draw(c, frame)
				m.mu.Unlock()
				_ = os.WriteFile(shotPath, []byte(c.RenderPlain()), 0o644)
			}
			frame++
			continue
		}
		w, h := 110, 44
		if tw, th, err := termui.TerminalSize(); err == nil {
			w, h = tw, th
		}
		if w > 120 {
			w = 120
		}
		c := termui.NewCanvas(w, h)
		m.mu.Lock()
		m.draw(c, frame)
		m.mu.Unlock()
		fmt.Print(c.Render())
		if shotPath != "" {
			_ = os.WriteFile(shotPath, []byte(c.RenderPlain()), 0o644)
		}
		if m.isDone() {
			m.writeSummary()
		}
		frame++
	}
}

func (m *Master) isDone() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.job != nil && (m.job.Phase == common.PhaseDone || m.job.Phase == common.PhaseFailed)
}

func (m *Master) plainLine() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	alive := m.aliveCountLocked()
	if m.job == nil {
		return fmt.Sprintf("master: phase=waiting workers=%d/%d — auto-start when %d workers connect",
			alive, m.cfg.TargetWorkers, m.cfg.TargetWorkers)
	}
	return fmt.Sprintf("master: phase=%s map=%d/%d shuffle=%d/%d reduce=%d/%d workers=%d/%d rate=%.0fw/s",
		m.job.Phase, m.mapDoneLocked(), m.job.M, m.fetchDone, m.fetchTotal,
		m.reduceDoneLocked(), m.job.R, alive, m.cfg.TargetWorkers, m.clusterRateLocked())
}

func (m *Master) clusterRateLocked() float64 {
	var r float64
	for _, w := range m.workers {
		if w.Alive {
			r += w.Rate
		}
	}
	return r
}

// draw renders the whole dashboard onto the canvas (call with m.mu held).
func (m *Master) draw(c *termui.Canvas, frame int) {
	w, h := c.W, c.H
	if w < 60 || h < 20 {
		c.Text(0, 0, "CuteyMR master — terminal too small (need >=60x20)", colTitle)
		return
	}
	y := m.drawHeader(c, 0, w)
	y++
	y = m.drawProgress(c, y, w)
	y++

	avail := h - y - 1 // footer
	doneView := m.job != nil && m.job.Phase == common.PhaseDone
	topoH := 21
	compact := false
	if avail < topoH+8 || doneView {
		topoH = 14
		compact = true
	}
	if avail < topoH+6 {
		topoH = 8
	}
	m.drawTopology(c, 0, y, w, topoH, frame, compact)
	y += topoH + 1

	// workers table
	nRows := len(m.order)
	if nRows > 10 {
		nRows = 10
	}
	tableH := nRows + 4
	logH := avail - topoH - tableH - 2
	showTasks := !doneView && logH >= 10
	showResults := doneView && logH >= 6
	tasksH := 0
	if showTasks {
		tasksH = 6
		logH -= tasksH + 1
	}
	if showResults {
		tasksH = 8
		if logH-(tasksH+1) < 4 {
			tasksH = 7
		}
		logH -= tasksH + 1
	}
	if logH < 4 {
		if showTasks || showResults {
			showTasks, showResults = false, false
			logH += tasksH + 1
		}
	}
	if logH < 4 {
		logH = 4
		tableH = avail - topoH - logH - 2
		if tableH < 4 {
			tableH = 4
		}
	}
	if tableH >= 4 {
		m.drawWorkersTable(c, 0, y, w, tableH)
		y += tableH + 1
	}
	if showTasks {
		m.drawTasks(c, 0, y, w, tasksH, frame)
		y += tasksH + 1
	}
	if showResults {
		m.drawResults(c, 0, y, w, tasksH)
		y += tasksH + 1
	}
	m.drawLog(c, 0, y, w, h-1-y)
	m.drawFooter(c, h-1, w)
}

func (m *Master) drawHeader(c *termui.Canvas, y, w int) int {
	phase := common.PhaseWaiting
	var jobName string
	elapsed := time.Since(m.startedAt)
	if m.job != nil {
		phase = m.job.Phase
		jobName = m.job.Name
		elapsed = time.Since(m.job.StartedAt)
	}
	dot := colDim
	switch phase {
	case common.PhaseMap:
		dot = colMap
	case common.PhaseShuffle:
		dot = colShuffle
	case common.PhaseReduce:
		dot = colReduce
	case common.PhaseMerge:
		dot = colYellow
	case common.PhaseDone:
		dot = colGood
	case common.PhaseFailed:
		dot = colHot
	}
	c.Text(1, y, "●", dot)
	c.Text(3, y, "CUTEYMR MASTER", colTitle)
	if jobName != "" {
		c.Text(20, y, "· wordcount job", colDim)
	}
	alive := m.aliveCountLocked()
	right := fmt.Sprintf("%s · workers %d/%d · %s", phaseLabel(phase), alive, m.cfg.TargetWorkers, termui.FmtDur(elapsed))
	c.TextRight(w-len(right)-1, y, len(right), right, colDim)
	c.HLine(0, y+1, w, '─', colBorder)
	return y + 2
}

func phaseLabel(p common.Phase) string {
	switch p {
	case common.PhaseWaiting:
		return "WAITING"
	case common.PhaseMap:
		return "MAP"
	case common.PhaseShuffle:
		return "SHUFFLE"
	case common.PhaseReduce:
		return "REDUCE"
	case common.PhaseMerge:
		return "MERGE"
	case common.PhaseDone:
		return "DONE"
	case common.PhaseFailed:
		return "FAILED"
	}
	return string(p)
}

func phaseStyle(p common.Phase) termui.Style {
	switch p {
	case common.PhaseMap:
		return colMap
	case common.PhaseShuffle:
		return colShuffle
	case common.PhaseReduce:
		return colReduce
	case common.PhaseMerge:
		return colYellow
	case common.PhaseDone:
		return colGood
	case common.PhaseFailed:
		return colHot
	}
	return colDim
}

func (m *Master) drawProgress(c *termui.Canvas, y, w int) int {
	labelW := 9
	barW := w - labelW - 46
	if barW > 56 {
		barW = 56
	}
	if barW < 16 {
		barW = 16
	}
	if m.job == nil {
		alive := m.aliveCountLocked()
		spin := termui.Spinner(int(m.frame))
		c.Text(1, y, fmt.Sprintf("%c waiting for workers %d/%d — job auto-starts when all %d connect",
			spin, alive, m.cfg.TargetWorkers, m.cfg.TargetWorkers), colDim)
		return y + 1
	}
	j := m.job
	now := time.Now()

	mapDone := m.mapDoneLocked()
	mapPct := 0.0
	if j.M > 0 {
		mapPct = float64(mapDone) / float64(j.M)
	}
	mapElapsed := now.Sub(j.StartedAt)
	if !j.MapEndedAt.IsZero() {
		mapElapsed = j.MapEndedAt.Sub(j.StartedAt)
	}
	c.Text(1, y, "MAP     ", phaseStyle(common.PhaseMap))
	c.Bar(labelW, y, barW, mapPct, colMap, colBorder)
	c.Text(labelW+barW+2, y, fmt.Sprintf("%3.0f%%  %d/%d tasks  %s",
		mapPct*100, mapDone, j.M, termui.FmtDur(mapElapsed)), colText)

	shPct := 0.0
	if m.fetchTotal > 0 {
		shPct = float64(m.fetchDone) / float64(m.fetchTotal)
	}
	c.Text(1, y+1, "SHUFFLE ", phaseStyle(common.PhaseShuffle))
	c.Bar(labelW, y+1, barW, shPct, colNet, colBorder)
	c.Text(labelW+barW+2, y+1, fmt.Sprintf("%3.0f%%  %d/%d files  %s / %s",
		shPct*100, m.fetchDone, m.fetchTotal, common.HumanBytes(m.fetchBytes),
		common.HumanBytes(j.intermediateBytes)), colText)

	redDone := m.reduceDoneLocked()
	redPct := 0.0
	if j.R > 0 {
		redPct = float64(redDone) / float64(j.R)
	}
	redElapsed := time.Duration(0)
	if !j.ReduceStart.IsZero() {
		redElapsed = now.Sub(j.ReduceStart)
		if !j.ReduceEndedAt.IsZero() {
			redElapsed = j.ReduceEndedAt.Sub(j.ReduceStart)
		}
	}
	c.Text(1, y+2, "REDUCE  ", phaseStyle(common.PhaseReduce))
	c.Bar(labelW, y+2, barW, redPct, colReduce, colBorder)
	c.Text(labelW+barW+2, y+2, fmt.Sprintf("%3.0f%%  %d/%d tasks  %s",
		redPct*100, redDone, j.R, termui.FmtDur(redElapsed)), colText)

	var emitted int64
	rate := m.clusterRateLocked()
	for _, wr := range m.workers {
		if wr.Alive {
			emitted += wr.WordsCounted
		}
	}
	uptime := time.Since(m.startedAt)
	if j.Phase == common.PhaseDone {
		uptime = j.MergeEndedAt.Sub(m.startedAt)
		if d := j.MergeEndedAt.Sub(j.StartedAt); d > 0 {
			rate = float64(j.TotalWords) / d.Seconds()
		}
	}
	c.Text(1, y+3, fmt.Sprintf("rate %s w/s · counted %s words · input %s · intermediate %s · uptime %s",
		common.Humanize(int64(rate)), common.Humanize(emitted), common.HumanBytes(j.TotalBytes),
		common.HumanBytes(j.intermediateBytes), termui.FmtDur(uptime)), colDim)
	return y + 4
}

func packetGlyph(frame int) rune {
	glyphs := []rune{'·', '∙', 'o', 'O', 'o', '∙'}
	return glyphs[frame%len(glyphs)]
}

func faceFor(w *WorkerInfo, phase common.Phase) string {
	if !w.Alive {
		return "(x_x)"
	}
	if phase == common.PhaseDone {
		return "(^▽^)"
	}
	if len(w.Active) == 0 {
		if phase == common.PhaseWaiting {
			return "(-.-)z"
		}
		return "(o_o)"
	}
	hasMap, hasRed := false, false
	for _, a := range w.Active {
		if a.Type == common.TaskMap {
			hasMap = true
		}
		if a.Type == common.TaskReduce {
			hasRed = true
		}
	}
	switch {
	case hasMap && hasRed:
		return "(>o<)"
	case hasMap:
		return "(>_<)"
	case hasRed:
		return "(^_-)"
	default:
		return "(o_o)"
	}
}

func workerBorderStyle(w *WorkerInfo, phase common.Phase) termui.Style {
	if !w.Alive {
		return colHot
	}
	if phase == common.PhaseDone {
		return colGood
	}
	hasMap, hasRed := false, false
	for _, a := range w.Active {
		if a.Type == common.TaskMap {
			hasMap = true
		}
		if a.Type == common.TaskReduce {
			hasRed = true
		}
	}
	switch {
	case hasMap && hasRed:
		return colOrange
	case hasMap:
		return colMap
	case hasRed:
		return colReduce
	}
	return colBorder
}

func (m *Master) drawTopology(c *termui.Canvas, x, y, w, h int, frame int, compact bool) {
	alive := m.aliveCountLocked()
	title := fmt.Sprintf("CLUSTER TOPOLOGY — %d/%d online", alive, m.cfg.TargetWorkers)
	c.TitledBox(x, y, w, h, title, colBorder, colNet)
	iw := w - 2
	ix := x + 1
	iy := y + 1
	ih := h - 2
	if iw < 30 || ih < 5 {
		return
	}

	phase := common.PhaseWaiting
	if m.job != nil {
		phase = m.job.Phase
	}

	workers := make([]*WorkerInfo, 0, len(m.order))
	for _, id := range m.order {
		workers = append(workers, m.workers[id])
	}
	ghosts := 0
	if m.job == nil {
		if g := m.cfg.TargetWorkers - len(workers); g > 0 {
			ghosts = g
		}
	}
	n := len(workers) + ghosts

	if n == 0 {
		c.TextCenter(ix, iy+ih/2, iw, "waiting for the first worker to connect…", colDim)
		return
	}

	cardW := (iw - 4 - (n-1)*2) / n
	if cardW > 30 {
		cardW = 30
	}
	if cardW < 12 {
		m.drawRoster(c, ix, iy, iw, ih, workers, ghosts, phase, frame)
		return
	}

	// master box
	mw := 38
	if mw > iw-4 {
		mw = iw - 4
	}
	mh := 6
	if ih < 16 {
		mh = 4
	}
	mx := ix + (iw-mw)/2
	border := colBorder
	if phase == common.PhaseDone {
		border = colGood
	}
	c.Box(mx, iy, mw, mh, border)
	c.TextCenter(mx, iy+1, mw, "♛ CUTEYMR MASTER", colTitle)
	if mh == 6 {
		c.TextCenter(mx, iy+2, mw, fmt.Sprintf("listening on :%d", m.cfg.Port), colDim)
		ph := phaseLabel(phase)
		if m.job != nil {
			mapPct := 0.0
			if m.job.M > 0 {
				mapPct = float64(m.mapDoneLocked()) / float64(m.job.M)
			}
			ph = fmt.Sprintf("phase %s %.0f%%", ph, mapPct*100)
		}
		c.TextCenter(mx, iy+3, mw, ph, phaseStyle(phase))
		rate := m.clusterRateLocked()
		rateTxt := fmt.Sprintf("%s words/s", common.Humanize(int64(rate)))
		if phase == common.PhaseDone && m.job != nil {
			if d := m.job.MergeEndedAt.Sub(m.job.StartedAt); d > 0 {
				rate = float64(m.job.TotalWords) / d.Seconds()
				rateTxt = fmt.Sprintf("%s words/s avg", common.Humanize(int64(rate)))
			}
		}
		c.TextCenter(mx, iy+4, mw, rateTxt, colYellow)
	} else {
		ph := phaseLabel(phase)
		if m.job != nil {
			mapPct := 0.0
			if m.job.M > 0 {
				mapPct = float64(m.mapDoneLocked()) / float64(m.job.M)
			}
			ph = fmt.Sprintf("%s %.0f%%", ph, mapPct*100)
		}
		c.TextCenter(mx, iy+2, mw, ph, phaseStyle(phase))
	}

	busY := iy + mh + 2
	if busY >= y+h-1 {
		busY = y + h - 2
	}
	cx := mx + mw/2
	c.VLine(cx, iy+mh, busY-(iy+mh), '│', colNet)

	totalW := n*cardW + (n-1)*2
	startX := ix + (iw-totalW)/2
	if startX < ix {
		startX = ix
	}
	cardY := busY + 2
	cardH := y + h - 1 - cardY
	if cardH > 9 {
		cardH = 9
	}

	firstC := startX + cardW/2
	lastC := startX + (n-1)*(cardW+2) + cardW/2
	if lastC >= firstC {
		c.HLine(firstC, busY, lastC-firstC+1, '─', colNet)
		if phase == common.PhaseShuffle {
			pos := (frame * 5) % (lastC - firstC + 1)
			c.Set(firstC+pos, busY, 'o', colYellow)
		}
	}

	for i := 0; i < n; i++ {
		ccx := startX + i*(cardW+2) + cardW/2
		isGhost := i >= len(workers)
		glyph := rune('│')
		styl := colNet
		if !isGhost {
			wr := workers[i]
			if len(wr.Active) > 0 && wr.Alive && phase != common.PhaseDone {
				glyph = packetGlyph(frame + i)
				styl = colYellow
			}
		}
		if ccx != cx || busY > iy+mh {
			c.Set(ccx, busY+1, glyph, styl)
		}
		if isGhost {
			m.drawGhostCard(c, startX+i*(cardW+2), cardY, cardW, cardH, frame)
		} else {
			m.drawWorkerCard(c, startX+i*(cardW+2), cardY, cardW, cardH, workers[i], phase, frame)
		}
	}
}

func (m *Master) drawGhostCard(c *termui.Canvas, x, y, w, h int, frame int) {
	if h < 3 || w < 8 {
		return
	}
	c.Box(x, y, w, h, colBorder)
	c.TextCenter(x, y+h/2, w, " (? ?) ", colDim)
	if h >= 5 {
		c.TextCenter(x, y+h/2+1, w, "awaiting worker…", colDim)
	}
}

func (m *Master) drawWorkerCard(c *termui.Canvas, x, y, w, h int, wr *WorkerInfo, phase common.Phase, frame int) {
	if h < 3 || w < 10 {
		return
	}
	bs := workerBorderStyle(wr, phase)
	c.Box(x, y, w, h, bs)
	inner := w - 2
	face := faceFor(wr, phase)
	name := termui.Truncate(wr.ID, inner-8)
	c.Text(x+2, y+1, face, bs)
	c.Text(x+8, y+1, name, colText)

	status := "idle"
	if !wr.Alive {
		status = "OFFLINE"
	} else if phase == common.PhaseDone {
		status = "finished"
	} else if len(wr.Active) > 0 {
		mapN, redN := 0, 0
		for _, a := range wr.Active {
			if a.Type == common.TaskMap {
				mapN++
			} else if a.Type == common.TaskReduce {
				redN++
			}
		}
		switch {
		case mapN > 0 && redN > 0:
			status = fmt.Sprintf("map×%d red×%d", mapN, redN)
		case mapN > 0:
			status = fmt.Sprintf("mapping×%d", mapN)
		default:
			status = fmt.Sprintf("reducing×%d", redN)
		}
	} else if phase == common.PhaseWaiting {
		status = "ready"
	} else if phase == common.PhaseDone {
		status = "finished"
	}
	statusStyle := colDim
	if !wr.Alive {
		statusStyle = colHot
	} else if phase == common.PhaseDone {
		statusStyle = colGood
	} else if len(wr.Active) > 0 {
		statusStyle = colYellow
	}

	row := 2
	if h >= 5 {
		host := termui.Truncate(wr.FileAddr, inner)
		c.Text(x+2, y+row, host, colDim)
		row++
	}
	if h >= 6 {
		busy := len(wr.Active)
		barW := inner - 10
		if barW > 10 {
			barW = 10
		}
		c.Text(x+2, y+row, "cpu", colDim)
		c.Bar(x+6, y+row, barW, float64(busy)/float64(max(wr.Cores, 1)), colNet, colBorder)
		c.Text(x+6+barW+1, y+row, fmt.Sprintf("%d/%d", busy, wr.Cores), colDim)
		row++
	}
	if h >= 7 {
		c.Text(x+2, y+row, fmt.Sprintf("done %d · %s words", wr.TasksDone, common.Humanize(wr.WordsCounted)), colDim)
		row++
	}
	if h >= 8 {
		c.Spark(x+2, y+row, inner-4, wr.History, colOrange)
		row++
	}
	if row < h-1 {
		c.Text(x+2, y+row, termui.Truncate(status, inner), statusStyle)
	} else {
		// no space for a dedicated status row: overwrite name row tail
		c.Text(x+8+len(name)+1, y+1, termui.Truncate(" "+status, inner-8-len(name)-1), statusStyle)
	}
}

func (m *Master) drawRoster(c *termui.Canvas, x, y, w, h int, workers []*WorkerInfo, ghosts int, phase common.Phase, frame int) {
	row := 0
	for _, wr := range workers {
		if row >= h {
			return
		}
		face := faceFor(wr, phase)
		line := fmt.Sprintf("%s %-22s %-21s %2dc  %5s  done %3d  %s",
			face, termui.Truncate(wr.ID, 22), termui.Truncate(wr.FileAddr, 21), wr.Cores,
			common.HumanBytes(wr.MemoryMB<<20), wr.TasksDone,
			termui.Truncate(fmt.Sprintf("%s words", common.Humanize(wr.WordsEmitted)), 16))
		styl := colText
		if !wr.Alive {
			styl = colHot
		} else if len(wr.Active) > 0 {
			styl = colYellow
		}
		c.Text(x, y+row, termui.Truncate(line, w), styl)
		row++
	}
	for g := 0; g < ghosts; g++ {
		if row >= h {
			return
		}
		c.Text(x, y+row, "(? ?)  awaiting worker…", colDim)
		row++
	}
}

func (m *Master) drawWorkersTable(c *termui.Canvas, x, y, w, h int) {
	c.TitledBox(x, y, w, h, "WORKERS", colBorder, colTitle)
	iw := w - 2
	ix := x + 1
	iy := y + 1
	n := len(m.order)
	if n > h-3 {
		n = h - 3
	}
	wide := iw >= 92
	header := "WORKER                HOST                CORES  MEM      STATE    DONE   WORDS      RATE"
	if !wide {
		header = "WORKER                HOST                CORES  MEM      STATE"
	}
	c.Text(ix, iy, termui.Truncate(header, iw), colDim)
	c.HLine(ix, iy+1, iw, '─', colBorder)
	row := 0
	for i := 0; i < len(m.order) && row < n; i++ {
		wr := m.workers[m.order[i]]
		state := "alive"
		styl := colText
		if !wr.Alive {
			state = "DEAD"
			styl = colHot
		}
		line := fmt.Sprintf("%-21s %-20s %-5d  %-8s %-8s %-6d %-11s %s",
			termui.Truncate(wr.ID, 21), termui.Truncate(wr.FileAddr, 20), wr.Cores,
			common.HumanBytes(wr.MemoryMB<<20), state, wr.TasksDone,
			common.Humanize(wr.WordsCounted), common.Humanize(int64(wr.Rate))+"/s")
		if !wide {
			line = fmt.Sprintf("%-21s %-20s %-5d  %-8s %s",
				termui.Truncate(wr.ID, 21), termui.Truncate(wr.FileAddr, 20), wr.Cores,
				common.HumanBytes(wr.MemoryMB<<20), state)
		}
		c.Text(ix, iy+2+row, termui.Truncate(line, iw), styl)
		row++
	}
	if n < len(m.order) {
		c.Text(ix, iy+2+n, fmt.Sprintf("… and %d more", len(m.order)-n), colDim)
	}
}

func (m *Master) drawTasks(c *termui.Canvas, x, y, w, h int, frame int) {
	c.TitledBox(x, y, w, h, "TASKS", colBorder, colTitle)
	iw := w - 2
	ix := x + 1
	iy := y + 1
	if m.job == nil || len(m.mapTasks) == 0 {
		c.Text(ix, iy, "no tasks yet — waiting for the job to start", colDim)
		return
	}
	rows := h - 2
	if rows > 4 {
		rows = 4
	}
	m.drawTaskGrid(c, ix, iy, iw, 2, m.job.M, "map", frame)
	if rows >= 4 {
		m.drawTaskGrid(c, ix, iy+2, iw, 2, m.job.R, "reduce", frame)
	} else {
		m.drawTaskGrid(c, ix, iy+1, iw, 1, m.job.R, "reduce", frame)
	}
}

func (m *Master) drawTaskGrid(c *termui.Canvas, x, y, w, maxRows int, total int, kind string, frame int) {
	if total <= 0 {
		return
	}
	label := "MAP    "
	gridW := w - len(label) - 12
	if kind == "reduce" {
		label = "REDUCE "
	}
	cols := gridW / 5
	if cols < 1 {
		cols = 1
	}
	rows := (total + cols - 1) / cols
	truncated := false
	if rows > maxRows {
		rows = maxRows
		truncated = true
	}
	for r := 0; r < rows; r++ {
		if r == 0 {
			c.Text(x, y+r, label, colDim)
		} else {
			c.Text(x, y+r, "       ", colDim)
		}
		for col := 0; col < cols; col++ {
			idx := r*cols + col
			if idx >= total {
				break
			}
			var st common.TaskState
			if kind == "map" {
				st = m.mapTasks[idx].State
			} else {
				st = m.reduceTasks[idx].State
			}
			short := fmt.Sprintf("%03d", idx)
			cx := x + len(label) + col*5
			c.Text(cx, y+r, short, colDim)
			switch st {
			case common.TaskIdle:
				c.Set(cx+3, y+r, '·', colDim)
			case common.TaskInProgress:
				c.Set(cx+3, y+r, termui.Spinner(frame+idx), colYellow)
			case common.TaskCompleted:
				c.Set(cx+3, y+r, '■', colGood)
			}
		}
	}
	var doneN, totalN int
	if kind == "map" {
		doneN, totalN = m.mapDoneLocked(), m.job.M
	} else {
		doneN, totalN = m.reduceDoneLocked(), m.job.R
	}
	summary := fmt.Sprintf(" %d/%d", doneN, totalN)
	if truncated {
		summary = fmt.Sprintf(" %d/%d +%d more", doneN, totalN, total-rows*cols)
	}
	sx := x + len(label) + cols*5 + 1
	if sx+len(summary) < x+w {
		c.Text(sx, y, summary, colText)
	}
}

func (m *Master) drawResults(c *termui.Canvas, x, y, w, h int) {
	c.TitledBox(x, y, w, h, "RESULTS — TOP WORDS", colBorder, colGood)
	iw := w - 2
	ix := x + 1
	iy := y + 1
	if len(m.topWords) == 0 {
		c.Text(ix, iy, "see wordcount.tsv for the full result", colDim)
		return
	}
	var max int64
	for _, t := range m.topWords {
		if t.Count > max {
			max = t.Count
		}
	}
	rows := h - 2
	if rows > len(m.topWords) {
		rows = len(m.topWords)
	}
	for i := 0; i < rows; i++ {
		t := m.topWords[i]
		barW := iw - 29
		if barW < 4 {
			barW = 4
		}
		bar := int(float64(t.Count) / float64(max) * float64(barW))
		c.Text(ix, iy+i, fmt.Sprintf("%-16s %9s ", t.Word, common.Humanize(t.Count)), colText)
		c.HLine(ix+27, iy+i, barW, '░', colBorder)
		for k := 0; k < bar; k++ {
			c.Set(ix+27+k, iy+i, '█', colGood)
		}
	}
}

func (m *Master) drawLog(c *termui.Canvas, x, y, w, h int) {
	c.TitledBox(x, y, w, h, "EVENT LOG", colBorder, colTitle)
	if h < 3 {
		return
	}
	iw := w - 2
	lines := m.log.Last(h - 2)
	for i, line := range lines {
		if len(line) < 9 {
			continue
		}
		ts := line[:8]
		msg := line[9:]
		c.Text(x+1, y+1+i, ts, colDim)
		c.Text(x+10, y+1+i, termui.Truncate(msg, iw-10), colText)
	}
}

func (m *Master) drawFooter(c *termui.Canvas, y, w int) {
	out := m.cfg.OutputDir
	footer := fmt.Sprintf(" ctrl+c to exit · GET /api/status · final result → %s/wordcount.tsv ", out)
	c.Text(0, y, termui.Truncate(footer, w), colDim)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
