// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"cuteymr/internal/common"
	"cuteymr/internal/termui"
)

// Palette (kept in sync with the master dashboard).
var (
	wcolBorder = termui.Style{Fg: 240}
	wcolTitle  = termui.Style{Fg: 15, Bold: true}
	wcolDim    = termui.Style{Fg: 245}
	wcolText   = termui.Style{Fg: 250}
	wcolGood   = termui.Style{Fg: 114}
	wcolMap    = termui.Style{Fg: 114}
	wcolReduce = termui.Style{Fg: 213}
	wcolNet    = termui.Style{Fg: 80}
	wcolYellow = termui.Style{Fg: 220}
	wcolHot    = termui.Style{Fg: 196}
)

// RunDashboard renders the worker terminal dashboard until ctx is done.
func (w *Worker) RunDashboard(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	plain := w.cfg.Headless
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
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	frame := 0
	lastPlain := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if plain {
			if time.Since(lastPlain) >= 5*time.Second {
				lastPlain = time.Now()
				fmt.Println(w.plainLine())
			}
			if shotPath != "" {
				c := termui.NewCanvas(110, 40)
				w.mu.Lock()
				w.draw(c, frame)
				w.mu.Unlock()
				_ = os.WriteFile(shotPath, []byte(c.RenderPlain()), 0o644)
			}
			frame++
			continue
		}
		wd, ht := 110, 40
		if tw, th, err := termui.TerminalSize(); err == nil {
			wd, ht = tw, th
		}
		if wd > 120 {
			wd = 120
		}
		c := termui.NewCanvas(wd, ht)
		w.mu.Lock()
		w.draw(c, frame)
		w.mu.Unlock()
		fmt.Print(c.Render())
		if shotPath != "" {
			_ = os.WriteFile(shotPath, []byte(c.RenderPlain()), 0o644)
		}
		frame++
	}
}

func (w *Worker) plainLine() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	busy := 0
	for _, s := range w.slots {
		if s.TaskID != "" {
			busy++
		}
	}
	return fmt.Sprintf("%s: phase=%s slots=%d/%d busy tasks=%d words=%s errors=%d",
		w.ID, w.cluster.Phase, busy, len(w.slots),
		w.stats.MapDone+w.stats.ReduceDone, common.Humanize(w.stats.WordsEmitted), w.stats.Errors)
}

// draw renders the whole worker dashboard (call with w.mu held).
func (w *Worker) draw(c *termui.Canvas, frame int) {
	width, height := c.W, c.H
	if width < 60 || height < 16 {
		c.Text(0, 0, "CuteyMR worker — terminal too small (need >=60x16)", wcolTitle)
		return
	}
	y := w.drawHeader(c, 0, width)
	y++
	y = w.drawSlots(c, y, width)
	y++
	y = w.drawStats(c, y, width)
	y++
	y = w.drawCluster(c, y, width)
	y++
	w.drawLog(c, y, width, height-1-y)
	c.Text(0, height-1, termui.Truncate(
		fmt.Sprintf(" ctrl+c to exit · master %s · file server %s ", w.cfg.MasterURL, w.fileAddr), width), wcolDim)
}

func (w *Worker) drawHeader(c *termui.Canvas, y, width int) int {
	phase := w.cluster.Phase
	dot := wcolDim
	switch phase {
	case common.PhaseMap:
		dot = wcolMap
	case common.PhaseShuffle:
		dot = termui.Style{Fg: 171}
	case common.PhaseReduce:
		dot = wcolReduce
	case common.PhaseMerge:
		dot = wcolYellow
	case common.PhaseDone:
		dot = wcolGood
	case common.PhaseFailed:
		dot = wcolHot
	}
	c.Text(1, y, "●", dot)
	c.Text(3, y, "CUTEYMR WORKER", wcolTitle)
	c.Text(20, y, termui.Truncate(w.ID, 30), wcolText)
	if phase == common.PhaseDone {
		c.Text(20+len(termui.Truncate(w.ID, 30))+1, y, "(^▽^)", wcolGood)
	} else {
		busy := 0
		for _, s := range w.slots {
			if s.TaskID != "" {
				busy++
			}
		}
		if busy > 0 {
			c.Text(20+len(termui.Truncate(w.ID, 30))+1, y, "(>_<)", wcolYellow)
		}
	}
	right := fmt.Sprintf("%s · cluster %d/%d · %s",
		phaseLabel(phase), w.cluster.Alive, w.cluster.Target, termui.FmtDur(time.Since(w.startedAt)))
	c.TextRight(width-len(right)-1, y, len(right), right, wcolDim)
	c.HLine(0, y+1, width, '─', wcolBorder)
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

func (w *Worker) drawSlots(c *termui.Canvas, y, width int) int {
	n := len(w.slots)
	shown := n
	if shown > 10 {
		shown = 10
	}
	h := shown + 4
	if n > shown {
		h++ // extra line for "… and N more slots"
	}
	c.TitledBox(0, y, width, h, fmt.Sprintf("TASK SLOTS — %d cores, one goroutine each", n), wcolBorder, wcolTitle)
	iw := width - 2
	ix := 1
	iy := y + 1
	typeW := 8
	idW := 8
	detailW := 14
	barW := 20
	barX := ix + 5 + typeW + 1 + idW + 1 + detailW + 1
	wordsX := barX + barW + 6
	rateX := barX + barW + 15
	// header
	c.Text(ix, iy, fmt.Sprintf("%-4s %-*s %-*s %-*s", "slot", typeW, "type", idW, "task", detailW, "stage"), wcolDim)
	c.Text(barX, iy, "progress", wcolDim)
	c.Text(wordsX, iy, "words", wcolDim)
	if rateX+5 < ix+iw {
		c.Text(rateX, iy, "rate", wcolDim)
	}
	c.HLine(ix, iy+1, iw, '─', wcolBorder)
	for i := 0; i < shown; i++ {
		s := w.slots[i]
		row := iy + 2 + i
		if s.TaskID == "" {
			c.Text(ix, row, fmt.Sprintf("s%-2d", i), wcolDim)
			c.Text(ix+5, row, "— idle, waiting for tasks", wcolDim)
			continue
		}
		typ := s.Type
		typSt := wcolYellow
		if s.Type == common.TaskMap {
			typSt = wcolMap
		} else if s.Type == common.TaskReduce {
			typSt = wcolReduce
		}
		c.Text(ix, row, fmt.Sprintf("s%-2d", i), wcolDim)
		c.Text(ix+5, row, termui.PadRight(typ, typeW), typSt)
		c.Text(ix+5+typeW+1, row, termui.PadRight(termui.Truncate(s.TaskID, idW), idW), wcolText)
		c.Text(ix+5+typeW+1+idW+1, row, termui.PadRight(termui.Truncate(s.Detail, detailW), detailW), wcolDim)
		c.Bar(barX, row, barW, s.Progress, typSt, wcolBorder)
		pct := fmt.Sprintf("%3.0f%%", s.Progress*100)
		c.Text(barX+barW+1, row, pct, wcolDim)
		c.Text(wordsX, row, termui.PadRight(common.Humanize(s.Words), 8), wcolText)
		if rateX+5 < ix+iw {
			c.Text(rateX, row, common.Humanize(int64(s.Rate))+"/s", wcolDim)
		}
	}
	if n > shown {
		c.Text(ix, iy+2+shown, fmt.Sprintf("… and %d more slots", n-shown), wcolDim)
	}
	return y + h
}

func (w *Worker) drawStats(c *termui.Canvas, y, width int) int {
	c.TitledBox(0, y, width, 5, "THIS WORKER", wcolBorder, wcolTitle)
	iw := width - 2
	c.Text(1, y+1, fmt.Sprintf("tasks %d  (map %d · reduce %d)   words counted %s   pairs merged %s",
		w.stats.MapDone+w.stats.ReduceDone, w.stats.MapDone, w.stats.ReduceDone,
		common.Humanize(w.stats.WordsEmitted), common.Humanize(w.stats.WordsMerged)), wcolText)
	c.Text(1, y+2, fmt.Sprintf("read %s   wrote %s   errors %d   uptime %s",
		common.HumanBytes(w.stats.BytesRead), common.HumanBytes(w.stats.BytesWritten),
		w.stats.Errors, termui.FmtDur(time.Since(w.startedAt))), wcolDim)
	// a sparkline of the combined live slot rate
	var rates []float64
	totalRate := 0.0
	for _, s := range w.slots {
		totalRate += s.Rate
	}
	rates = append(rates, totalRate)
	w.rateHistory = append(w.rateHistory, totalRate)
	if len(w.rateHistory) > 60 {
		w.rateHistory = w.rateHistory[len(w.rateHistory)-60:]
	}
	c.Text(1, y+3, fmt.Sprintf("%s w/s", common.Humanize(int64(totalRate))), wcolYellow)
	c.Spark(12, y+3, iw-14, w.rateHistory, wcolYellow)
	return y + 5
}

func (w *Worker) drawCluster(c *termui.Canvas, y, width int) int {
	c.TitledBox(0, y, width, 4, "CLUSTER", wcolBorder, wcolNet)
	share := 0.0
	if w.cluster.ClusterWords > 0 {
		share = float64(w.stats.WordsEmitted) / float64(w.cluster.ClusterWords) * 100
	}
	c.Text(1, y+1, fmt.Sprintf("phase %s   workers %d/%d   cluster rate %s w/s",
		phaseLabel(w.cluster.Phase), w.cluster.Alive, w.cluster.Target,
		common.Humanize(int64(w.cluster.ClusterRate))), wcolText)
	c.Text(1, y+2, fmt.Sprintf("my share of counted words: %.1f%%  (%s of %s)",
		share, common.Humanize(w.stats.WordsEmitted), common.Humanize(w.cluster.ClusterWords)), wcolDim)
	return y + 4
}

func (w *Worker) drawLog(c *termui.Canvas, y, width, height int) {
	h := height
	if h < 5 {
		h = 5
	}
	c.TitledBox(0, y, width, h, "EVENT LOG", wcolBorder, wcolTitle)
	if h < 3 {
		return
	}
	iw := width - 2
	lines := w.log.Last(h - 2)
	for i, line := range lines {
		if len(line) < 9 {
			continue
		}
		c.Text(1, y+1+i, line[:8], wcolDim)
		c.Text(10, y+1+i, termui.Truncate(line[9:], iw-10), wcolText)
	}
}
