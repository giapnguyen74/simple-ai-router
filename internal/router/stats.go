package router

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

const (
	statsFile     = "stats.json"
	statsDir      = ".router"
	statsSamples  = 20
	statsSaveWait = 5 * time.Second
	// ewma weighs a new load time or correction against what was learned.
	ewma = 0.3
)

// stats is what the router learns from finished work: how long each
// (model, path) takes, how long each model takes to load, and how far off
// each model's own estimates are. It is kept in <jobs.dir>/.router/stats.json
// so a restart keeps it. Guarded by Router.mu.
type stats struct {
	Models map[string]*modelStats `json:"models"`
	// Paths holds the last durations in seconds per "model /path".
	Paths map[string][]float64 `json:"paths"`

	path   string
	loaded bool // read from disk
	dirty  bool
	saving sync.Mutex
}

type modelStats struct {
	// LoadS is the learned load time in seconds (start to healthy).
	LoadS float64 `json:"load_s,omitempty"`
	Loads int     `json:"loads,omitempty"`
	// Correction is actual / estimated for the workload's own eta_s.
	Correction float64 `json:"correction,omitempty"`
}

func loadStats(jobsDir string) *stats {
	s := &stats{Models: map[string]*modelStats{}, Paths: map[string][]float64{}}
	if jobsDir == "" {
		return s
	}
	s.path = filepath.Join(jobsDir, statsDir, statsFile)
	data, err := os.ReadFile(s.path)
	if err != nil {
		return s
	}
	if err := json.Unmarshal(data, s); err != nil {
		slog.Warn("ignoring unreadable stats", "path", s.path, "err", err)
		s.Models, s.Paths = map[string]*modelStats{}, map[string][]float64{}
		return s
	}
	if s.Models == nil {
		s.Models = map[string]*modelStats{}
	}
	if s.Paths == nil {
		s.Paths = map[string][]float64{}
	}
	s.loaded = true
	return s
}

func pathKey(model, path string) string { return model + " /" + path }

func (s *stats) model(name string) *modelStats {
	ms := s.Models[name]
	if ms == nil {
		ms = &modelStats{}
		s.Models[name] = ms
	}
	return ms
}

// duration is the planning estimate for one run of (model, path): the 75th
// percentile of the recent runs. ok is false when nothing is known yet.
func (s *stats) duration(model, path string) (time.Duration, bool) {
	recent := s.Paths[pathKey(model, path)]
	if len(recent) == 0 {
		return 0, false
	}
	sorted := slices.Clone(recent)
	slices.Sort(sorted)
	p75 := sorted[(len(sorted)*3)/4]
	if len(sorted) < 4 {
		p75 = sorted[len(sorted)-1]
	}
	return seconds(p75), true
}

// average is the mean of the recent runs, for a running job's ETA.
func (s *stats) average(model, path string) (time.Duration, bool) {
	recent := s.Paths[pathKey(model, path)]
	if len(recent) == 0 {
		return 0, false
	}
	sum := 0.0
	for _, d := range recent {
		sum += d
	}
	return seconds(sum / float64(len(recent))), true
}

func (s *stats) observe(model, path string, d time.Duration) {
	key := pathKey(model, path)
	recent := append(s.Paths[key], d.Seconds())
	if n := len(recent); n > statsSamples {
		recent = recent[n-statsSamples:]
	}
	s.Paths[key] = recent
	s.dirty = true
}

func (s *stats) observeLoad(model string, d time.Duration) {
	ms := s.model(model)
	if ms.Loads == 0 {
		ms.LoadS = d.Seconds()
	} else {
		ms.LoadS = (1-ewma)*ms.LoadS + ewma*d.Seconds()
	}
	ms.Loads++
	s.dirty = true
}

// observeEstimate learns how far off the workload's own estimate was.
func (s *stats) observeEstimate(model string, estimated, actual time.Duration) {
	if estimated <= 0 || actual <= 0 {
		return
	}
	ratio := min(max(actual.Seconds()/estimated.Seconds(), 0.1), 10)
	ms := s.model(model)
	if ms.Correction == 0 {
		ms.Correction = ratio
	} else {
		ms.Correction = (1-ewma)*ms.Correction + ewma*ratio
	}
	s.dirty = true
}

func (s *stats) correction(model string) float64 {
	if ms := s.Models[model]; ms != nil && ms.Correction > 0 {
		return ms.Correction
	}
	return 1
}

func (s *stats) loadTime(model string, guess time.Duration) time.Duration {
	if ms := s.Models[model]; ms != nil && ms.Loads > 0 {
		return seconds(ms.LoadS)
	}
	return guess
}

// snapshot returns the data to write, or nil when nothing changed.
func (s *stats) snapshot() []byte {
	if !s.dirty || s.path == "" {
		return nil
	}
	s.dirty = false
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil
	}
	return append(data, '\n')
}

// write stores a snapshot atomically. It runs without Router.mu.
func (s *stats) write(data []byte) {
	if data == nil {
		return
	}
	s.saving.Lock()
	defer s.saving.Unlock()
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		slog.Warn("store stats", "err", err)
		return
	}
	tmp, err := os.CreateTemp(dir, statsFile+".*")
	if err != nil {
		slog.Warn("store stats", "err", err)
		return
	}
	_, err = tmp.Write(data)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), s.path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		slog.Warn("store stats", "err", err)
	}
}

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }
