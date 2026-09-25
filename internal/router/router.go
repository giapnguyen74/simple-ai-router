// Package router decides which model process runs and hands requests to it.
// Only one model runs at a time: acquiring a model stops every other one.
package router

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
	"github.com/giapnguyen74/simple-ai-router/internal/process"
)

var ErrUnknownModel = errors.New("unknown model")

const ttlCheckInterval = 5 * time.Second

type Router struct {
	cfg   *config.Config
	procs map[string]*process.Process

	// mu serializes swap decisions, starts and stops.
	mu sync.Mutex

	// draining holds models being drained before a stop. mu is held for the
	// whole drain, so requests for these models bypass it: clients can keep
	// polling a job server that is finishing its queue.
	drainMu  sync.Mutex
	draining map[*process.Process]bool
}

func New(cfg *config.Config, logOut io.Writer) *Router {
	r := &Router{
		cfg:      cfg,
		procs:    make(map[string]*process.Process),
		draining: make(map[*process.Process]bool),
	}
	for name, m := range cfg.Models {
		r.procs[name] = process.New(m, cfg.HealthCheckTimeout, logOut)
	}
	return r
}

// Acquire makes sure the model (name or alias) is running and ready, stopping
// any other model first. The caller must call release when the request is done.
func (r *Router) Acquire(ctx context.Context, name string) (p *process.Process, release func(), err error) {
	m, ok := r.cfg.Resolve(name)
	if !ok {
		return nil, nil, ErrUnknownModel
	}
	p = r.procs[m.Name]

	var once sync.Once
	release = func() { once.Do(p.Release) }

	r.drainMu.Lock()
	if r.draining[p] {
		p.Acquire()
		r.drainMu.Unlock()
		return p, release, nil
	}
	r.drainMu.Unlock()

	r.mu.Lock()
	for _, other := range r.procs {
		if other != p && other.Running() {
			r.drainAndStop(other)
		}
	}
	ready := p.Begin()
	// Count the request as in flight before waiting, so a concurrent swap
	// away from this model waits for it.
	p.Acquire()
	r.mu.Unlock()

	if err := process.WaitReady(ctx, ready); err != nil {
		release()
		return nil, nil, err
	}
	if err := p.StartErr(); err != nil {
		release()
		return nil, nil, err
	}
	return p, release, nil
}

// Unload stops one model, or every model when name is empty.
func (r *Router) Unload(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name == "" {
		r.forEachRunning(r.drainAndStop)
		return nil
	}
	m, ok := r.cfg.Resolve(name)
	if !ok {
		return ErrUnknownModel
	}
	r.drainAndStop(r.procs[m.Name])
	return nil
}

// Shutdown stops every model without draining.
func (r *Router) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forEachRunning(func(p *process.Process) { p.Stop(0) })
}

// forEachRunning calls fn on every running model in parallel.
func (r *Router) forEachRunning(fn func(*process.Process)) {
	var wg sync.WaitGroup
	for _, p := range r.procs {
		if p.Running() {
			wg.Go(func() { fn(p) })
		}
	}
	wg.Wait()
}

// drainAndStop waits for p's requests and background work to finish (up to
// its drainTimeout), then stops it. Must be called with r.mu held.
func (r *Router) drainAndStop(p *process.Process) {
	r.drainMu.Lock()
	r.draining[p] = true
	r.drainMu.Unlock()

	p.Drain(p.Config().DrainTimeout)

	r.drainMu.Lock()
	delete(r.draining, p)
	r.drainMu.Unlock()
	p.Stop(0)
}

// RunTTL unloads models that have been idle longer than their ttl, until ctx
// is canceled.
func (r *Router) RunTTL(ctx context.Context) {
	t := time.NewTicker(ttlCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.unloadIdle()
		}
	}
}

func (r *Router) unloadIdle() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.procs {
		ttl := p.Config().TTL
		if ttl <= 0 {
			continue
		}
		// Re-checked under r.mu, so no new request can slip in between.
		if idle, ok := p.IdleFor(); ok && idle >= ttl {
			if p.Busy() {
				p.Touch()
				continue
			}
			p.Stop(0)
		}
	}
}

// Infos returns the status of every model, sorted by name.
func (r *Router) Infos() []process.Info {
	infos := make([]process.Info, 0, len(r.procs))
	for _, p := range r.procs {
		infos = append(infos, p.Info())
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}

func (r *Router) Config() *config.Config { return r.cfg }
