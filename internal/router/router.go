// Package router decides which model process runs and hands requests to it.
// Only one model runs at a time. Work waits in the router as tickets, one
// FIFO queue per model, and a time-share scheduler decides which model holds
// the GPU (see docs/time-share-plan.md).
package router

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
	"github.com/giapnguyen74/simple-ai-router/internal/process"
)

var (
	ErrUnknownModel = errors.New("unknown model")
	ErrQueueFull    = errors.New("queue full")
	ErrQueueTimeout = errors.New("timed out waiting in the queue")
	ErrShutdown     = errors.New("router is shutting down")
)

const ttlCheckInterval = 5 * time.Second

// Kind tells a request that stays open (sync) from a stored job.
type Kind int

const (
	KindSync Kind = iota
	KindJob
)

type ticketState int

const (
	ticketWaiting ticketState = iota
	ticketAdmitted
	ticketDone
)

// Ticket is one unit of work waiting for, or holding, a slot on its model.
type Ticket struct {
	r     *Router
	m     *model
	kind  Kind
	enq   time.Time
	state ticketState
	// admitted is closed when the ticket is admitted, or fails (err is set).
	admitted chan struct{}
	err      error
	ready    <-chan struct{}
}

// model is the scheduler's view of one configured model.
type model struct {
	cfg *config.ModelConfig
	p   *process.Process

	queue        []*Ticket // waiting, oldest first
	admitted     int
	admittedJobs int
	// idleSince is when the model last had neither waiting nor admitted work.
	idleSince time.Time
	// lastHeld is when the model last gave up the GPU. A model's wait for
	// its next turn counts from here, so a long queue cannot starve others.
	lastHeld time.Time

	unload        bool
	unloadWaiters []chan struct{}
	// bypass lets requests through while the model drains. Only models with
	// a busyCheck get it: their clients poll a job queue inside the server.
	bypass bool
}

type Router struct {
	cfg    *config.Config
	procs  map[string]*process.Process
	models map[string]*model

	// mu guards everything below and every model and ticket field. Nothing
	// that blocks (drain, stop, health check) runs with it held.
	mu sync.Mutex
	// active is the model that holds the GPU: starting, serving or yielding.
	active *model
	// yielding is set while the active model is being drained and stopped.
	// Nothing is admitted and no other decision is taken until it is done.
	yielding bool
	// sliceStart is when the active model became ready; zero until then.
	sliceStart time.Time
	epoch      int // bumped on every activation
	watching   bool
	timer      *time.Timer
	closed     bool
}

func New(cfg *config.Config, logOut io.Writer) *Router {
	r := &Router{
		cfg:    cfg,
		procs:  make(map[string]*process.Process),
		models: make(map[string]*model),
	}
	now := time.Now()
	for name, m := range cfg.Models {
		p := process.New(m, cfg.HealthCheckTimeout, logOut)
		r.procs[name] = p
		r.models[name] = &model{cfg: m, p: p, idleSince: now}
	}
	return r
}

func (r *Router) Config() *config.Config { return r.cfg }

// Acquire waits for a slot on the model (name or alias) and for the model to
// be ready. The caller must call release when the request is done.
func (r *Router) Acquire(ctx context.Context, name string) (p *process.Process, release func(), err error) {
	r.mu.Lock()
	mc, ok := r.cfg.Resolve(name)
	if !ok {
		r.mu.Unlock()
		return nil, nil, ErrUnknownModel
	}
	m := r.models[mc.Name]
	if r.yielding && r.active == m && m.bypass && !r.closed {
		m.p.Acquire()
		r.mu.Unlock()
		var once sync.Once
		return m.p, func() { once.Do(m.p.Release) }, nil
	}
	t, err := r.enqueueLocked(m, KindSync, false)
	r.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return t.Wait(ctx)
}

// Enqueue puts a ticket in the model's queue and returns at once. The caller
// must call Wait on it.
func (r *Router) Enqueue(name string, kind Kind) (*Ticket, error) {
	return r.enqueue(name, kind, false)
}

// Requeue is Enqueue for work the router already accepted (jobs found on
// disk at start): maxQueue does not apply.
func (r *Router) Requeue(name string, kind Kind) (*Ticket, error) {
	return r.enqueue(name, kind, true)
}

func (r *Router) enqueue(name string, kind Kind, force bool) (*Ticket, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mc, ok := r.cfg.Resolve(name)
	if !ok {
		return nil, ErrUnknownModel
	}
	return r.enqueueLocked(r.models[mc.Name], kind, force)
}

func (r *Router) enqueueLocked(m *model, kind Kind, force bool) (*Ticket, error) {
	if r.closed {
		return nil, ErrShutdown
	}
	if !force && m.cfg.MaxQueue > 0 && len(m.queue) >= m.cfg.MaxQueue {
		return nil, ErrQueueFull
	}
	t := &Ticket{r: r, m: m, kind: kind, enq: time.Now(), admitted: make(chan struct{})}
	m.queue = append(m.queue, t)
	r.scheduleLocked()
	return t, nil
}

// QueueFull reports whether a new ticket for the model would be refused.
func (r *Router) QueueFull(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	mc, ok := r.cfg.Resolve(name)
	if !ok {
		return false
	}
	m := r.models[mc.Name]
	return m.cfg.MaxQueue > 0 && len(m.queue) >= m.cfg.MaxQueue
}

// Wait blocks until the ticket is admitted and its model is ready. On
// success the caller must call release when the work is done. On error the
// ticket is gone and there is nothing to release.
func (t *Ticket) Wait(ctx context.Context) (p *process.Process, release func(), err error) {
	var timeout <-chan time.Time
	if d := t.m.cfg.QueueTimeout; d > 0 {
		timer := time.NewTimer(max(0, d-time.Since(t.enq)))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-t.admitted:
	case <-ctx.Done():
		err = ctx.Err()
	case <-timeout:
		err = ErrQueueTimeout
	}
	if err != nil {
		if t.abandon() {
			return nil, nil, err
		}
		// Admitted, or failed, at the same moment: give the slot back.
		<-t.admitted
		t.release()
		return nil, nil, err
	}
	if t.err != nil {
		return nil, nil, t.err
	}
	if err := process.WaitReady(ctx, t.ready); err != nil {
		t.release()
		return nil, nil, err
	}
	if err := t.m.p.StartErr(); err != nil {
		t.release()
		return nil, nil, err
	}
	return t.m.p, t.release, nil
}

// Model is the name of the ticket's model.
func (t *Ticket) Model() string { return t.m.cfg.Name }

// abandon removes a waiting ticket from its queue. It returns false when the
// ticket is no longer waiting.
func (t *Ticket) abandon() bool {
	r := t.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.state != ticketWaiting {
		return false
	}
	t.state = ticketDone
	for i, q := range t.m.queue {
		if q == t {
			t.m.queue = append(t.m.queue[:i], t.m.queue[i+1:]...)
			break
		}
	}
	r.markIdleLocked(t.m)
	r.scheduleLocked()
	return true
}

func (t *Ticket) release() {
	r := t.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.state != ticketAdmitted {
		return
	}
	t.state = ticketDone
	t.m.admitted--
	if t.kind == KindJob {
		t.m.admittedJobs--
	}
	t.m.p.Release()
	r.markIdleLocked(t.m)
	r.scheduleLocked()
}

func (r *Router) markIdleLocked(m *model) {
	if m.admitted == 0 && len(m.queue) == 0 {
		m.idleSince = time.Now()
	}
}

// scheduleLocked takes every decision that is due. It never blocks: stopping
// a model is handed to a goroutine, and scheduleLocked runs again when that
// is done. It is called on every event: ticket added or removed, request
// finished, model ready, timer.
func (r *Router) scheduleLocked() {
	if r.closed || r.yielding {
		return
	}
	if r.active == nil {
		m := r.pickNextLocked()
		if m == nil {
			return
		}
		r.active = m
		r.sliceStart = time.Time{}
		r.epoch++
		slog.Info("model takes the GPU", "model", m.cfg.Name, "queued", len(m.queue))
	}
	a := r.active
	if a.unload {
		r.startYieldLocked(a, "unload")
		return
	}

	now := time.Now()
	var next time.Time // when to look again
	if r.contendedLocked(a) {
		if !r.sliceStart.IsZero() {
			end := r.sliceStart.Add(r.sliceLocked(a))
			if !now.Before(end) {
				r.startYieldLocked(a, "slice ended")
				return
			}
			next = end
		}
		if a.admitted == 0 && len(a.queue) == 0 {
			end := a.idleSince.Add(*a.cfg.Linger)
			if !now.Before(end) {
				r.startYieldLocked(a, "idle")
				return
			}
			if next.IsZero() || end.Before(next) {
				next = end
			}
		}
	}
	r.admitLocked(a)
	if !next.IsZero() {
		r.setTimerLocked(next.Sub(now))
	}
}

// contendedLocked reports whether a model other than a has work waiting.
func (r *Router) contendedLocked(a *model) bool {
	for _, m := range r.models {
		if m != a && len(m.queue) > 0 {
			return true
		}
	}
	return false
}

// sliceLocked is the active model's slice: its share of the period among the
// models that have work, and at least minSlice.
func (r *Router) sliceLocked(a *model) time.Duration {
	total := 0
	for _, m := range r.models {
		if m == a || len(m.queue) > 0 || m.admitted > 0 {
			total += m.cfg.Share
		}
	}
	ts := r.cfg.TimeShare
	slice := time.Duration(float64(ts.Period) * float64(a.cfg.Share) / float64(total))
	return max(slice, ts.MinSlice)
}

// pickNextLocked returns the model that has waited longest for the GPU: by
// its oldest ticket, or by when it last gave the GPU up if that is later.
func (r *Router) pickNextLocked() *model {
	var best *model
	var bestAt time.Time
	for _, m := range r.models {
		if len(m.queue) == 0 {
			continue
		}
		at := m.queue[0].enq
		if m.lastHeld.After(at) {
			at = m.lastHeld
		}
		if best == nil || at.Before(bestAt) || (at.Equal(bestAt) && m.cfg.Name < best.cfg.Name) {
			best, bestAt = m, at
		}
	}
	return best
}

// admitLocked admits the active model's waiting tickets, oldest first, as
// far as its concurrency allows. A job that has to wait for the running job
// does not hold back the sync requests behind it.
func (r *Router) admitLocked(a *model) {
	if len(a.queue) == 0 {
		return
	}
	jobSlots := max(1, a.cfg.Concurrency)
	kept := a.queue[:0]
	for _, t := range a.queue {
		full := a.cfg.Concurrency > 0 && a.admitted >= a.cfg.Concurrency
		if full || (t.kind == KindJob && a.admittedJobs >= jobSlots) {
			kept = append(kept, t)
			continue
		}
		t.state = ticketAdmitted
		a.admitted++
		if t.kind == KindJob {
			a.admittedJobs++
		}
		t.ready = a.p.Begin()
		// Counted as in flight before the model is ready, so a yield waits
		// for it.
		a.p.Acquire()
		close(t.admitted)
		r.watchReadyLocked(a, t.ready)
	}
	for i := len(kept); i < len(a.queue); i++ {
		a.queue[i] = nil
	}
	a.queue = kept
}

// watchReadyLocked starts the slice once the model is ready: load time is
// not charged to it.
func (r *Router) watchReadyLocked(a *model, ready <-chan struct{}) {
	if !r.sliceStart.IsZero() || r.watching {
		return
	}
	r.watching = true
	epoch := r.epoch
	go func() {
		<-ready
		r.mu.Lock()
		defer r.mu.Unlock()
		r.watching = false
		if r.closed || r.epoch != epoch || r.active != a || !r.sliceStart.IsZero() {
			return
		}
		if a.p.State() == process.StateReady {
			r.sliceStart = time.Now()
		}
		r.scheduleLocked()
	}()
}

func (r *Router) setTimerLocked(d time.Duration) {
	d = max(d, time.Millisecond)
	if r.timer == nil {
		r.timer = time.AfterFunc(d, func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.scheduleLocked()
		})
		return
	}
	r.timer.Reset(d)
}

// startYieldLocked stops admitting to the active model, waits for its work
// to end (bounded by its drainTimeout), stops it and frees the GPU.
func (r *Router) startYieldLocked(a *model, reason string) {
	r.yielding = true
	a.bypass = a.cfg.BusyCheck != nil
	slog.Info("model yields the GPU", "model", a.cfg.Name, "reason", reason,
		"admitted", a.admitted, "queued", len(a.queue))
	go func() {
		deadline := time.Now().Add(a.cfg.DrainTimeout)
		a.p.Drain(a.cfg.DrainTimeout)
		r.mu.Lock()
		bypassed := a.bypass
		a.bypass = false
		r.mu.Unlock()
		if bypassed {
			// A request may have slipped through the bypass after the drain
			// saw the model idle; wait for it too.
			a.p.WaitInflight(max(time.Until(deadline), time.Millisecond))
		}
		a.p.Stop(0)

		r.mu.Lock()
		defer r.mu.Unlock()
		r.finishYieldLocked(a)
	}()
}

func (r *Router) finishYieldLocked(a *model) {
	a.lastHeld = time.Now()
	a.unload = false
	for _, ch := range a.unloadWaiters {
		close(ch)
	}
	a.unloadWaiters = nil
	if r.active == a {
		r.active = nil
	}
	r.yielding = false
	r.scheduleLocked()
}

// Unload stops one model, or the running model when name is empty, after its
// admitted work has finished (bounded by its drainTimeout). Its waiting
// tickets stay queued and load it again when its turn comes.
func (r *Router) Unload(name string) error {
	r.mu.Lock()
	var m *model
	if name == "" {
		m = r.active
	} else {
		mc, ok := r.cfg.Resolve(name)
		if !ok {
			r.mu.Unlock()
			return ErrUnknownModel
		}
		m = r.models[mc.Name]
	}
	if m == nil || r.closed {
		r.mu.Unlock()
		return nil
	}
	if r.active != m {
		r.mu.Unlock()
		// Not holding the GPU. A child can still be around after a failed
		// start.
		if m.p.Running() {
			m.p.Stop(0)
		}
		return nil
	}
	done := make(chan struct{})
	m.unload = true
	m.unloadWaiters = append(m.unloadWaiters, done)
	r.scheduleLocked()
	r.mu.Unlock()
	<-done
	return nil
}

// ForceStop stops the model at once, without waiting for its work. It is for
// a model that does not give a slot back, such as one that keeps running a
// cancelled job.
func (r *Router) ForceStop(name string) {
	r.mu.Lock()
	mc, ok := r.cfg.Resolve(name)
	if !ok || r.closed {
		r.mu.Unlock()
		return
	}
	m := r.models[mc.Name]
	if r.active != m || r.yielding {
		r.mu.Unlock()
		return
	}
	r.yielding = true
	r.mu.Unlock()
	slog.Warn("stopping model without draining", "model", m.cfg.Name)
	m.p.Stop(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishYieldLocked(m)
}

// Ready reports whether the model is loaded right now, and where it listens.
func (r *Router) Ready(name string) (proxy string, ok bool) {
	mc, found := r.cfg.Resolve(name)
	if !found {
		return "", false
	}
	return mc.Proxy, r.procs[mc.Name].State() == process.StateReady
}

// Shutdown fails every waiting ticket and stops every model without draining.
func (r *Router) Shutdown() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		if r.timer != nil {
			r.timer.Stop()
		}
		for _, m := range r.models {
			for _, t := range m.queue {
				t.state = ticketDone
				t.err = ErrShutdown
				close(t.admitted)
			}
			m.queue = nil
			for _, ch := range m.unloadWaiters {
				close(ch)
			}
			m.unloadWaiters = nil
		}
	}
	r.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range r.procs {
		if p.Running() {
			wg.Go(func() { p.Stop(0) })
		}
	}
	wg.Wait()
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
	a := r.active
	if a == nil || r.yielding || r.closed || a.cfg.TTL <= 0 || a.admitted > 0 || len(a.queue) > 0 {
		r.mu.Unlock()
		return
	}
	if idle, ok := a.p.IdleFor(); !ok || idle < a.cfg.TTL {
		r.mu.Unlock()
		return
	}
	// Reserved: tickets that arrive now wait until this is decided.
	r.yielding = true
	r.mu.Unlock()

	busy := a.p.Busy()
	if busy {
		a.p.Touch()
	} else {
		slog.Info("model yields the GPU", "model", a.cfg.Name, "reason", "ttl")
		a.p.Stop(0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if busy {
		r.yielding = false
		r.scheduleLocked()
		return
	}
	r.finishYieldLocked(a)
}

// Info is the status of one model: its process and its place in the
// schedule.
type Info struct {
	process.Info
	// Queued is the number of tickets waiting in the router.
	Queued int `json:"queued"`
	// Admitted is the number of requests and jobs the model is working on.
	Admitted int `json:"admitted"`
	// Active is true for the model that holds the GPU.
	Active bool `json:"active"`
	// SliceRemaining is set while another model waits for the GPU.
	SliceRemaining string `json:"sliceRemaining,omitempty"`
	// Jobs counts the model's stored jobs by status. Filled in by the server.
	Jobs map[string]int `json:"jobs,omitempty"`
}

// Infos returns the status of every model, sorted by name.
func (r *Router) Infos() []Info {
	r.mu.Lock()
	defer r.mu.Unlock()
	infos := make([]Info, 0, len(r.models))
	for _, m := range r.models {
		info := Info{Info: m.p.Info(), Queued: len(m.queue), Admitted: m.admitted, Active: r.active == m}
		if info.Active && !r.sliceStart.IsZero() && r.contendedLocked(m) {
			left := time.Until(r.sliceStart.Add(r.sliceLocked(m)))
			info.SliceRemaining = max(left, 0).Round(time.Second).String()
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}
