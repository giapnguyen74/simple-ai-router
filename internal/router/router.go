// Package router decides which model process runs and hands requests to it.
// Only one model runs at a time. Work waits in the router as tickets, one
// FIFO queue per model, booked into batches; each model runs its part of a
// batch in one turn (see docs/time-share-v2-plan.md).
package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
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
	ErrBusy         = errors.New("model busy")
)

// BusyError refuses work whose predicted start is too far away.
type BusyError struct {
	Model    string
	StartsIn time.Duration
}

func (e *BusyError) Error() string {
	return fmt.Sprintf("%s is busy: next turn in ~%s", e.Model, e.StartsIn.Round(time.Second))
}

func (e *BusyError) Is(target error) bool { return target == ErrBusy }

const ttlCheckInterval = 5 * time.Second

// Kind tells a request that stays open (sync) from a stored job.
type Kind int

const (
	KindSync Kind = iota
	KindJob
)

// Work describes a request, for the estimate of its cost.
type Work struct {
	// Path is the request path without the leading slash.
	Path string
	// Estimate is the workload's own estimate (eta_s from validate), or 0.
	Estimate time.Duration
	// NoWaitLimit skips the maxWait and syncMaxWait refusals (work the router
	// already accepted).
	NoWaitLimit bool
	// After are the tickets of the jobs this work depends on. The ticket is
	// booked after them and blocked (never admitted) until Unblock.
	After []*Ticket
}

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
	path  string
	enq   time.Time
	state ticketState
	// admitted is closed when the ticket is admitted, or fails (err is set).
	admitted chan struct{}
	err      error
	ready    <-chan struct{}

	// batch is the batch the ticket is booked in.
	batch int
	// cost is the estimate it is booked with; guess when nothing was known.
	cost  time.Duration
	guess bool
	// estimate is the workload's own estimate, to learn its correction.
	estimate time.Duration
	// start is when the work began (model ready); noSample skips learning.
	start    time.Time
	noSample bool
	// after are the tickets it depends on; blocked until they are done.
	after   []*Ticket
	blocked bool
}

// model is the scheduler's view of one configured model.
type model struct {
	cfg *config.ModelConfig
	p   *process.Process

	queue        []*Ticket // waiting, oldest first; batches never decrease
	running      []*Ticket // admitted
	admitted     int
	admittedJobs int
	// idleSince is when the model last had nothing admitted and nothing
	// waiting in the running batch.
	idleSince time.Time

	// Its turn in the running batch.
	turnDone   bool // over, or it has no part in the batch
	turnEnded  bool // quota used up: finishing admitted work, admitting none
	turnAdmits int
	quota      time.Duration
	// debt is the overrun of its last turn, taken off its next quota.
	debt time.Duration

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
	// turnStart is when the active model became ready in its turn; zero
	// until then. loadStart is when it was started, while it loads.
	turnStart time.Time
	loadStart time.Time
	// cur is the running batch, curStart when it started (or when another
	// model began to wait for a model that was alone), order its turns.
	cur       int
	curStart  time.Time
	order     []*model
	contended bool
	epoch     int // bumped on every activation
	watching  bool
	timer     *time.Timer
	closed    bool

	stats     *stats
	saveTimer *time.Timer
	started   time.Time
	switches  int
	loadTotal time.Duration
}

func New(cfg *config.Config, logOut io.Writer) *Router {
	now := time.Now()
	r := &Router{
		cfg:     cfg,
		procs:   make(map[string]*process.Process),
		models:  make(map[string]*model),
		stats:   loadStats(cfg.Jobs.Dir),
		started: now,
	}
	for name, m := range cfg.Models {
		p := process.New(m, cfg.HealthCheckTimeout, logOut)
		r.procs[name] = p
		r.models[name] = &model{cfg: m, p: p, idleSince: now, turnDone: true}
	}
	return r
}

func (r *Router) Config() *config.Config { return r.cfg }

// Acquire waits for a slot on the model (name or alias) and for the model to
// be ready. The caller must call release when the request is done.
func (r *Router) Acquire(ctx context.Context, name string) (p *process.Process, release func(), err error) {
	return r.AcquirePath(ctx, name, "")
}

// AcquirePath is Acquire for a request to path, whose learned duration is
// its cost in the schedule.
func (r *Router) AcquirePath(ctx context.Context, name, path string) (p *process.Process, release func(), err error) {
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
	t, err := r.enqueueLocked(m, KindSync, false, Work{Path: path})
	r.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return t.Wait(ctx)
}

// Enqueue puts a ticket in the model's queue and returns at once. The caller
// must call Wait on it.
func (r *Router) Enqueue(name string, kind Kind, w Work) (*Ticket, error) {
	return r.enqueue(name, kind, false, w)
}

// Requeue is Enqueue for work the router already accepted (jobs found on
// disk at start): maxQueue and maxWait do not apply.
func (r *Router) Requeue(name string, kind Kind, w Work) (*Ticket, error) {
	w.NoWaitLimit = true
	return r.enqueue(name, kind, true, w)
}

func (r *Router) enqueue(name string, kind Kind, force bool, w Work) (*Ticket, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mc, ok := r.cfg.Resolve(name)
	if !ok {
		return nil, ErrUnknownModel
	}
	return r.enqueueLocked(r.models[mc.Name], kind, force, w)
}

func (r *Router) enqueueLocked(m *model, kind Kind, force bool, w Work) (*Ticket, error) {
	if r.closed {
		return nil, ErrShutdown
	}
	if !force && m.cfg.MaxQueue > 0 && len(m.queue) >= m.cfg.MaxQueue {
		return nil, ErrQueueFull
	}
	now := time.Now()
	t := &Ticket{r: r, m: m, kind: kind, path: w.Path, enq: now, admitted: make(chan struct{}), estimate: w.Estimate,
		after: w.After, blocked: len(w.After) > 0}
	t.cost, t.guess = r.costLocked(m, w)
	if r.joinLocked(m, t, now) {
		t.batch = r.cur
		m.queue = append(m.queue, t)
		if r.contended {
			m.quota += t.cost
		}
	} else {
		moves := r.bookLocked(m, t)
		if limit := r.waitLimit(m, kind); limit > 0 && !w.NoWaitLimit && r.contendedFor(m) {
			place := r.predictLocked(now, true)[t]
			wait := place.StartsIn
			if r.active != m {
				wait -= r.predictOwnLoad(m) // its own load is not a wait for others
			}
			if wait > limit {
				m.queue = m.queue[:len(m.queue)-1]
				undo(moves)
				return nil, &BusyError{Model: m.cfg.Name, StartsIn: place.StartsIn}
			}
		}
	}
	r.scheduleLocked()
	return t, nil
}

func (r *Router) predictOwnLoad(m *model) time.Duration {
	if ms := r.stats.Models[m.cfg.Name]; ms != nil && ms.Loads > 0 {
		return r.loadTimeLocked(m)
	}
	return 0
}

func (r *Router) waitLimit(m *model, kind Kind) time.Duration {
	if kind == KindSync {
		return *m.cfg.SyncMaxWait
	}
	return m.cfg.MaxWait
}

// contendedFor reports whether another model has work booked or running, so
// that m's wait is a wait for others.
func (r *Router) contendedFor(m *model) bool {
	for _, o := range r.models {
		if o != m && (len(o.queue) > 0 || o.admitted > 0) {
			return true
		}
	}
	return false
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
		t.noSample = true
		t.release()
		return nil, nil, err
	}
	if t.err != nil {
		return nil, nil, t.err
	}
	if err := process.WaitReady(ctx, t.ready); err != nil {
		t.noSample = true
		t.release()
		return nil, nil, err
	}
	if err := t.m.p.StartErr(); err != nil {
		t.noSample = true
		t.release()
		return nil, nil, err
	}
	t.r.mu.Lock()
	t.start = time.Now()
	t.r.mu.Unlock()
	return t.m.p, t.release, nil
}

// Unblock lets a ticket booked with After be admitted: its dependencies are
// done.
func (t *Ticket) Unblock() {
	r := t.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if !t.blocked {
		return
	}
	t.blocked = false
	r.scheduleLocked()
}

// NoSample keeps the ticket's run out of what the router learns: the work
// failed and its duration says nothing about the next one.
func (t *Ticket) NoSample() {
	t.r.mu.Lock()
	t.noSample = true
	t.r.mu.Unlock()
}

// Place is where the waiting ticket stands; ok is false once it is admitted.
func (t *Ticket) Place() (Place, bool) {
	t.r.mu.Lock()
	defer t.r.mu.Unlock()
	if t.state != ticketWaiting {
		return Place{}, false
	}
	p, ok := t.r.predictLocked(time.Now(), false)[t]
	return p, ok
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
	t.m.queue = remove(t.m.queue, t)
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
	t.m.running = remove(t.m.running, t)
	if t.kind == KindJob {
		t.m.admittedJobs--
	}
	if !t.start.IsZero() && !t.noSample {
		d := time.Since(t.start)
		r.stats.observe(t.m.cfg.Name, t.path, d)
		if t.estimate > 0 {
			r.stats.observeEstimate(t.m.cfg.Name, t.estimate, d)
		}
		r.saveStatsLocked()
	}
	t.m.p.Release()
	r.markIdleLocked(t.m)
	r.scheduleLocked()
}

func remove(ts []*Ticket, t *Ticket) []*Ticket {
	for i, q := range ts {
		if q == t {
			return append(ts[:i], ts[i+1:]...)
		}
	}
	return ts
}

func (r *Router) markIdleLocked(m *model) {
	if m.admitted == 0 && !m.hasRunnable(r.cur) {
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
	now := time.Now()
	if r.active == nil {
		m := r.nextTurnLocked(now)
		if m == nil {
			return
		}
		r.activateLocked(m, now)
	}
	a := r.active
	if a.unload {
		r.startYieldLocked(a, "unload")
		return
	}

	if !r.contendedLocked(a) {
		// Alone: everything is admitted and the turn never ends. The batch
		// starts over when another model shows up.
		r.contended = false
		for _, t := range a.queue {
			t.batch = r.cur
		}
		a.turnDone, a.turnEnded = false, false
		r.curStart = now
		if !r.turnStart.IsZero() {
			r.turnStart = now
		}
		r.admitLocked(a)
		return
	}
	if !r.contended {
		r.contended = true
		r.contentionLocked(a, now)
	}

	var next time.Time // when to look again
	if !a.turnEnded && !r.turnStart.IsZero() && a.turnAdmits > 0 {
		end := r.turnStart.Add(a.quota)
		if !now.Before(end) {
			slog.Info("turn ends: quota used", "model", a.cfg.Name, "quota", a.quota.Round(time.Millisecond))
			a.turnEnded = true
			r.pushBackLocked(a)
		} else {
			next = end
		}
	}
	if !a.turnEnded {
		r.admitLocked(a)
	}
	if a.admitted == 0 && !a.hasRunnable(r.cur) {
		if !a.turnEnded {
			// Nothing left in its turn: wait linger for work that joins.
			end := a.idleSince.Add(*a.cfg.Linger)
			if now.Before(end) {
				if next.IsZero() || end.Before(next) {
					next = end
				}
				r.setTimerLocked(next.Sub(now))
				return
			}
		}
		r.finishTurnLocked(a, now)
		return
	}
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

// contentionLocked starts the running batch over when another model begins
// to wait for a model that was alone: its turn counts from now, and keeps
// only one cycle of its waiting work; the rest moves to the next batch.
func (r *Router) contentionLocked(a *model, now time.Time) {
	r.curStart = now
	// Its turn starts now: an idle model gets linger for its own client's
	// next request before it yields.
	a.turnDone, a.turnEnded = false, false
	if a.admitted == 0 {
		a.idleSince = now
	}
	if !r.turnStart.IsZero() {
		r.turnStart = now
	}
	var work time.Duration
	for _, t := range a.running {
		work += t.remaining(now)
	}
	over := false
	for _, t := range a.queue {
		if t.batch > r.cur {
			break
		}
		if over || work+t.cost > r.cfg.Batch.Cycle && (work > 0 || a.admitted > 0) {
			t.batch, over = r.cur+1, true // and every later one: arrival order
			continue
		}
		work += t.cost
	}
	r.rebalanceLocked(r.cur + 1)
	r.fixDepsLocked()
	a.quota = max(work, tolerance)
	a.turnAdmits = a.admitted
	r.order = []*model{a}
}

// finishTurnLocked ends the active model's turn: the next model of the
// running batch takes the GPU, or the next batch starts; when the active
// model has work in it, it goes first and keeps the GPU.
func (r *Router) finishTurnLocked(a *model, now time.Time) {
	a.turnDone = true
	if !r.turnStart.IsZero() {
		if over := now.Sub(r.turnStart) - a.quota; over > 0 {
			a.debt = min(over, r.cfg.Batch.Cycle)
		}
	}
	r.pushBackLocked(a)
	for _, m := range r.order {
		if m != a && !m.turnDone && m.hasWaiting(r.cur) {
			r.startYieldLocked(a, "turn over")
			return
		}
	}
	k := r.minBatchLocked()
	if k == 0 {
		// Nobody has work: the model stays loaded, alone again.
		r.contended = false
		return
	}
	if !a.hasWaiting(k) {
		r.startYieldLocked(a, "batch over")
		return
	}
	r.startBatchLocked(k, a, now)
	a.turnDone, a.idleSince = false, now
	if !r.turnStart.IsZero() {
		r.turnStart = now
	}
	r.scheduleLocked()
}

// nextTurnLocked returns the model whose turn is next: in the running batch,
// or the first of the next batch.
func (r *Router) nextTurnLocked(now time.Time) *model {
	for _, m := range r.order {
		if !m.turnDone && m.hasWaiting(r.cur) {
			return m
		}
	}
	k := r.minBatchLocked()
	if k == 0 {
		return nil
	}
	r.startBatchLocked(k, nil, now)
	return r.order[0]
}

func (r *Router) activateLocked(m *model, now time.Time) {
	r.active = m
	r.turnStart, r.loadStart = time.Time{}, time.Time{}
	r.epoch++
	m.turnEnded, m.turnAdmits, m.idleSince = false, 0, now
	if m.p.State() == process.StateReady {
		r.turnStart = now
	} else {
		r.loadStart = now
		r.switches++
	}
	slog.Info("model takes the GPU", "model", m.cfg.Name, "batch", r.cur, "queued", len(m.queue))
}

// admitLocked admits the active model's waiting tickets of the running
// batch, oldest first, as far as its concurrency allows. A job that has to
// wait for the running job does not hold back the sync requests behind it.
func (r *Router) admitLocked(a *model) {
	if len(a.queue) == 0 {
		return
	}
	jobSlots := max(1, a.cfg.Concurrency)
	kept := a.queue[:0]
	for _, t := range a.queue {
		full := a.cfg.Concurrency > 0 && a.admitted >= a.cfg.Concurrency
		if t.batch > r.cur || t.blocked || full || (t.kind == KindJob && a.admittedJobs >= jobSlots) {
			kept = append(kept, t)
			continue
		}
		t.state = ticketAdmitted
		a.admitted++
		a.turnAdmits++
		a.running = append(a.running, t)
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

// watchReadyLocked starts the turn's clock once the model is ready: load
// time is not charged to it, and is learned.
func (r *Router) watchReadyLocked(a *model, ready <-chan struct{}) {
	if !r.turnStart.IsZero() || r.watching {
		return
	}
	r.watching = true
	epoch := r.epoch
	go func() {
		<-ready
		r.mu.Lock()
		defer r.mu.Unlock()
		r.watching = false
		if r.closed || r.epoch != epoch || r.active != a || !r.turnStart.IsZero() {
			return
		}
		if a.p.State() == process.StateReady {
			now := time.Now()
			r.turnStart = now
			if !r.loadStart.IsZero() {
				d := now.Sub(r.loadStart)
				r.loadTotal += d
				r.stats.observeLoad(a.cfg.Name, d)
				r.saveStatsLocked()
				r.loadStart = time.Time{}
			}
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
	a.unload = false
	a.turnDone = true
	r.pushBackLocked(a)
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

	r.mu.Lock()
	if r.saveTimer != nil {
		r.saveTimer.Stop()
		r.saveTimer = nil
	}
	data := r.stats.snapshot()
	r.mu.Unlock()
	r.stats.write(data)

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
	// Turn is the model's turn in the running batch: "running", "next"
	// (still to come), "done", or empty when it has no part in it.
	Turn string `json:"turn,omitempty"`
	// QuotaS and UsedS are its time in the running batch while another
	// model waits; DebtS the overrun taken off its next turn.
	QuotaS *float64 `json:"quota_s,omitempty"`
	UsedS  *float64 `json:"used_s,omitempty"`
	DebtS  float64  `json:"debt_s,omitempty"`
	// LoadS is its learned (or configured) load time.
	LoadS float64 `json:"load_s"`
	// Jobs counts the model's stored jobs by status. Filled in by the server.
	Jobs map[string]int `json:"jobs,omitempty"`
}

func secs(d time.Duration) float64 { return float64(d.Round(time.Millisecond)) / float64(time.Second) }

// Infos returns the status of every model, sorted by name.
func (r *Router) Infos() []Info {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	infos := make([]Info, 0, len(r.models))
	for _, m := range r.models {
		info := Info{Info: m.p.Info(), Queued: len(m.queue), Admitted: m.admitted, Active: r.active == m,
			DebtS: secs(m.debt), LoadS: secs(r.loadTimeLocked(m))}
		switch {
		case info.Active && !m.turnDone:
			info.Turn = "running"
		case r.inOrder(m) && !m.turnDone:
			info.Turn = "next"
		case r.inOrder(m):
			info.Turn = "done"
		}
		if info.Active && r.contended {
			q := secs(m.quota)
			info.QuotaS = &q
			if !r.turnStart.IsZero() {
				u := secs(now.Sub(r.turnStart))
				info.UsedS = &u
			}
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}

// Schedule is the batch view of GET /running.
type Schedule struct {
	CycleS float64 `json:"cycle_s"`
	// ElapsedS is the running batch's age.
	ElapsedS float64         `json:"elapsed_s"`
	Batches  []BatchInfo     `json:"batches"`
	GPU      GPUInfo         `json:"gpu"`
	Paths    map[string]Path `json:"paths"`
}

// BatchInfo is one booked batch: 1 is the running one.
type BatchInfo struct {
	Batch    int         `json:"batch"`
	StartsIn float64     `json:"starts_in_s"`
	Models   []BatchPart `json:"models"`
}

type BatchPart struct {
	Model   string  `json:"model"`
	Tickets int     `json:"tickets"`
	WorkS   float64 `json:"work_s"`
}

type GPUInfo struct {
	UptimeS  float64 `json:"uptime_s"`
	Switches int     `json:"switches"`
	LoadS    float64 `json:"load_s"`
	// SwitchOverhead is the share of the uptime spent loading models.
	SwitchOverhead float64 `json:"switch_overhead"`
}

// Path is what the router learned about one model and path.
type Path struct {
	EstimateS float64 `json:"estimate_s"`
	Runs      int     `json:"runs"`
}

// Schedule returns the booked batches and what the router learned.
func (r *Router) Schedule() Schedule {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	places := r.predictLocked(now, false)
	byBatch := map[int]*BatchInfo{}
	parts := map[int]map[string]*BatchPart{}
	for _, m := range r.models {
		for _, t := range m.queue {
			pl, ok := places[t]
			if !ok {
				continue
			}
			b := byBatch[pl.Batch]
			if b == nil {
				b = &BatchInfo{Batch: pl.Batch, StartsIn: secs(pl.StartsIn)}
				byBatch[pl.Batch], parts[pl.Batch] = b, map[string]*BatchPart{}
			}
			b.StartsIn = min(b.StartsIn, secs(pl.StartsIn))
			p := parts[pl.Batch][m.cfg.Name]
			if p == nil {
				p = &BatchPart{Model: m.cfg.Name}
				parts[pl.Batch][m.cfg.Name] = p
			}
			p.Tickets++
			p.WorkS += secs(t.cost)
		}
	}
	s := Schedule{CycleS: secs(r.cfg.Batch.Cycle), Paths: map[string]Path{}}
	if !r.curStart.IsZero() && r.active != nil {
		s.ElapsedS = secs(now.Sub(r.curStart))
	}
	for k, b := range byBatch {
		for _, p := range parts[k] {
			b.Models = append(b.Models, *p)
		}
		sort.Slice(b.Models, func(i, j int) bool { return b.Models[i].Model < b.Models[j].Model })
		s.Batches = append(s.Batches, *b)
	}
	sort.Slice(s.Batches, func(i, j int) bool { return s.Batches[i].Batch < s.Batches[j].Batch })
	up := now.Sub(r.started)
	s.GPU = GPUInfo{UptimeS: secs(up), Switches: r.switches, LoadS: secs(r.loadTotal)}
	if up > 0 {
		s.GPU.SwitchOverhead = float64(r.loadTotal) / float64(up)
	}
	for key, recent := range r.stats.Paths {
		name, path, _ := strings.Cut(key, " /")
		if d, ok := r.stats.duration(name, path); ok {
			s.Paths[key] = Path{EstimateS: secs(d), Runs: len(recent)}
		}
	}
	return s
}

// Average is the mean duration of the recent runs of a model and path.
func (r *Router) Average(name, path string) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats.average(name, path)
}

// Seed adds a finished run found on disk to what the router learned, unless
// the learned data was itself found on disk (it then holds that run).
func (r *Router) Seed(name, path string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stats.loaded {
		r.stats.observe(name, path, d)
		r.saveStatsLocked()
	}
}

// saveStatsLocked writes what was learned a little later, once for a burst
// of changes.
func (r *Router) saveStatsLocked() {
	if r.saveTimer != nil || r.stats.path == "" {
		return
	}
	r.saveTimer = time.AfterFunc(statsSaveWait, func() {
		r.mu.Lock()
		r.saveTimer = nil
		data := r.stats.snapshot()
		r.mu.Unlock()
		r.stats.write(data)
	})
}
