package router

import (
	"log/slog"
	"sort"
	"time"
)

// Batch planning (docs/time-share-v2-plan.md, part A). Every waiting ticket
// is booked into a numbered batch; r.cur is the running one. A batch is
// shared among the models booked in it, max-min fair by share, and each model
// runs its part in one turn. All functions here run with r.mu held.

const tolerance = time.Millisecond

// costLocked estimates how long the work takes: the workload's own estimate
// corrected by what was learned, else the recent runs of the same path, else
// the model's defaultEta (guess is then true).
func (r *Router) costLocked(m *model, w Work) (cost time.Duration, guess bool) {
	if w.Estimate > 0 {
		return time.Duration(float64(w.Estimate) * r.stats.correction(m.cfg.Name)), false
	}
	if d, ok := r.stats.duration(m.cfg.Name, w.Path); ok {
		return d, false
	}
	return m.cfg.DefaultEta, true
}

func (r *Router) loadTimeLocked(m *model) time.Duration {
	return r.stats.loadTime(m.cfg.Name, m.cfg.LoadTime)
}

// hasWaiting reports whether m has waiting tickets booked in batch k.
func (m *model) hasWaiting(k int) bool {
	for _, t := range m.queue {
		if t.batch == k {
			return true
		}
		if t.batch > k {
			return false
		}
	}
	return false
}

// hasRunnable reports whether m has waiting tickets in batch k (or earlier)
// that can be admitted now: not blocked by a dependency.
func (m *model) hasRunnable(k int) bool {
	for _, t := range m.queue {
		if t.batch > k {
			return false
		}
		if !t.blocked {
			return true
		}
	}
	return false
}

// minAfter is the earliest batch a ticket may be booked in because of its
// dependencies: the batch of a waiting one, or the one after it when it
// belongs to another model (whose turn may come later in that batch).
func (t *Ticket) minAfter() int {
	k := 0
	for _, d := range t.after {
		if d.state != ticketWaiting {
			continue
		}
		dk := d.batch
		if d.m != t.m {
			dk++
		}
		k = max(k, dk)
	}
	return k
}

// fixDepsLocked moves tickets booked before what they depend on (after the
// dependency itself moved) later, with the model's tickets behind them.
func (r *Router) fixDepsLocked() {
	for range 100 {
		changed := false
		for _, m := range r.models {
			floor := 0
			for _, t := range m.queue {
				floor = max(floor, t.minAfter())
				if t.batch < floor {
					t.batch = floor
					changed = true
				}
				floor = max(floor, t.batch)
			}
		}
		if !changed {
			return
		}
	}
}

// lastBatch is the batch of m's last waiting ticket, 0 when none waits.
func (m *model) lastBatch() int {
	if len(m.queue) == 0 {
		return 0
	}
	return m.queue[len(m.queue)-1].batch
}

// remaining is what is left of an admitted ticket's work.
func (t *Ticket) remaining(now time.Time) time.Duration {
	if t.start.IsZero() {
		return t.cost
	}
	return max(0, t.cost-now.Sub(t.start))
}

// demandLocked is the booked work per model in batch k.
func (r *Router) demandLocked(k int) map[*model]time.Duration {
	d := make(map[*model]time.Duration)
	for _, m := range r.models {
		for _, t := range m.queue {
			if t.batch == k {
				d[m] += t.cost
			} else if t.batch > k {
				break
			}
		}
	}
	return d
}

// cycleLocked is the length of batch k, which adapts to what is booked in
// it: long enough that loading its models takes at most maxSwitchOverhead of
// it, and that its longest job fits, within minCycle and maxCycle. extra is
// a ticket about to be booked in it. loads is what its model loads take:
// the model already loaded when the batch starts costs none (the active
// model, for the running and the next batch; for later ones the guess is
// that it is the slowest-loading one).
func (r *Router) cycleLocked(k int, demand map[*model]time.Duration, extra time.Duration) (cycle, loads time.Duration) {
	b := r.cfg.Batch
	var slowest time.Duration
	carried := false
	for m := range demand {
		load := r.loadTimeLocked(m)
		if (k == r.cur || k == r.cur+1) && m == r.active {
			carried = true
			continue
		}
		loads += load
		slowest = max(slowest, load)
	}
	if !carried && k > r.cur+1 {
		loads -= slowest // the model loaded then is not known yet
	}
	longest := extra
	for _, m := range r.models {
		for _, t := range m.queue {
			if t.batch == k {
				longest = max(longest, t.cost)
			} else if t.batch > k {
				break
			}
		}
	}
	amortized := time.Duration(float64(loads) / b.MaxSwitchOverhead)
	cycle = min(max(b.MinCycle, amortized, longest+loads), b.MaxCycle)
	return cycle, loads
}

// budgetLocked is the time batch k has for work: its cycle minus its loads.
func (r *Router) budgetLocked(k int, demand map[*model]time.Duration, extra time.Duration) time.Duration {
	cycle, loads := r.cycleLocked(k, demand, extra)
	return max(cycle-loads, cycle/4)
}

// waterfill shares budget max-min fair by share: a model that needs less
// than its share gets what it needs, and the rest is shared among the others.
func waterfill(demand map[*model]time.Duration, budget time.Duration) map[*model]time.Duration {
	alloc := make(map[*model]time.Duration, len(demand))
	open := make([]*model, 0, len(demand))
	for m, d := range demand {
		if d > 0 {
			open = append(open, m)
		}
	}
	left := float64(budget)
	for len(open) > 0 {
		shares := 0
		for _, m := range open {
			shares += m.cfg.Share
		}
		level := left / float64(shares)
		kept := open[:0]
		for _, m := range open {
			if d := float64(demand[m]); d <= level*float64(m.cfg.Share) {
				alloc[m] = demand[m]
				left -= d
			} else {
				kept = append(kept, m)
			}
		}
		if len(kept) == len(open) {
			for _, m := range kept {
				alloc[m] = time.Duration(level * float64(m.cfg.Share))
			}
			break
		}
		open = kept
	}
	return alloc
}

// move records a ticket moved to a later batch, so a refused booking can be
// undone.
type move struct {
	t    *Ticket
	from int
}

// bookLocked books a ticket into the earliest later batch where its model
// still fits its fair part, never before the model's last booked ticket.
// A model with nothing booked in a batch always fits: a ticket longer than a
// whole part still runs, alone. Models over their part of that batch then
// move their last tickets to the next one.
func (r *Router) bookLocked(m *model, t *Ticket) []move {
	k := max(r.cur+1, m.lastBatch(), t.minAfter())
	for ; ; k++ {
		demand := r.demandLocked(k)
		if demand[m] == 0 {
			break
		}
		if t.guess && t.kind == KindJob && r.hasGuessLocked(m, k, t.path) {
			continue // measure the first run before booking more on a guess
		}
		demand[m] += t.cost
		if alloc := waterfill(demand, r.budgetLocked(k, demand, t.cost)); demand[m] <= alloc[m]+tolerance {
			break
		}
	}
	t.batch = k
	m.queue = append(m.queue, t)
	moves := r.rebalanceLocked(k)
	r.fixDepsLocked()
	return moves
}

func (r *Router) hasGuessLocked(m *model, k int, path string) bool {
	for _, t := range m.queue {
		if t.batch == k && t.guess && t.path == path {
			return true
		}
	}
	return false
}

// rebalanceLocked moves, from batch k on, the last tickets of every model
// over its part to the next batch, keeping at least one per model.
func (r *Router) rebalanceLocked(k int) []move {
	var moves []move
	for ; ; k++ {
		demand := r.demandLocked(k)
		if len(demand) == 0 {
			return moves
		}
		alloc := waterfill(demand, r.budgetLocked(k, demand, 0))
		moved := false
		for m, d := range demand {
			for i := len(m.queue) - 1; i >= 0 && d > alloc[m]+tolerance; i-- {
				t := m.queue[i]
				if t.batch > k {
					continue
				}
				if t.batch < k || i == 0 || m.queue[i-1].batch != k {
					break // its first ticket in k stays
				}
				moves = append(moves, move{t, t.batch})
				t.batch = k + 1
				d -= t.cost
				moved = true
			}
		}
		if !moved {
			return moves
		}
	}
}

func undo(moves []move) {
	for i := len(moves) - 1; i >= 0; i-- {
		moves[i].t.batch = moves[i].from
	}
}

// pushBackLocked moves m's waiting tickets of the running batch to the next
// one: its turn is over.
func (r *Router) pushBackLocked(m *model) {
	pushed := false
	for _, t := range m.queue {
		if t.batch <= r.cur {
			t.batch = r.cur + 1
			pushed = true
		}
	}
	if pushed {
		r.rebalanceLocked(r.cur + 1)
		r.fixDepsLocked()
	}
}

// joinLocked decides whether a new ticket joins the running batch. It does
// when its model is alone (nobody else waits), or when the model's turn in
// the running batch is not over, it has nothing booked later (arrival order),
// and the batch has unbooked time for it: booked tickets always keep their
// place.
func (r *Router) joinLocked(m *model, t *Ticket, now time.Time) bool {
	if r.active == m && !r.yielding && !r.contendedLocked(m) {
		return true
	}
	if r.cur == 0 || m.turnDone || m.lastBatch() > r.cur || t.minAfter() > r.cur {
		return false
	}
	switch {
	case r.active == m:
		if r.yielding || m.turnEnded {
			return false
		}
	case !m.hasWaiting(r.cur) || !r.inOrder(m):
		return false
	}
	elapsed := time.Duration(0)
	if !r.curStart.IsZero() {
		elapsed = now.Sub(r.curStart)
	}
	return elapsed+r.remainingLocked(now)+t.cost <= r.curCycle
}

func (r *Router) inOrder(m *model) bool {
	for _, o := range r.order {
		if o == m {
			return true
		}
	}
	return false
}

// remainingLocked is the running batch's work still to do: admitted tickets
// and those waiting in it.
func (r *Router) remainingLocked(now time.Time) time.Duration {
	var left time.Duration
	for _, m := range r.models {
		for _, t := range m.running {
			left += t.remaining(now)
		}
		for _, t := range m.queue {
			if t.batch > r.cur {
				break
			}
			left += t.cost
		}
	}
	return left
}

// minBatchLocked is the earliest batch with a waiting ticket, 0 when none.
func (r *Router) minBatchLocked() int {
	k := 0
	for _, m := range r.models {
		if len(m.queue) > 0 && (k == 0 || m.queue[0].batch < k) {
			k = m.queue[0].batch
		}
	}
	return k
}

// batchOrder is the order of turns in batch k: the carried (loaded) model
// first, then by oldest ticket; the last turn goes to a model that has work
// in the batch after, so that boundary costs no switch either.
func (r *Router) batchOrder(k int, carry *model) []*model {
	type entry struct {
		m      *model
		oldest time.Time
	}
	var es []entry
	for _, m := range r.models {
		for _, t := range m.queue {
			if t.batch == k {
				es = append(es, entry{m, t.enq})
				break
			}
			if t.batch > k {
				break
			}
		}
	}
	sort.Slice(es, func(i, j int) bool {
		if !es[i].oldest.Equal(es[j].oldest) {
			return es[i].oldest.Before(es[j].oldest)
		}
		return es[i].m.cfg.Name < es[j].m.cfg.Name
	})
	order := make([]*model, 0, len(es))
	for _, e := range es {
		if e.m == carry {
			order = append([]*model{e.m}, order...)
		} else {
			order = append(order, e.m)
		}
	}
	if len(order) > 2 {
		for i := len(order) - 1; i >= 1; i-- {
			if order[i].lastBatch() > k {
				m := order[i]
				order = append(append(order[:i:i], order[i+1:]...), m)
				break
			}
		}
	}
	return order
}

// startBatchLocked makes batch k the running one. Each model's quota is its
// fair part of the budget, or its booked work when that is more, minus the
// debt of its last overrun.
func (r *Router) startBatchLocked(k int, carry *model, now time.Time) {
	r.cur, r.curStart = k, now
	r.order = r.batchOrder(k, carry)
	demand := r.demandLocked(k)
	r.curCycle, _ = r.cycleLocked(k, demand, 0)
	budget := r.budgetLocked(k, demand, 0)
	alloc := waterfill(demand, budget)
	shares := 0
	for m := range demand {
		shares += m.cfg.Share
	}
	for _, m := range r.models {
		m.turnEnded, m.turnAdmits = false, 0
		d, in := demand[m]
		m.turnDone = !in
		if !in {
			continue
		}
		fair := time.Duration(float64(budget) * float64(m.cfg.Share) / float64(shares))
		m.quota = max(alloc[m], fair, d) - m.debt
		m.debt = 0
	}
	names := make([]string, len(r.order))
	for i, m := range r.order {
		names[i] = m.cfg.Name
	}
	slog.Info("batch starts", "batch", k, "cycle", r.curCycle.Round(time.Second), "order", names)
}

// Place is where a waiting ticket stands: its batch (1 = the running one) and
// when it is expected to start.
type Place struct {
	Batch    int
	StartsIn time.Duration
	// Cost is the ticket's own estimated run time.
	Cost time.Duration
}

// predictLocked replays the booked batches with the learned costs and load
// times, and returns the expected start of every waiting ticket. With sure,
// guesses (defaultEta, a load time never measured) count as nothing: the
// result is a start the ticket will not beat, for refusing work.
func (r *Router) predictLocked(now time.Time, sure bool) map[*Ticket]Place {
	res := make(map[*Ticket]Place)
	var at time.Duration
	prev := r.active
	rel := func(k int) int { return max(1, k-r.cur+1) }
	cost := func(t *Ticket, d time.Duration) time.Duration {
		if sure && t.guess {
			return 0
		}
		return d
	}
	load := func(m *model) time.Duration {
		if ms := r.stats.Models[m.cfg.Name]; sure && (ms == nil || ms.Loads == 0) {
			return 0
		}
		return r.loadTimeLocked(m)
	}
	turn := func(m *model, k int) {
		for _, t := range m.queue {
			if t.batch > k {
				break
			}
			res[t] = Place{rel(t.batch), at, t.cost}
			at += cost(t, t.cost)
		}
	}
	if a := r.active; a != nil {
		if r.turnStart.IsZero() && !r.loadStart.IsZero() {
			at += max(0, load(a)-now.Sub(r.loadStart))
		}
		for _, t := range a.running {
			at += cost(t, t.remaining(now))
		}
		if !a.turnEnded && !r.yielding {
			turn(a, r.cur)
		}
	}
	for _, m := range r.order {
		if m == r.active || m.turnDone || !m.hasWaiting(r.cur) {
			continue
		}
		if m != prev {
			at += load(m)
		}
		turn(m, r.cur)
		prev = m
	}
	last := 0
	for _, m := range r.models {
		last = max(last, m.lastBatch())
	}
	for k := r.cur + 1; k <= last; k++ {
		for _, m := range r.batchOrder(k, prev) {
			if m != prev {
				at += load(m)
			}
			turn(m, k)
			prev = m
		}
	}
	return res
}
