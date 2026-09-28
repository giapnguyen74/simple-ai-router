package router

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/fakeserver"
	"github.com/giapnguyen74/simple-ai-router/internal/process"
)

func TestMain(m *testing.M) {
	fakeserver.MaybeRun()
	os.Exit(m.Run())
}

func newRouter(t *testing.T, globals string, models map[string]fakeserver.Model) *Router {
	t.Helper()
	r := New(fakeserver.Config(t, globals, models), nil)
	t.Cleanup(r.Shutdown)
	return r
}

func acquire(t *testing.T, r *Router, name string) (*process.Process, func()) {
	t.Helper()
	p, release, err := r.Acquire(context.Background(), name)
	if err != nil {
		t.Fatalf("acquire %s: %v", name, err)
	}
	return p, release
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func TestConcurrentColdStartLaunchesOnce(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Env: []string{"FAKE_START_DELAY=500ms"}},
	})

	const n = 20
	pids := make(chan int, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			p, release, err := r.Acquire(context.Background(), "a")
			if err != nil {
				t.Error(err)
				return
			}
			pids <- p.Info().PID
			release()
		})
	}
	wg.Wait()
	close(pids)

	first := 0
	for pid := range pids {
		if first == 0 {
			first = pid
		}
		if pid != first {
			t.Fatalf("model launched more than once: pids %d and %d", first, pid)
		}
	}
}

func TestAliasAndUnknownModel(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Extra: "aliases: [gpt-4o-mini]"},
	})
	p, release := acquire(t, r, "gpt-4o-mini")
	release()
	if p.Name() != "a" {
		t.Fatalf("alias resolved to %q", p.Name())
	}
	if _, _, err := r.Acquire(context.Background(), "nope"); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("got %v, want ErrUnknownModel", err)
	}
}

func TestSwapStopsOtherModel(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {}, "b": {}})

	a, release := acquire(t, r, "a")
	pidA := a.Info().PID
	release()

	_, release = acquire(t, r, "b")
	release()

	if s := a.State(); s != process.StateStopped {
		t.Fatalf("a state = %s, want stopped", s)
	}
	if alive(pidA) {
		t.Fatalf("a (pid %d) still alive after swap", pidA)
	}
}

func TestSwapWaitsForInflight(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {}, "b": {}})

	_, releaseA := acquire(t, r, "a")
	got := make(chan struct{})
	go func() {
		_, release := acquire(t, r, "b")
		release()
		close(got)
	}()

	select {
	case <-got:
		t.Fatal("swapped to b while a had a request in flight")
	case <-time.After(300 * time.Millisecond):
	}
	releaseA()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("b never became ready after a drained")
	}
}

func TestStartFailureReturnsQuickly(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Env: []string{"FAKE_EXIT_AT_START=1"}},
	})
	start := time.Now()
	_, _, err := r.Acquire(context.Background(), "a")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "exiting at start") {
		t.Fatalf("error does not include process logs: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %s to report failure", d)
	}
}

func TestStopFallsBackToSIGKILL(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Env: []string{"FAKE_IGNORE_TERM=1"}},
	})
	p, release := acquire(t, r, "a")
	release()
	pid := p.Info().PID

	start := time.Now()
	if err := r.Unload("a"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < time.Second {
		t.Fatalf("stopped after %s; SIGTERM should have been ignored", d)
	}
	if alive(pid) {
		t.Fatalf("pid %d still alive", pid)
	}
}

func TestTTLUnloadsIdleModel(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Extra: "ttl: 200ms"},
	})
	p, release := acquire(t, r, "a")

	time.Sleep(300 * time.Millisecond)
	r.unloadIdle()
	if p.State() != process.StateReady {
		t.Fatal("unloaded a model with a request in flight")
	}

	release()
	time.Sleep(300 * time.Millisecond)
	r.unloadIdle()
	if s := p.State(); s != process.StateStopped {
		t.Fatalf("state = %s, want stopped", s)
	}
}

func TestRestartAfterCrash(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {}})
	p, release := acquire(t, r, "a")
	release()
	pid := p.Info().PID
	syscall.Kill(pid, syscall.SIGKILL)

	deadline := time.Now().Add(2 * time.Second)
	for p.State() != process.StateStopped {
		if time.Now().After(deadline) {
			t.Fatal("crash not detected")
		}
		time.Sleep(20 * time.Millisecond)
	}

	p, release = acquire(t, r, "a")
	release()
	if p.Info().PID == pid {
		t.Fatal("expected a new process")
	}
}

func TestShutdownKillsEverything(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {}})
	p, release := acquire(t, r, "a")
	release()
	pid := p.Info().PID

	r.Shutdown()
	if alive(pid) {
		t.Fatalf("pid %d still alive after shutdown", pid)
	}
}

const busyModel = `busyCheck:
  endpoint: /state
  fields: [running, queued]
  interval: 50ms
  grace: 300ms`

// startWork kicks off a background job on a ready model, as a job server's
// "submit" call would: the request returns before the work is done.
func startWork(t *testing.T, r *Router, name string, d time.Duration) {
	t.Helper()
	p, release := acquire(t, r, name)
	defer release()
	resp, err := http.Get(p.Config().Proxy + "/work?for=" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestSwapWaitsForBackgroundWork(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {Extra: busyModel}, "b": {}})
	startWork(t, r, "a", time.Second)

	swapped := make(chan time.Time)
	go func() {
		_, release := acquire(t, r, "b")
		release()
		swapped <- time.Now()
	}()

	// While a drains, requests for a must still go through, so clients can
	// poll job status.
	time.Sleep(200 * time.Millisecond)
	pollStart := time.Now()
	_, release := acquire(t, r, "a")
	release()
	if d := time.Since(pollStart); d > 100*time.Millisecond {
		t.Fatalf("request to draining model blocked for %s", d)
	}

	select {
	case at := <-swapped:
		if at.Sub(pollStart) < 500*time.Millisecond {
			t.Fatal("swapped before background work finished")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("swap never happened")
	}
}

func TestDrainWaitsForClientsToFetchResults(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {Extra: busyModel}, "b": {}})
	startWork(t, r, "a", 200*time.Millisecond)

	swapped := make(chan struct{})
	go func() {
		_, release := acquire(t, r, "b")
		release()
		close(swapped)
	}()

	// Keep polling a after its work is done, as a client fetching results
	// would; the grace period restarts with each request.
	for range 5 {
		time.Sleep(150 * time.Millisecond)
		select {
		case <-swapped:
			t.Fatal("swapped while the client was still fetching results")
		default:
		}
		_, release := acquire(t, r, "a")
		release()
	}
	select {
	case <-swapped:
	case <-time.After(3 * time.Second):
		t.Fatal("swap never happened after the client went quiet")
	}
}

func TestDrainTimeoutStopsBusyModel(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Extra: busyModel + "\ndrainTimeout: 300ms"},
		"b": {},
	})
	startWork(t, r, "a", time.Minute)

	start := time.Now()
	_, release := acquire(t, r, "b")
	release()
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("swap took %s despite 300ms drainTimeout", d)
	}
}

func TestTTLKeepsBusyModel(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Extra: busyModel + "\nttl: 100ms"},
	})
	p, release := acquire(t, r, "a")
	release()
	startWork(t, r, "a", 600*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	r.unloadIdle()
	if p.State() != process.StateReady {
		t.Fatal("TTL unloaded a busy model")
	}

	time.Sleep(600 * time.Millisecond) // work done; Touch pushed lastUsed forward
	r.unloadIdle()
	if s := p.State(); s != process.StateStopped {
		t.Fatalf("state = %s, want stopped", s)
	}
}

// --- batch scheduler ---

const fastShare = "batch:\n  cycle: 600ms\n  linger: 50ms\n"

// pump keeps one request after another going to a model until stop is
// closed, and counts them.
func pump(r *Router, name string, hold time.Duration, stop <-chan struct{}, count *atomic.Int64) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, release, err := r.Acquire(ctx, name)
		cancel()
		if err != nil {
			continue
		}
		time.Sleep(hold)
		release()
		count.Add(1)
	}
}

func TestAloneModelIsNeverSwapped(t *testing.T) {
	r := newRouter(t, fastShare, map[string]fakeserver.Model{"a": {}, "b": {}})
	p, release := acquire(t, r, "a")
	release()
	pid := p.Info().PID
	stop := make(chan struct{})
	var n atomic.Int64
	go pump(r, "a", 10*time.Millisecond, stop, &n)
	time.Sleep(1500 * time.Millisecond) // several periods
	close(stop)
	if p.Info().PID != pid || p.State() != process.StateReady {
		t.Fatalf("a was restarted with no other model waiting (pid %d -> %d)", pid, p.Info().PID)
	}
}

func TestModelsWithWorkTakeTurns(t *testing.T) {
	r := newRouter(t, fastShare, map[string]fakeserver.Model{
		"a": {Extra: "share: 2"},
		"b": {Extra: "share: 1"},
	})
	stop := make(chan struct{})
	var na, nb atomic.Int64
	go pump(r, "a", 10*time.Millisecond, stop, &na)
	go pump(r, "b", 10*time.Millisecond, stop, &nb)
	time.Sleep(4 * time.Second)
	close(stop)
	time.Sleep(100 * time.Millisecond)
	a, b := na.Load(), nb.Load()
	t.Logf("served a=%d b=%d", a, b)
	if a == 0 || b == 0 {
		t.Fatalf("a model starved: a=%d b=%d", a, b)
	}
}

func TestSharesSplitABatch(t *testing.T) {
	r := newRouter(t, "batch:\n  cycle: 3s\n", map[string]fakeserver.Model{
		"a": {Extra: planned + "\nshare: 2"}, "b": {Extra: planned}, "c": {Extra: planned},
	})
	_, release := acquire(t, r, "c")
	defer release()
	for range 40 {
		enqueueJob(t, r, "a", 100*time.Millisecond)
	}
	for range 40 {
		enqueueJob(t, r, "b", 100*time.Millisecond)
	}
	var next BatchInfo
	for _, b := range r.Schedule().Batches {
		if b.Batch == 2 {
			next = b
		}
	}
	work := map[string]float64{}
	for _, p := range next.Models {
		work[p.Model] = p.WorkS
	}
	t.Logf("batch 2: %+v", next.Models)
	if a, b := work["a"], work["b"]; b == 0 || a/b < 1.6 || a/b > 2.5 || a+b > 3.05 {
		t.Fatalf("batch 2 work a=%.1fs b=%.1fs, want about 2:1 within the 3s cycle", a, b)
	}
}

func TestLoadedModelGoesFirstInTheNextBatch(t *testing.T) {
	r := newRouter(t, fastShare, map[string]fakeserver.Model{"a": {}, "b": {}})
	p, releaseA := acquire(t, r, "a")
	pid := p.Info().PID

	var order []string
	var pidA2 int
	var mu sync.Mutex
	var wg sync.WaitGroup
	serve := func(name string) {
		p, release := acquire(t, r, name)
		mu.Lock()
		order = append(order, name)
		if name == "a" {
			pidA2 = p.Info().PID
		}
		mu.Unlock()
		release()
	}
	wg.Go(func() { serve("b") }) // booked in batch 2
	time.Sleep(200 * time.Millisecond)
	// The running batch has no room for another minute-long guess: batch 2.
	wg.Go(func() { serve("a") })
	time.Sleep(200 * time.Millisecond)
	releaseA()
	wg.Wait()
	if len(order) != 2 || order[0] != "a" {
		t.Fatalf("order %v: a is loaded and booked in batch 2, so it goes first", order)
	}
	if pidA2 != pid {
		t.Fatal("a was reloaded between its turns")
	}
}

func TestIdleModelYieldsAtOnceOrAfterLinger(t *testing.T) {
	for _, linger := range []time.Duration{0, 700 * time.Millisecond} {
		// A long period, so the slice does not end before linger does.
		r := newRouter(t, "batch:\n  cycle: 10s\n", map[string]fakeserver.Model{
			"a": {Extra: "linger: " + linger.String()},
			"b": {},
		})
		_, release := acquire(t, r, "a")
		release()
		start := time.Now()
		_, release = acquire(t, r, "b")
		release()
		took := time.Since(start)
		if linger == 0 && took > 600*time.Millisecond {
			t.Fatalf("idle model took %s to yield", took)
		}
		if linger > 0 && took < linger {
			t.Fatalf("yielded after %s despite linger %s", took, linger)
		}
		r.Shutdown()
	}
}

func TestNextModelIsTheOneWaitingLongest(t *testing.T) {
	r := newRouter(t, fastShare, map[string]fakeserver.Model{"a": {}, "b": {}, "c": {}})
	_, releaseA := acquire(t, r, "a")

	got := make(chan string, 2)
	var wg sync.WaitGroup
	for _, name := range []string{"c", "b"} {
		wg.Go(func() {
			_, release := acquire(t, r, name)
			got <- name
			release()
		})
		time.Sleep(150 * time.Millisecond)
	}
	releaseA()
	wg.Wait()
	if first := <-got; first != "c" {
		t.Fatalf("%s served first; c had waited longer", first)
	}
}

func TestConcurrencyLimitsAdmission(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {Extra: "concurrency: 1"}})
	var holding, maxHolding atomic.Int64
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			_, release := acquire(t, r, "a")
			if n := holding.Add(1); n > maxHolding.Load() {
				maxHolding.Store(n)
			}
			time.Sleep(50 * time.Millisecond)
			holding.Add(-1)
			release()
		})
	}
	wg.Wait()
	if maxHolding.Load() != 1 {
		t.Fatalf("%d requests admitted at once with concurrency 1", maxHolding.Load())
	}
}

func TestCancelledRequestLeavesQueue(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {Extra: "concurrency: 1"}})
	_, release := acquire(t, r, "a")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, _, err := r.Acquire(ctx, "a")
		errc <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if q := r.Infos()[0].Queued; q != 1 {
		t.Fatalf("queued = %d, want 1", q)
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if q := r.Infos()[0].Queued; q != 0 {
		t.Fatalf("queued = %d after cancel", q)
	}
	release()
}

func TestMaxQueueAndQueueTimeout(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{
		"a": {Extra: "concurrency: 1\nmaxQueue: 1\nqueueTimeout: 300ms"},
	})
	_, release := acquire(t, r, "a")
	defer release()

	errc := make(chan error, 1)
	go func() {
		_, _, err := r.Acquire(context.Background(), "a")
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, _, err := r.Acquire(context.Background(), "a"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third request: %v, want ErrQueueFull", err)
	}
	start := time.Now()
	if err := <-errc; !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("second request: %v, want ErrQueueTimeout", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("queue timeout took %s", d)
	}
}

func TestUnloadDoesNotBlockBehindAnotherModelsDrain(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {}, "b": {}})
	_, release := acquire(t, r, "a")
	defer release()
	start := time.Now()
	if err := r.Unload("b"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("unloading an idle model took %s", d)
	}
}

func TestUnloadKeepsTicketsAndReloads(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {Extra: "concurrency: 1"}})
	p, release := acquire(t, r, "a")
	pid := p.Info().PID
	got := make(chan struct{})
	go func() {
		_, release := acquire(t, r, "a")
		release()
		close(got)
	}()
	time.Sleep(100 * time.Millisecond)
	unloaded := make(chan struct{})
	go func() {
		r.Unload("a")
		close(unloaded)
	}()
	time.Sleep(100 * time.Millisecond)
	release()
	<-unloaded
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("queued request never ran after the unload")
	}
	if p.Info().PID == pid {
		t.Fatal("model was not reloaded for the queued request")
	}
}

func TestInfosReportSchedule(t *testing.T) {
	r := newRouter(t, fastShare, map[string]fakeserver.Model{"a": {}, "b": {}})
	_, release := acquire(t, r, "a")
	var wg sync.WaitGroup
	wg.Go(func() {
		_, rel, err := r.Acquire(context.Background(), "b")
		if err == nil {
			rel()
		}
	})
	time.Sleep(200 * time.Millisecond)
	var a, b Info
	for _, i := range r.Infos() {
		switch i.Name {
		case "a":
			a = i
		case "b":
			b = i
		}
	}
	release()
	wg.Wait()
	if !a.Active || a.Admitted != 1 || a.Turn != "running" || a.QuotaS == nil {
		t.Fatalf("a: %+v", a)
	}
	if b.Active || b.Queued != 1 {
		t.Fatalf("b: %+v", b)
	}
}

// planned models: loads are quick and a guess is short, so batches are
// planned from the estimates the tests give.
const planned = "loadTime: 1ms\ndefaultEta: 100ms"

func enqueueJob(t *testing.T, r *Router, name string, est time.Duration) *Ticket {
	t.Helper()
	tk, err := r.Enqueue(name, KindJob, Work{Path: "run", Estimate: est})
	if err != nil {
		t.Fatalf("enqueue %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		tk.Wait(ctx)
	})
	return tk
}

func batchOf(t *testing.T, tk *Ticket) int {
	t.Helper()
	p, ok := tk.Place()
	if !ok {
		return 0 // admitted
	}
	return p.Batch
}

func TestNewcomerTakesItsPartAndFloodMovesBack(t *testing.T) {
	r := newRouter(t, "batch:\n  cycle: 1s\n", map[string]fakeserver.Model{
		"a": {Extra: planned}, "b": {Extra: planned}, "c": {Extra: planned},
	})
	_, release := acquire(t, r, "a")
	defer release()
	b := enqueueJob(t, r, "b", 200*time.Millisecond)
	var as []*Ticket
	for range 12 {
		as = append(as, enqueueJob(t, r, "a", 200*time.Millisecond))
	}
	count := func() map[int]int {
		n := map[int]int{}
		for _, tk := range as {
			n[batchOf(t, tk)]++
		}
		return n
	}
	before := count()
	if batchOf(t, b) != 2 || before[2] == 0 {
		t.Fatalf("b in batch %d, a per batch %v", batchOf(t, b), before)
	}
	for i := 1; i < len(as); i++ {
		if bi, bj := batchOf(t, as[i-1]), batchOf(t, as[i]); bi > bj && bj != 0 {
			t.Fatalf("a's tickets out of arrival order: %d then %d", bi, bj)
		}
	}
	c := enqueueJob(t, r, "c", 200*time.Millisecond)
	after := count()
	t.Logf("a per batch before c %v, after %v", before, after)
	if batchOf(t, c) != 2 || batchOf(t, b) != 2 {
		t.Fatalf("c in batch %d, b in batch %d; both belong in batch 2", batchOf(t, c), batchOf(t, b))
	}
	if after[2] >= before[2] {
		t.Fatalf("a kept %d tickets in batch 2 (had %d): the flood did not move back", after[2], before[2])
	}
}

func TestJoinUsesOnlyUnbookedTime(t *testing.T) {
	r := newRouter(t, "batch:\n  cycle: 1s\n", map[string]fakeserver.Model{
		"a": {Extra: planned}, "b": {Extra: planned},
	})
	_, release := acquire(t, r, "a")
	defer release()
	b := enqueueJob(t, r, "b", 200*time.Millisecond)
	first := enqueueJob(t, r, "a", 100*time.Millisecond)  // admitted: jobs run one at a time
	joined := enqueueJob(t, r, "a", 100*time.Millisecond) // waits in a's running turn
	if got := batchOf(t, joined); got != 1 {
		t.Fatalf("a's job in batch %d; the running batch had room", got)
	}
	big := enqueueJob(t, r, "a", 2*time.Second)
	if got := batchOf(t, big); got < 2 {
		t.Fatalf("a job longer than the cycle joined the running batch")
	}
	if batchOf(t, b) != 2 {
		t.Fatalf("b moved to batch %d", batchOf(t, b))
	}
	_ = first
}

func TestTicketLongerThanItsPartBooksAlone(t *testing.T) {
	r := newRouter(t, "batch:\n  cycle: 1s\n", map[string]fakeserver.Model{
		"a": {Extra: planned}, "b": {Extra: planned},
	})
	_, release := acquire(t, r, "a")
	defer release()
	b1 := enqueueJob(t, r, "b", 5*time.Second)
	b2 := enqueueJob(t, r, "b", 100*time.Millisecond)
	if batchOf(t, b1) != 2 || batchOf(t, b2) != 3 {
		t.Fatalf("b's long job in batch %d, the next in %d; want 2 and 3", batchOf(t, b1), batchOf(t, b2))
	}
}

func TestSyncRequestIsBusyPastSyncMaxWait(t *testing.T) {
	r := newRouter(t, "batch:\n  cycle: 1s\n", map[string]fakeserver.Model{
		"a": {Extra: planned}, "b": {Extra: planned + "\nsyncMaxWait: 200ms\nmaxWait: 200ms"},
	})
	_, release := acquire(t, r, "a")
	defer release()
	enqueueJob(t, r, "a", 10*time.Second)
	_, _, err := r.Acquire(context.Background(), "b")
	var busy *BusyError
	if !errors.As(err, &busy) || busy.StartsIn < 5*time.Second {
		t.Fatalf("sync request: %v, want busy with the wait", err)
	}
	if _, err := r.Enqueue("b", KindJob, Work{Path: "run"}); !errors.Is(err, ErrBusy) {
		t.Fatalf("job: %v, want busy past maxWait", err)
	}
	if q := r.Infos()[1].Queued; q != 0 {
		t.Fatalf("a refused ticket stayed queued (%d)", q)
	}
}

func TestLearnsDurationsAndLoadTime(t *testing.T) {
	r := newRouter(t, "", map[string]fakeserver.Model{"a": {}})
	for range 3 {
		_, release, err := r.AcquirePath(context.Background(), "a", "x")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		release()
	}
	s := r.Schedule()
	p, ok := s.Paths["a /x"]
	if !ok || p.Runs != 3 || p.EstimateS < 0.04 || p.EstimateS > 0.5 {
		t.Fatalf("learned %+v", s.Paths)
	}
	if s.GPU.Switches != 1 || s.GPU.LoadS <= 0 {
		t.Fatalf("gpu %+v", s.GPU)
	}
	if l := r.Infos()[0].LoadS; l <= 0 || l > 5 {
		t.Fatalf("load time %v", l)
	}
}
