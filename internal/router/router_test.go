package router

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
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
