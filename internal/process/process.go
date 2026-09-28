// Package process manages the lifecycle of one inference server child process.
package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
)

type State string

const (
	StateStopped  State = "stopped"
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateStopping State = "stopping"
	StateFailed   State = "failed"
)

const (
	logBufferSize   = 64 << 10
	logTailLines    = 20
	healthInterval  = 250 * time.Millisecond
	healthReqTimout = 2 * time.Second
)

var ErrStopped = errors.New("process stopped")

// Process is one managed inference server. Start and Stop must be serialized
// by the caller (the router does this); in-flight tracking is safe for
// concurrent use.
type Process struct {
	cfg           *config.ModelConfig
	healthTimeout time.Duration
	logs          *LogBuffer

	mu       sync.Mutex
	state    State
	cmd      *exec.Cmd
	ready    chan struct{} // closed when the current start attempt finishes
	startErr error
	exited   chan struct{} // closed when the current cmd has been reaped
	started  time.Time
	lastUsed time.Time
	inflight int
	idle     chan struct{} // closed when inflight drops to zero
}

func New(cfg *config.ModelConfig, healthTimeout time.Duration, logOut io.Writer) *Process {
	var tee io.Writer
	if logOut != nil {
		tee = newPrefixWriter(logOut, "["+cfg.Name+"] ")
	}
	return &Process{
		cfg:           cfg,
		healthTimeout: healthTimeout,
		logs:          NewLogBuffer(logBufferSize, tee),
		state:         StateStopped,
	}
}

func (p *Process) Name() string                { return p.cfg.Name }
func (p *Process) Config() *config.ModelConfig { return p.cfg }
func (p *Process) Logs() *LogBuffer            { return p.logs }

func (p *Process) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// Begin launches the process unless it is already starting or ready, and
// returns a channel that is closed once the start attempt finishes. Call
// StartErr afterwards to learn the outcome.
func (p *Process) Begin() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.state {
	case StateStarting, StateReady:
		return p.ready
	}
	// A previous child (crashed or killed after a failed start) may not be
	// reaped yet; wait so the new one does not fight it for the port.
	if p.exited != nil {
		exited := p.exited
		p.mu.Unlock()
		<-exited
		p.mu.Lock()
	}
	p.ready = make(chan struct{})
	p.startErr = nil
	if err := p.launchLocked(); err != nil {
		p.state = StateFailed
		p.startErr = err
		close(p.ready)
	}
	return p.ready
}

// StartErr returns the result of the most recent start attempt.
func (p *Process) StartErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.startErr
}

func (p *Process) launchLocked() error {
	args := p.cfg.Args
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(os.Environ(), p.cfg.Env...)
	cmd.Stdout = p.logs
	cmd.Stderr = p.logs
	setProcAttrs(cmd)

	slog.Info("starting model", "model", p.cfg.Name, "cmd", args)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", p.cfg.Name, err)
	}
	exited := make(chan struct{})
	p.cmd = cmd
	p.exited = exited
	p.state = StateStarting
	p.started = time.Now()

	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		if p.cmd == cmd && p.state == StateReady {
			slog.Warn("model exited unexpectedly", "model", p.cfg.Name, "err", err)
			p.state = StateStopped
		}
		p.mu.Unlock()
		close(exited)
	}()
	go p.waitHealthy(cmd, exited, p.ready)
	return nil
}

func (p *Process) waitHealthy(cmd *exec.Cmd, exited <-chan struct{}, ready chan struct{}) {
	err := p.pollHealth(exited)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != cmd {
		return
	}
	if err == nil && p.state == StateStarting {
		p.state = StateReady
		p.lastUsed = time.Now()
		slog.Info("model ready", "model", p.cfg.Name, "took", time.Since(p.started).Round(time.Millisecond))
	} else {
		if err == nil {
			err = ErrStopped
		}
		if tail := p.logs.Tail(logTailLines); tail != "" {
			err = fmt.Errorf("%w\n--- last log lines ---\n%s", err, tail)
		}
		p.startErr = err
		if p.state == StateStarting {
			p.state = StateFailed
			slog.Error("model failed to start", "model", p.cfg.Name, "err", err)
			// Make sure a hung process does not linger.
			killGroup(cmd)
		}
	}
	close(ready)
}

func (p *Process) pollHealth(exited <-chan struct{}) error {
	if p.cfg.CheckEndpoint == "none" {
		return nil
	}
	url := p.cfg.Proxy + p.cfg.CheckEndpoint
	client := &http.Client{Timeout: healthReqTimout}
	deadline := time.NewTimer(p.healthTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(healthInterval)
	defer tick.Stop()
	for {
		resp, err := client.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-exited:
			return fmt.Errorf("%s exited during startup", p.cfg.Name)
		case <-deadline.C:
			return fmt.Errorf("%s not healthy after %s", p.cfg.Name, p.healthTimeout)
		case <-tick.C:
		}
	}
}

// Stop drains the process (up to drainTimeout), then terminates the process
// group: SIGTERM first, SIGKILL after the model's stopTimeout.
func (p *Process) Stop(drainTimeout time.Duration) {
	p.Drain(drainTimeout)

	p.mu.Lock()
	cmd, exited := p.cmd, p.exited
	if cmd == nil {
		p.state = StateStopped
		p.mu.Unlock()
		return
	}
	p.state = StateStopping
	p.mu.Unlock()

	select {
	case <-exited:
	default:
		slog.Info("stopping model", "model", p.cfg.Name)
		termGroup(cmd)
		select {
		case <-exited:
		case <-time.After(p.cfg.StopTimeout):
			slog.Warn("model ignored SIGTERM, killing", "model", p.cfg.Name)
			killGroup(cmd)
			<-exited
		}
	}

	p.mu.Lock()
	if p.cmd == cmd {
		p.cmd = nil
		p.state = StateStopped
	}
	p.mu.Unlock()
}

// Running reports whether the process has a live child.
func (p *Process) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

// Acquire marks a request as in flight.
func (p *Process) Acquire() {
	p.mu.Lock()
	p.inflight++
	if p.inflight == 1 {
		p.idle = make(chan struct{})
	}
	p.mu.Unlock()
}

// Release marks an in-flight request as done.
func (p *Process) Release() {
	p.mu.Lock()
	p.inflight--
	p.lastUsed = time.Now()
	if p.inflight == 0 {
		close(p.idle)
	}
	p.mu.Unlock()
}

// Drain blocks until the process is idle, or until the timeout elapses. Idle
// means no requests in flight and, with a busyCheck, the model reports no
// work and has had no requests for the check's grace period, so clients can
// fetch results of finished jobs. It returns false on timeout.
func (p *Process) Drain(timeout time.Duration) bool {
	if timeout <= 0 {
		return p.waitInflight(0)
	}
	deadline := time.Now().Add(timeout)
	b := p.cfg.BusyCheck
	for {
		if !p.waitInflight(time.Until(deadline)) {
			break
		}
		if b == nil {
			return true
		}
		wait := b.Interval
		if p.Busy() {
			p.Touch()
		} else if quiet := b.Grace - p.sinceLastUsed(); quiet > 0 {
			wait = min(wait, quiet)
		} else {
			return true
		}
		if time.Until(deadline) <= 0 {
			break
		}
		select {
		case <-time.After(min(wait, time.Until(deadline))):
		case <-p.exitedChan():
			return true
		}
	}
	slog.Warn("drain timeout, stopping while busy", "model", p.cfg.Name)
	return false
}

func (p *Process) sinceLastUsed() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Since(p.lastUsed)
}

// WaitInflight blocks until no request is in flight, or until the timeout
// elapses. It returns false on timeout.
func (p *Process) WaitInflight(timeout time.Duration) bool { return p.waitInflight(timeout) }

func (p *Process) waitInflight(timeout time.Duration) bool {
	p.mu.Lock()
	if p.inflight == 0 {
		p.mu.Unlock()
		return true
	}
	idle := p.idle
	p.mu.Unlock()
	if timeout <= 0 {
		return false
	}
	select {
	case <-idle:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (p *Process) exitedChan() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exited == nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	return p.exited
}

// Busy reports whether the model's busyCheck says it is still working.
// Models without a busyCheck, or that are not ready, are never busy.
func (p *Process) Busy() bool {
	b := p.cfg.BusyCheck
	if b == nil || p.State() != StateReady {
		return false
	}
	resp, err := busyClient.Get(p.cfg.Proxy + b.Endpoint)
	if err != nil {
		slog.Warn("busy check failed", "model", p.cfg.Name, "err", err)
		return false
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		slog.Warn("busy check: bad JSON", "model", p.cfg.Name, "err", err)
		return false
	}
	for _, f := range b.Fields {
		if truthy(body[f]) {
			return true
		}
	}
	return false
}

var busyClient = &http.Client{Timeout: 5 * time.Second}

func truthy(v any) bool {
	switch v := v.(type) {
	case nil:
		return false
	case bool:
		return v
	case float64:
		return v != 0
	case string:
		return v != ""
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	}
	return true
}

// Touch marks the process as just used, postponing its TTL unload.
func (p *Process) Touch() {
	p.mu.Lock()
	p.lastUsed = time.Now()
	p.mu.Unlock()
}

// IdleFor reports how long the process has been ready with nothing in flight.
func (p *Process) IdleFor() (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != StateReady || p.inflight > 0 {
		return 0, false
	}
	return time.Since(p.lastUsed), true
}

type Info struct {
	Name     string `json:"name"`
	State    State  `json:"state"`
	PID      int    `json:"pid,omitempty"`
	Proxy    string `json:"proxy"`
	Inflight int    `json:"inflight"`
	Uptime   string `json:"uptime,omitempty"`
}

func (p *Process) Info() Info {
	p.mu.Lock()
	defer p.mu.Unlock()
	info := Info{Name: p.cfg.Name, State: p.state, Proxy: p.cfg.Proxy, Inflight: p.inflight}
	if p.cmd != nil && p.cmd.Process != nil && p.state != StateStopped {
		info.PID = p.cmd.Process.Pid
		info.Uptime = time.Since(p.started).Round(time.Second).String()
	}
	return info
}

// WaitReady waits for a start attempt begun with Begin.
func WaitReady(ctx context.Context, ready <-chan struct{}) error {
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
