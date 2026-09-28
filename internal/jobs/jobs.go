// Package jobs gives every model an asynchronous job API on top of a
// synchronous workload. A job is a stored request: the router queues it,
// replays it to the model when its turn comes, and serves the file the
// workload wrote into the job's folder.
//
// Layout on disk, which agents on the same machine may read directly:
//
//	<jobs.dir>/<model>/<job id>/<file>           the workload's output
//	<jobs.dir>/<model>/<job id>/.router/job.json the router's record
//	<jobs.dir>/<model>/<job id>/.router/request.body  until the job ends
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
	"github.com/giapnguyen74/simple-ai-router/internal/router"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Done      Status = "done"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

func (s Status) Terminal() bool { return s == Done || s == Failed || s == Cancelled }

const (
	routerDir       = ".router"
	recordName      = "job.json"
	bodyName        = "request.body"
	maxReplySize    = 1 << 20 // a workload's JSON answer, or a validation reply
	validateTimeout = 10 * time.Second
	progressTimeout = 2 * time.Second
	sweepInterval   = time.Hour
	historySize     = 10

	// ErrRestarted is the error of a job that was running when the router
	// stopped.
	ErrRestarted = "router restarted while the job was running"
)

var idRE = regexp.MustCompile(`^[0-9a-f]{12}$`)

// Job is one stored request and what became of it. The exported fields are
// the record kept in job.json.
type Job struct {
	ID             string          `json:"id"`
	Model          string          `json:"model"`
	Method         string          `json:"method"`
	Path           string          `json:"path"`
	Query          string          `json:"query,omitempty"`
	ContentType    string          `json:"content_type,omitempty"`
	BodySize       int64           `json:"body_size"`
	Status         Status          `json:"status"`
	Created        float64         `json:"created"`
	Started        float64         `json:"started,omitempty"`
	Finished       float64         `json:"finished,omitempty"`
	Progress       map[string]any  `json:"progress,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	File           string          `json:"file,omitempty"`
	Error          string          `json:"error,omitempty"`
	UpstreamStatus int             `json:"upstream_status,omitempty"`

	cancel    context.CancelFunc
	cancelled bool // DELETE was called
	done      chan struct{}
	doneOnce  sync.Once
}

// View is what the job routes return.
type View struct {
	ID             string          `json:"id"`
	Model          string          `json:"model"`
	Path           string          `json:"path"`
	Status         Status          `json:"status"`
	Position       *int            `json:"position"`
	ETA            *int            `json:"eta_s"`
	Created        float64         `json:"created"`
	Started        *float64        `json:"started"`
	Finished       *float64        `json:"finished"`
	Elapsed        *float64        `json:"elapsed_s"`
	Progress       map[string]any  `json:"progress"`
	Result         json.RawMessage `json:"result"`
	File           *string         `json:"file"`
	Error          *string         `json:"error"`
	UpstreamStatus *int            `json:"upstream_status"`
}

// Error is a refusal by the router, with the HTTP status to answer.
type Error struct {
	Status int
	Detail string
}

func (e *Error) Error() string { return e.Detail }

func errorf(status int, format string, args ...any) *Error {
	return &Error{Status: status, Detail: fmt.Sprintf(format, args...)}
}

// UpstreamReply is a workload's own refusal of a request (from its validate
// endpoint), passed on to the client as it is.
type UpstreamReply struct {
	Status      int
	ContentType string
	Body        []byte
}

func (u *UpstreamReply) Error() string { return fmt.Sprintf("workload answered %d", u.Status) }

// Request is a job submission.
type Request struct {
	Model         string
	Method        string
	Path          string // without the leading slash
	Query         string
	ContentType   string
	ContentLength int64 // -1 when unknown
	Body          io.Reader
}

type Manager struct {
	rt  *router.Router
	cfg *config.Config
	dir string
	// client runs jobs: no timeout of its own.
	client *http.Client

	mu     sync.Mutex
	jobs   map[string]*Job
	order  []*Job               // oldest first
	recent map[string][]float64 // durations of finished jobs, per model and path
	closed bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// New loads the jobs found on disk and starts the retention sweep. Jobs that
// were queued are queued again; one that was running is marked failed.
func New(rt *router.Router) (*Manager, error) {
	cfg := rt.Config()
	m := &Manager{
		rt:     rt,
		cfg:    cfg,
		dir:    cfg.Jobs.Dir,
		client: &http.Client{Transport: &http.Transport{DisableCompression: true}},
		jobs:   make(map[string]*Job),
		recent: make(map[string][]float64),
		stop:   make(chan struct{}),
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	m.Sweep(time.Now())
	m.wg.Go(func() {
		t := time.NewTicker(sweepInterval)
		defer t.Stop()
		for {
			select {
			case <-m.stop:
				return
			case now := <-t.C:
				m.Sweep(now)
			}
		}
	})
	return m, nil
}

// Close stops running jobs without changing their records: queued jobs stay
// queued for the next start, and a running one is marked failed then.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for _, j := range m.jobs {
		if j.cancel != nil {
			j.cancel()
		}
	}
	m.mu.Unlock()
	close(m.stop)
	m.wg.Wait()
}

func now() float64 { return float64(time.Now().UnixMicro()) / 1e6 }

func newID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func (m *Manager) jobDir(j *Job) string { return filepath.Join(m.dir, j.Model, j.ID) }

// Dir is the folder the job's workload writes into.
func (m *Manager) Dir(j *Job) string { return m.jobDir(j) }

// Submit stores the request and queues it.
func (m *Manager) Submit(req Request) (*View, error) {
	mc, ok := m.cfg.Resolve(req.Model)
	if !ok {
		return nil, errorf(http.StatusNotFound, "model not found: %s", req.Model)
	}
	limit := int64(mc.MaxBodySize)
	if m.rt.QueueFull(mc.Name) {
		return nil, errorf(http.StatusTooManyRequests, "queue full (%d waiting for %s)", mc.MaxQueue, mc.Name)
	}
	if req.ContentLength > limit {
		return nil, errorf(http.StatusRequestEntityTooLarge, "request body exceeds %d bytes", limit)
	}

	j := &Job{ID: newID(), Model: mc.Name, Method: req.Method, Path: req.Path, Query: req.Query,
		ContentType: req.ContentType, Status: Queued, Created: now(), done: make(chan struct{})}
	dir := m.jobDir(j)
	if err := os.MkdirAll(filepath.Join(dir, routerDir), 0o755); err != nil {
		return nil, errorf(http.StatusInternalServerError, "create job folder: %v", err)
	}
	keep := false
	defer func() {
		if !keep {
			os.RemoveAll(dir)
		}
	}()

	size, err := storeBody(filepath.Join(dir, routerDir, bodyName), req.Body, limit)
	if err != nil {
		return nil, err
	}
	j.BodySize = size

	if err := m.validate(mc, j); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errorf(http.StatusServiceUnavailable, "router is shutting down")
	}
	t, err := m.rt.Enqueue(mc.Name, router.KindJob)
	if err != nil {
		switch {
		case errors.Is(err, router.ErrQueueFull):
			return nil, errorf(http.StatusTooManyRequests, "queue full (%d waiting for %s)", mc.MaxQueue, mc.Name)
		case errors.Is(err, router.ErrShutdown):
			return nil, errorf(http.StatusServiceUnavailable, "router is shutting down")
		}
		return nil, errorf(http.StatusInternalServerError, "%v", err)
	}
	if err := m.save(j); err != nil {
		// The ticket is dropped by a Wait on a cancelled context.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		t.Wait(ctx)
		return nil, errorf(http.StatusInternalServerError, "store job: %v", err)
	}
	keep = true
	m.startLocked(j, t)
	return m.viewLocked(j), nil
}

// storeBody streams the body to a file and refuses one over the limit.
func storeBody(path string, body io.Reader, limit int64) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, errorf(http.StatusInternalServerError, "store request: %v", err)
	}
	defer f.Close()
	if body == nil {
		return 0, nil
	}
	n, err := io.Copy(f, io.LimitReader(body, limit+1))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return 0, errorf(http.StatusRequestEntityTooLarge, "request body exceeds %d bytes", limit)
		}
		return 0, errorf(http.StatusBadRequest, "read request body: %v", err)
	}
	if n > limit {
		return 0, errorf(http.StatusRequestEntityTooLarge, "request body exceeds %d bytes", limit)
	}
	return n, f.Close()
}

// validate asks a loaded model whether it accepts the request. It never
// starts or swaps a model: when the model is not ready, or does not answer
// in time, the job is queued unchecked.
func (m *Manager) validate(mc *config.ModelConfig, j *Job) error {
	if mc.Validate == nil {
		return nil
	}
	proxy, ready := m.rt.Ready(mc.Name)
	if !ready {
		return nil
	}
	body, err := os.Open(filepath.Join(m.jobDir(j), routerDir, bodyName))
	if err != nil {
		return nil
	}
	defer body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), validateTimeout)
	defer cancel()
	url := proxy + mc.Validate.Endpoint
	if j.Query != "" {
		url += "?" + j.Query
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil
	}
	req.ContentLength = j.BodySize
	if j.ContentType != "" {
		req.Header.Set("Content-Type", j.ContentType)
	}
	req.Header.Set("X-Job-Path", "/"+j.Path)
	resp, err := m.client.Do(req)
	if err != nil {
		slog.Warn("validation skipped", "model", mc.Name, "err", err)
		return nil
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, maxReplySize))
	if err != nil {
		slog.Warn("validation skipped", "model", mc.Name, "err", err)
		return nil
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &UpstreamReply{Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"), Body: reply}
}

// save writes the job's record atomically. Callers hold m.mu or own the job.
func (m *Manager) save(j *Job) error {
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Join(m.jobDir(j), routerDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, recordName+".*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(append(data, '\n'))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), filepath.Join(dir, recordName))
	}
	if err != nil {
		os.Remove(tmp.Name())
	}
	return err
}

func (m *Manager) saveLogged(j *Job) {
	if err := m.save(j); err != nil {
		slog.Error("store job", "job", j.ID, "err", err)
	}
}

// startLocked registers the job and starts the goroutine that runs it.
func (m *Manager) startLocked(j *Job, t *router.Ticket) {
	ctx, cancel := context.WithCancel(context.Background())
	j.cancel = cancel
	m.jobs[j.ID] = j
	m.order = append(m.order, j)
	m.wg.Go(func() {
		defer cancel()
		m.run(ctx, j, t)
	})
}

func (m *Manager) run(ctx context.Context, j *Job, t *router.Ticket) {
	mc := m.cfg.Models[j.Model]
	p, release, err := t.Wait(ctx)
	if err != nil {
		m.finish(j, Failed, func() { j.Error = err.Error() })
		return
	}
	defer release()

	m.mu.Lock()
	if m.closed || j.Status.Terminal() {
		m.mu.Unlock()
		m.finish(j, Cancelled, nil)
		return
	}
	j.Status, j.Started = Running, now()
	m.saveLogged(j)
	m.mu.Unlock()

	runCtx := ctx
	if mc.JobTimeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, mc.JobTimeout)
		defer cancel()
	}
	pollCtx, stopPoll := context.WithCancel(ctx)
	var poll sync.WaitGroup
	if mc.Progress != nil {
		poll.Go(func() { m.pollProgress(pollCtx, mc, j) })
	}
	status, reply, err := m.call(runCtx, p.Config().Proxy, j)
	stopPoll()
	poll.Wait()

	if err != nil {
		timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()
		if !closed && (timedOut || ctx.Err() != nil) {
			// The request was closed under the workload. Give it time to
			// notice, so the next job does not meet a busy model.
			m.waitIdle(mc)
		}
		m.finish(j, Failed, func() {
			if timedOut {
				j.Error = fmt.Sprintf("job timed out after %s", mc.JobTimeout)
			} else {
				j.Error = "upstream error: " + err.Error()
			}
		})
		return
	}
	if status < 200 || status > 299 {
		m.finish(j, Failed, func() {
			j.UpstreamStatus = status
			j.Error = detailOf(reply)
		})
		return
	}
	file, result, err := m.checkReply(j, reply)
	if err != nil {
		m.finish(j, Failed, func() {
			j.UpstreamStatus = status
			j.Error = err.Error()
		})
		return
	}
	m.finish(j, Done, func() {
		j.UpstreamStatus = status
		j.File, j.Result = file, result
	})
}

// call replays the stored request to the workload.
func (m *Manager) call(ctx context.Context, proxy string, j *Job) (int, []byte, error) {
	dir := m.jobDir(j)
	body, err := os.Open(filepath.Join(dir, routerDir, bodyName))
	if err != nil {
		return 0, nil, err
	}
	defer body.Close()
	url := proxy + "/" + j.Path
	if j.Query != "" {
		url += "?" + j.Query
	}
	req, err := http.NewRequestWithContext(ctx, j.Method, url, body)
	if err != nil {
		return 0, nil, err
	}
	req.ContentLength = j.BodySize
	if j.BodySize == 0 {
		req.Body = http.NoBody
	}
	if j.ContentType != "" {
		req.Header.Set("Content-Type", j.ContentType)
	}
	req.Header.Set("X-Job-Id", j.ID)
	req.Header.Set("X-Job-Dir", dir)
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, maxReplySize))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, reply, nil
}

// checkReply reads the workload's {"file", "result"} answer and makes sure
// the file is a plain name of a regular file in the job's folder.
func (m *Manager) checkReply(j *Job, reply []byte) (string, json.RawMessage, error) {
	var a struct {
		File   *string         `json:"file"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(reply, &a); err != nil {
		return "", nil, fmt.Errorf("workload answer is not JSON: %v", err)
	}
	if a.File == nil || *a.File == "" {
		return "", nil, errors.New(`workload answer has no "file"`)
	}
	name := *a.File
	if err := checkName(name); err != nil {
		return "", nil, err
	}
	info, err := os.Lstat(filepath.Join(m.jobDir(j), name))
	if err != nil {
		return "", nil, fmt.Errorf("workload named file %q, which is not in the job folder", name)
	}
	if !info.Mode().IsRegular() {
		return "", nil, fmt.Errorf("workload named file %q, which is not a regular file", name)
	}
	if string(a.Result) == "null" {
		a.Result = nil
	}
	return name, a.Result, nil
}

func checkName(name string) error {
	if strings.ContainsAny(name, `/\`) || strings.HasPrefix(name, ".") || strings.Contains(name, "..") ||
		strings.ContainsRune(name, 0) || name != filepath.Base(name) {
		return fmt.Errorf("workload named file %q; it must be a plain file name", name)
	}
	return nil
}

// detailOf extracts a FastAPI-style {"detail": ...} from an error answer.
func detailOf(reply []byte) string {
	var a struct {
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal(reply, &a); err == nil && len(a.Detail) > 0 && string(a.Detail) != "null" {
		var s string
		if json.Unmarshal(a.Detail, &s) == nil {
			return s
		}
		return string(a.Detail)
	}
	text := strings.TrimSpace(string(reply))
	if text == "" {
		return "workload answered with an error and no detail"
	}
	return text
}

func (m *Manager) pollProgress(ctx context.Context, mc *config.ModelConfig, j *Job) {
	t := time.NewTicker(mc.Progress.Interval)
	defer t.Stop()
	for {
		if p, ok := m.progress(ctx, mc); ok {
			id, _ := p["id"].(string)
			busy, hasBusy := p["busy"].(bool)
			if (id == "" || id == j.ID) && (!hasBusy || busy) {
				delete(p, "id")
				delete(p, "busy")
				m.mu.Lock()
				j.Progress = p
				m.mu.Unlock()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Manager) progress(ctx context.Context, mc *config.ModelConfig) (map[string]any, bool) {
	ctx, cancel := context.WithTimeout(ctx, progressTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mc.Proxy+mc.Progress.Endpoint, nil)
	if err != nil {
		return nil, false
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var p map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReplySize)).Decode(&p); err != nil || p == nil {
		return nil, false
	}
	return p, true
}

// waitIdle waits until the workload reports that it stopped working, for up
// to its stopTimeout. A workload that keeps going is stopped, so it cannot
// run the abandoned job next to the following one. Without a progress
// endpoint there is nothing to ask and the slot is given back at once.
func (m *Manager) waitIdle(mc *config.ModelConfig) {
	if mc.Progress == nil {
		return
	}
	deadline := time.Now().Add(mc.StopTimeout)
	for {
		p, ok := m.progress(context.Background(), mc)
		if !ok {
			return // stopped or crashed: nothing is running
		}
		if busy, _ := p["busy"].(bool); !busy {
			return
		}
		if time.Now().After(deadline) {
			slog.Warn("workload still busy after its request was closed", "model", mc.Name)
			m.rt.ForceStop(mc.Name)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// finish ends a job. A job that DELETE already marked cancelled stays
// cancelled. set fills in the outcome and runs with m.mu held.
func (m *Manager) finish(j *Job, status Status, set func()) {
	m.mu.Lock()
	if m.closed && !j.Status.Terminal() {
		// Shutting down: the record on disk keeps its state for the restart.
		m.mu.Unlock()
		return
	}
	if !j.Status.Terminal() {
		if j.cancelled {
			status, set = Cancelled, nil
		}
		j.Status, j.Finished = status, now()
		if set != nil {
			set()
		}
	}
	final := j.Status
	if final == Done && j.Started > 0 {
		key := historyKey(j.Model, j.Path)
		m.recent[key] = append(m.recent[key], j.Finished-j.Started)
		if n := len(m.recent[key]); n > historySize {
			m.recent[key] = m.recent[key][n-historySize:]
		}
	}
	m.cleanFiles(j)
	m.saveLogged(j)
	m.mu.Unlock()

	j.doneOnce.Do(func() { close(j.done) })
	slog.Info("job finished", "job", j.ID, "model", j.Model, "status", final)
	m.prune(j.Model)
}

// cleanFiles removes the stored request, and for a job that did not succeed
// everything the workload left behind.
func (m *Manager) cleanFiles(j *Job) {
	dir := m.jobDir(j)
	os.Remove(filepath.Join(dir, routerDir, bodyName))
	if j.Status == Done {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.Name() != routerDir {
			os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

func historyKey(model, path string) string { return model + "\x00" + path }

// Cancel cancels a queued or running job.
func (m *Manager) Cancel(id string) (*View, error) {
	m.mu.Lock()
	j, ok := m.jobs[id]
	if !ok {
		m.mu.Unlock()
		return nil, errorf(http.StatusNotFound, "no job %s", id)
	}
	if j.Status.Terminal() {
		m.mu.Unlock()
		return nil, errorf(http.StatusConflict, "job is already %s", j.Status)
	}
	j.cancelled = true
	j.Status, j.Finished = Cancelled, now()
	m.saveLogged(j)
	j.cancel()
	v := m.viewLocked(j)
	m.mu.Unlock()
	j.doneOnce.Do(func() { close(j.done) })
	return v, nil
}

// Get returns a job's view.
func (m *Manager) Get(id string) (*View, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, errorf(http.StatusNotFound, "no job %s", id)
	}
	return m.viewLocked(j), nil
}

// Wait returns the job's view once it is finished, or the current view when
// the timeout passes or ctx ends.
func (m *Manager) Wait(ctx context.Context, id string, timeout time.Duration) (*View, error) {
	m.mu.Lock()
	j, ok := m.jobs[id]
	m.mu.Unlock()
	if !ok {
		return nil, errorf(http.StatusNotFound, "no job %s", id)
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-j.done:
	case <-t.C:
	case <-ctx.Done():
	case <-m.stop:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.viewLocked(j), nil
}

// List returns every job, newest first, of one model when model is not empty.
func (m *Manager) List(model string) ([]*View, error) {
	if model != "" {
		mc, ok := m.cfg.Resolve(model)
		if !ok {
			return nil, errorf(http.StatusNotFound, "model not found: %s", model)
		}
		model = mc.Name
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	views := make([]*View, 0, len(m.order))
	for i := len(m.order) - 1; i >= 0; i-- {
		if j := m.order[i]; model == "" || j.Model == model {
			views = append(views, m.viewLocked(j))
		}
	}
	return views, nil
}

// Result returns the path and name of a finished job's output file.
func (m *Manager) Result(id string) (path, name string, err error) {
	m.mu.Lock()
	j, ok := m.jobs[id]
	var status Status
	if ok {
		status, name, path = j.Status, j.File, filepath.Join(m.jobDir(j), j.File)
	}
	m.mu.Unlock()
	if !ok {
		return "", "", errorf(http.StatusNotFound, "no job %s", id)
	}
	if status != Done {
		return "", "", errorf(http.StatusConflict, "job is %s", status)
	}
	if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() {
		return "", "", errorf(http.StatusNotFound, "the output of job %s is gone", id)
	}
	return path, name, nil
}

// Counts returns the number of jobs per model and status.
func (m *Manager) Counts() map[string]map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]map[string]int)
	for _, j := range m.order {
		if counts[j.Model] == nil {
			counts[j.Model] = make(map[string]int)
		}
		counts[j.Model][string(j.Status)]++
	}
	return counts
}

func (m *Manager) viewLocked(j *Job) *View {
	v := &View{ID: j.ID, Model: j.Model, Path: "/" + j.Path, Status: j.Status, Created: j.Created,
		Progress: j.Progress, Result: j.Result}
	if j.Started > 0 {
		v.Started = &j.Started
		end := j.Finished
		if end == 0 {
			end = now()
		}
		elapsed := math.Round((end-j.Started)*1000) / 1000
		v.Elapsed = &elapsed
	}
	if j.Finished > 0 {
		v.Finished = &j.Finished
	}
	if j.File != "" {
		v.File = &j.File
	}
	if j.Error != "" {
		v.Error = &j.Error
	}
	if j.UpstreamStatus != 0 {
		v.UpstreamStatus = &j.UpstreamStatus
	}
	switch j.Status {
	case Queued:
		pos := 0
		eta, known := 0.0, true
		for _, o := range m.order {
			if o.Model != j.Model || o.Status.Terminal() {
				continue
			}
			if o == j {
				break
			}
			if o.Status == Queued {
				pos++
			}
			left, ok := m.remainingLocked(o)
			eta, known = eta+left, known && ok
		}
		v.Position = &pos
		if own, ok := m.remainingLocked(j); ok && known {
			v.ETA = seconds(eta + own)
		}
	case Running:
		if left, ok := m.remainingLocked(j); ok {
			v.ETA = seconds(left)
		}
	}
	return v
}

func seconds(s float64) *int {
	n := int(math.Round(s))
	return &n
}

// remainingLocked estimates the run time a job still needs: from the
// workload's own eta_s while it runs, otherwise from the recent jobs with the
// same model and path.
func (m *Manager) remainingLocked(j *Job) (float64, bool) {
	if j.Status == Running {
		if eta, ok := j.Progress["eta_s"].(float64); ok && eta >= 0 {
			return eta, true
		}
	}
	recent := m.recent[historyKey(j.Model, j.Path)]
	if len(recent) == 0 {
		return 0, false
	}
	avg := 0.0
	for _, d := range recent {
		avg += d
	}
	avg /= float64(len(recent))
	if j.Status == Running {
		return max(1, avg-(now()-j.Started)), true
	}
	return avg, true
}

// load reads the jobs of every configured model from disk.
func (m *Manager) load() error {
	var found []*Job
	for name := range m.cfg.Models {
		entries, err := os.ReadDir(filepath.Join(m.dir, name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("jobs: %w", err)
		}
		for _, e := range entries {
			if !e.IsDir() || !idRE.MatchString(e.Name()) {
				continue
			}
			data, err := os.ReadFile(filepath.Join(m.dir, name, e.Name(), routerDir, recordName))
			if err != nil {
				continue // no record: left to the sweep
			}
			j := &Job{}
			if err := json.Unmarshal(data, j); err != nil || j.ID != e.Name() || j.Model != name {
				slog.Warn("ignoring unreadable job record", "model", name, "job", e.Name())
				continue
			}
			j.done = make(chan struct{})
			found = append(found, j)
		}
	}
	sort.SliceStable(found, func(a, b int) bool { return found[a].Created < found[b].Created })

	m.mu.Lock()
	defer m.mu.Unlock()
	requeued := 0
	for _, j := range found {
		switch j.Status {
		case Running:
			j.Status, j.Finished, j.Error = Failed, now(), ErrRestarted
		case Queued:
			if _, err := os.Stat(filepath.Join(m.jobDir(j), routerDir, bodyName)); err != nil {
				j.Status, j.Finished, j.Error = Failed, now(), "stored request is missing"
				break
			}
			t, err := m.rt.Requeue(j.Model, router.KindJob)
			if err != nil {
				j.Status, j.Finished, j.Error = Failed, now(), err.Error()
				break
			}
			m.startLocked(j, t)
			requeued++
			continue
		case Done:
			if j.Started > 0 && j.Finished > 0 {
				key := historyKey(j.Model, j.Path)
				m.recent[key] = append(m.recent[key], j.Finished-j.Started)
				if n := len(m.recent[key]); n > historySize {
					m.recent[key] = m.recent[key][n-historySize:]
				}
			}
		}
		if j.Status != Done {
			m.cleanFiles(j)
			m.saveLogged(j)
		}
		close(j.done)
		j.doneOnce.Do(func() {})
		m.jobs[j.ID] = j
		m.order = append(m.order, j)
	}
	if len(found) > 0 {
		slog.Info("jobs loaded", "dir", m.dir, "jobs", len(found), "requeued", requeued)
	}
	return nil
}

// Sweep deletes finished jobs older than their model's resultTTL, and
// folders named like a job id that belong to no job and are that old.
func (m *Manager) Sweep(at time.Time) {
	removed := 0
	for name, mc := range m.cfg.Models {
		cutoff := float64(at.Add(-mc.ResultTTL).UnixMicro()) / 1e6
		m.mu.Lock()
		var old []*Job
		for _, j := range m.order {
			if j.Model == name && j.Status.Terminal() && j.Finished < cutoff {
				old = append(old, j)
			}
		}
		m.dropLocked(old)
		known := make(map[string]bool)
		for _, j := range m.order {
			if j.Model == name {
				known[j.ID] = true
			}
		}
		m.mu.Unlock()
		removed += len(old)

		entries, err := os.ReadDir(filepath.Join(m.dir, name))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || !idRE.MatchString(e.Name()) || known[e.Name()] {
				continue
			}
			info, err := e.Info()
			if err != nil || !info.ModTime().Before(at.Add(-mc.ResultTTL)) {
				continue
			}
			if os.RemoveAll(filepath.Join(m.dir, name, e.Name())) == nil {
				removed++
			}
		}
	}
	if removed > 0 {
		slog.Info("retention: removed old jobs", "jobs", removed)
	}
}

// prune keeps only the model's newest keepJobs finished jobs.
func (m *Manager) prune(model string) {
	keep := m.cfg.Models[model].KeepJobs
	if keep <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var finished []*Job
	for _, j := range m.order {
		if j.Model == model && j.Status.Terminal() {
			finished = append(finished, j)
		}
	}
	if len(finished) <= keep {
		return
	}
	sort.SliceStable(finished, func(a, b int) bool { return finished[a].Finished > finished[b].Finished })
	m.dropLocked(finished[keep:])
}

// dropLocked forgets the jobs and deletes their folders.
func (m *Manager) dropLocked(jobs []*Job) {
	if len(jobs) == 0 {
		return
	}
	drop := make(map[*Job]bool, len(jobs))
	for _, j := range jobs {
		drop[j] = true
		delete(m.jobs, j.ID)
		if err := os.RemoveAll(m.jobDir(j)); err != nil {
			slog.Warn("retention: remove job folder", "job", j.ID, "err", err)
		}
	}
	kept := m.order[:0]
	for _, j := range m.order {
		if !drop[j] {
			kept = append(kept, j)
		}
	}
	for i := len(kept); i < len(m.order); i++ {
		m.order[i] = nil
	}
	m.order = kept
}
