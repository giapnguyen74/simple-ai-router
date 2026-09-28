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
//	<jobs.dir>/.artifacts/                        files jobs refer to (package artifacts)
//
// A request may refer to the output of another job, or to an uploaded
// artifact, instead of carrying the file (refs.go). A job that refers to a
// job not done yet is blocked until it is.
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
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/artifacts"
	"github.com/giapnguyen74/simple-ai-router/internal/config"
	"github.com/giapnguyen74/simple-ai-router/internal/router"
)

type Status string

const (
	Blocked   Status = "blocked" // waits for the jobs it refers to
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
	resolvedName    = "request.resolved"
	maxReplySize    = 1 << 20 // a workload's JSON answer, or a validation reply
	validateTimeout = 10 * time.Second
	progressTimeout = 2 * time.Second
	sweepInterval   = time.Hour

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
	// Refs are the references in the request; DependsOn the jobs among them.
	Refs      []string `json:"refs,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
	// Sha256 is the output's id in the artifact store.
	Sha256 string `json:"sha256,omitempty"`

	pending   map[string]bool // jobs it waits for
	cancel    context.CancelFunc
	cancelled bool // DELETE was called
	ticket    *router.Ticket
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
	Batch          *int            `json:"batch,omitempty"`
	StartsIn       *int            `json:"starts_in_s,omitempty"`
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
	DependsOn      []string        `json:"depends_on,omitempty"`
	Sha256         *string         `json:"sha256,omitempty"`
}

// Error is a refusal by the router, with the HTTP status to answer.
type Error struct {
	Status int
	Detail string
	// StartsIn is the predicted wait of a job refused as busy.
	StartsIn *int
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

	store *artifacts.Store

	mu    sync.Mutex
	jobs  map[string]*Job
	order []*Job // oldest first
	// dependents are the blocked jobs waiting for a job, by its id.
	dependents map[string][]*Job
	closed     bool

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
		stop:   make(chan struct{}),
		store:  artifacts.New(cfg.Jobs.Dir, int64(cfg.Artifacts.MaxSize), cfg.Artifacts.TTL),

		dependents: make(map[string][]*Job),
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

	bodyPath := filepath.Join(dir, routerDir, bodyName)
	size, err := storeBody(bodyPath, req.Body, limit)
	if err != nil {
		return nil, err
	}
	j.BodySize = size
	if j.Refs, err = findRefs(bodyPath, j.ContentType); err != nil {
		return nil, errorf(http.StatusBadRequest, "%v", err)
	}
	m.mu.Lock()
	pending, rerr := m.checkRefsLocked(j)
	m.mu.Unlock()
	if rerr != nil {
		return nil, rerr
	}

	var estimate time.Duration
	if len(pending) == 0 {
		// Everything it refers to exists: the workload can check the
		// request as it will receive it.
		path, size, err := m.resolve(j)
		if err != nil {
			return nil, errorf(http.StatusUnprocessableEntity, "%v", err)
		}
		if estimate, err = m.validate(mc, j, path, size); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errorf(http.StatusServiceUnavailable, "router is shutting down")
	}
	// Dependencies may have finished meanwhile.
	if pending, rerr = m.checkRefsLocked(j); rerr != nil {
		return nil, rerr
	}
	var after []*router.Ticket
	for _, d := range pending {
		after = append(after, d.ticket)
	}
	t, err := m.rt.Enqueue(mc.Name, router.KindJob, router.Work{Path: j.Path, Estimate: estimate, After: after})
	if err != nil {
		var busy *router.BusyError
		switch {
		case errors.As(err, &busy):
			e := errorf(http.StatusTooManyRequests, "%s", busy.Error())
			e.StartsIn = seconds(busy.StartsIn.Seconds())
			return nil, e
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
	m.waitForLocked(j, pending)
	m.startLocked(j, t)
	return m.viewLocked(j), nil
}

// checkRefsLocked checks the job's references and returns the jobs it has to
// wait for.
func (m *Manager) checkRefsLocked(j *Job) ([]*Job, error) {
	var pending []*Job
	j.DependsOn = nil
	seen := map[string]bool{}
	for _, ref := range j.Refs {
		kind, id, _ := parseRef(ref)
		if kind == refArtifact {
			if !m.store.Has(id) {
				return nil, errorf(http.StatusUnprocessableEntity, "unknown artifact %s", id)
			}
			continue
		}
		d, ok := m.jobs[id]
		if !ok {
			return nil, errorf(http.StatusUnprocessableEntity, "unknown job %s", id)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		j.DependsOn = append(j.DependsOn, id)
		switch d.Status {
		case Done:
		case Failed, Cancelled:
			return nil, errorf(http.StatusUnprocessableEntity, "job %s is %s", id, d.Status)
		default:
			pending = append(pending, d)
		}
	}
	return pending, nil
}

// waitForLocked blocks the job until the pending jobs are done.
func (m *Manager) waitForLocked(j *Job, pending []*Job) {
	if len(pending) == 0 {
		return
	}
	j.Status = Blocked
	j.pending = make(map[string]bool, len(pending))
	for _, d := range pending {
		j.pending[d.ID] = true
		m.dependents[d.ID] = append(m.dependents[d.ID], j)
	}
	m.saveLogged(j)
}

// settleLocked tells the jobs waiting for j that it finished: they are
// queued once everything they wait for is done, and fail when j did not
// succeed.
func (m *Manager) settleLocked(j *Job) {
	waiting := m.dependents[j.ID]
	delete(m.dependents, j.ID)
	for _, d := range waiting {
		if d.Status.Terminal() {
			continue
		}
		if j.Status != Done {
			d.Status, d.Finished = Failed, now()
			d.Error = fmt.Sprintf("dependency %s is %s", j.ID, j.Status)
			m.cleanFiles(d)
			m.saveLogged(d)
			d.cancel()
			d.doneOnce.Do(func() { close(d.done) })
			m.settleLocked(d)
			continue
		}
		delete(d.pending, j.ID)
		if len(d.pending) == 0 && d.Status == Blocked {
			d.Status = Queued
			m.saveLogged(d)
			d.ticket.Unblock()
		}
	}
}

// resolve returns the body to send to the workload: the stored one, or for a
// request with references a copy with the files put in.
func (m *Manager) resolve(j *Job) (string, int64, error) {
	dir := filepath.Join(m.jobDir(j), routerDir)
	if len(j.Refs) == 0 {
		return filepath.Join(dir, bodyName), j.BodySize, nil
	}
	dst := filepath.Join(dir, resolvedName)
	n, err := rewrite(filepath.Join(dir, bodyName), dst, j.ContentType, m.lookup, int64(m.cfg.Jobs.MaxInlineRef))
	if err != nil {
		return "", 0, fmt.Errorf("resolve references: %v", err)
	}
	return dst, n, nil
}

// lookup finds the file a reference points to.
func (m *Manager) lookup(ref string) (file, error) {
	kind, id, err := parseRef(ref)
	if err != nil {
		return file{}, err
	}
	if kind == refJob {
		m.mu.Lock()
		d, ok := m.jobs[id]
		var status Status
		var path, name, sha string
		if ok {
			status, name, sha = d.Status, d.File, d.Sha256
			path = filepath.Join(m.jobDir(d), d.File)
		}
		m.mu.Unlock()
		if !ok || status != Done {
			return file{}, fmt.Errorf("job %s has no output (%s)", id, status)
		}
		if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
			return file{path, name, contentTypeOf(name), info.Size()}, nil
		}
		if sha == "" {
			return file{}, fmt.Errorf("the output of job %s is gone", id)
		}
		id = sha
	}
	path, meta, err := m.store.Open(id)
	if err != nil {
		return file{}, fmt.Errorf("artifact %s: %v", id, err)
	}
	ctype := meta.ContentType
	if ctype == "" {
		ctype = contentTypeOf(meta.Name)
	}
	return file{path, meta.Name, ctype, meta.Size}, nil
}

// contentTypeOf guesses a content type from a file name.
func contentTypeOf(name string) string {
	if t := mime.TypeByExtension(filepath.Ext(name)); t != "" {
		return t
	}
	return "application/octet-stream"
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

// validate asks a loaded model whether it accepts the request, and returns
// the workload's own estimate of its run time (eta_s in the answer) when it
// gives one. It never starts or swaps a model: when the model is not ready,
// or does not answer in time, the job is queued unchecked.
func (m *Manager) validate(mc *config.ModelConfig, j *Job, bodyPath string, bodySize int64) (time.Duration, error) {
	if mc.Validate == nil {
		return 0, nil
	}
	proxy, ready := m.rt.Ready(mc.Name)
	if !ready {
		return 0, nil
	}
	body, err := os.Open(bodyPath)
	if err != nil {
		return 0, nil
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
		return 0, nil
	}
	req.ContentLength = bodySize
	if j.ContentType != "" {
		req.Header.Set("Content-Type", j.ContentType)
	}
	req.Header.Set("X-Job-Path", "/"+j.Path)
	resp, err := m.client.Do(req)
	if err != nil {
		slog.Warn("validation skipped", "model", mc.Name, "err", err)
		return 0, nil
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, maxReplySize))
	if err != nil {
		slog.Warn("validation skipped", "model", mc.Name, "err", err)
		return 0, nil
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var a struct {
			ETA float64 `json:"eta_s"`
		}
		if json.Unmarshal(reply, &a) == nil && a.ETA > 0 && !math.IsInf(a.ETA, 0) {
			return time.Duration(a.ETA * float64(time.Second)), nil
		}
		return 0, nil
	}
	return 0, &UpstreamReply{Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"), Body: reply}
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
	j.ticket = t
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
	defer func() {
		m.mu.Lock()
		failed := j.Status != Done
		m.mu.Unlock()
		if failed {
			t.NoSample() // a failed run says nothing about the next one
		}
	}()

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
	var status int
	var reply []byte
	bodyPath, bodySize, err := m.resolve(j)
	if err == nil {
		status, reply, err = m.call(runCtx, p.Config().Proxy, j, bodyPath, bodySize)
	} else {
		stopPoll()
		poll.Wait()
		m.finish(j, Failed, func() { j.Error = err.Error() })
		return
	}
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
	// The output joins the artifact store, so a job that refers to it still
	// finds it after this job's folder is gone.
	var sha string
	if meta, err := m.store.Link(filepath.Join(m.jobDir(j), file), file, contentTypeOf(file)); err != nil {
		slog.Warn("store job output", "job", j.ID, "err", err)
	} else {
		sha, _ = artifacts.ParseID(meta.ID)
	}
	m.finish(j, Done, func() {
		j.UpstreamStatus = status
		j.File, j.Result, j.Sha256 = file, result, sha
	})
}

// call replays the stored request to the workload.
func (m *Manager) call(ctx context.Context, proxy string, j *Job, bodyPath string, bodySize int64) (int, []byte, error) {
	dir := m.jobDir(j)
	body, err := os.Open(bodyPath)
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
	req.ContentLength = bodySize
	if bodySize == 0 {
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
	m.cleanFiles(j)
	m.saveLogged(j)
	m.settleLocked(j)
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
	os.Remove(filepath.Join(dir, routerDir, resolvedName))
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
	m.settleLocked(j)
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
	v.DependsOn = j.DependsOn
	if j.Sha256 != "" {
		v.Sha256 = &j.Sha256
	}
	switch j.Status {
	case Queued, Blocked:
		pos := 0
		for _, o := range m.order {
			if o == j {
				break
			}
			if o.Model == j.Model && (o.Status == Queued || o.Status == Blocked) {
				pos++
			}
		}
		v.Position = &pos
		if j.ticket != nil {
			if p, ok := j.ticket.Place(); ok {
				v.Batch = &p.Batch
				v.StartsIn = seconds(p.StartsIn.Seconds())
				v.ETA = seconds((p.StartsIn + p.Cost).Seconds())
			}
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

// remainingLocked estimates the run time a running job still needs: the
// workload's own eta_s from progress, else the average of the recent runs of
// the same model and path, minus the time elapsed.
func (m *Manager) remainingLocked(j *Job) (float64, bool) {
	if eta, ok := j.Progress["eta_s"].(float64); ok && eta >= 0 {
		return eta, true
	}
	avg, ok := m.rt.Average(j.Model, j.Path)
	if !ok {
		return 0, false
	}
	return max(1, avg.Seconds()-(now()-j.Started)), true
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
		case Queued, Blocked:
			if _, err := os.Stat(filepath.Join(m.jobDir(j), routerDir, bodyName)); err != nil {
				j.Status, j.Finished, j.Error = Failed, now(), "stored request is missing"
				break
			}
			// The jobs it depends on were created before it: loaded already.
			pending, err := m.checkRefsLocked(j)
			if err != nil {
				j.Status, j.Finished, j.Error = Failed, now(), err.Error()
				break
			}
			var after []*router.Ticket
			for _, d := range pending {
				after = append(after, d.ticket)
			}
			t, err := m.rt.Requeue(j.Model, router.KindJob, router.Work{Path: j.Path, After: after})
			if err != nil {
				j.Status, j.Finished, j.Error = Failed, now(), err.Error()
				break
			}
			j.Status = Queued
			m.waitForLocked(j, pending)
			m.startLocked(j, t)
			requeued++
			continue
		case Done:
			if j.Started > 0 && j.Finished > 0 {
				m.rt.Seed(j.Model, j.Path, time.Duration((j.Finished-j.Started)*float64(time.Second)))
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
		removed += m.dropLocked(old)
		known := make(map[string]bool)
		for _, j := range m.order {
			if j.Model == name {
				known[j.ID] = true
			}
		}
		m.mu.Unlock()

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
	m.mu.Lock()
	_, shas := m.pinnedLocked()
	m.mu.Unlock()
	m.store.Sweep(at, func(sha string) bool { return shas[sha] })
}

// pinnedLocked is what jobs not finished yet refer to: those jobs, and the
// artifacts (their outputs' included). Retention keeps them.
func (m *Manager) pinnedLocked() (jobs, shas map[string]bool) {
	jobs, shas = map[string]bool{}, map[string]bool{}
	for _, j := range m.order {
		if j.Status.Terminal() {
			continue
		}
		for _, ref := range j.Refs {
			kind, id, _ := parseRef(ref)
			if kind == refArtifact {
				shas[id] = true
				continue
			}
			jobs[id] = true
			if d := m.jobs[id]; d != nil && d.Sha256 != "" {
				shas[d.Sha256] = true
			}
		}
	}
	return jobs, shas
}

// Artifacts is the store of files jobs refer to.
func (m *Manager) Artifacts() *artifacts.Store { return m.store }

// DeleteArtifact removes an artifact no queued or blocked job refers to.
func (m *Manager) DeleteArtifact(id string) error {
	m.mu.Lock()
	_, shas := m.pinnedLocked()
	m.mu.Unlock()
	err := m.store.Delete(id, func(sha string) bool { return shas[sha] })
	switch {
	case err == nil:
		return nil
	case errors.Is(err, artifacts.ErrNotFound):
		return errorf(http.StatusNotFound, "no artifact %s", id)
	}
	return errorf(http.StatusConflict, "%v", err)
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

// dropLocked forgets the jobs and deletes their folders, except the jobs a
// job not finished yet refers to.
func (m *Manager) dropLocked(jobs []*Job) int {
	if len(jobs) == 0 {
		return 0
	}
	pinned, _ := m.pinnedLocked()
	unpinned := jobs[:0:0]
	for _, j := range jobs {
		if !pinned[j.ID] {
			unpinned = append(unpinned, j)
		}
	}
	jobs = unpinned
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
	return len(jobs)
}
