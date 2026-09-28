package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
	"github.com/giapnguyen74/simple-ai-router/internal/fakeserver"
	"github.com/giapnguyen74/simple-ai-router/internal/jobs"
	"github.com/giapnguyen74/simple-ai-router/internal/router"
)

type jobStack struct {
	ts  *httptest.Server
	rt  *router.Router
	jm  *jobs.Manager
	dir string
}

// newJobStack starts a router with a job manager. models is extra YAML per
// model; every model gets progress and validate endpoints unless the extra
// YAML says otherwise.
func newJobStack(t *testing.T, dir, globals string, models map[string]fakeserver.Model) *jobStack {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	globals = fmt.Sprintf("jobs:\n  dir: %q\n%s", dir, globals)
	cfg := fakeserver.Config(t, globals, models)
	rt := router.New(cfg, nil)
	jm, err := jobs.New(rt)
	if err != nil {
		rt.Shutdown()
		t.Fatal(err)
	}
	s, err := New(rt, jm)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	st := &jobStack{ts: ts, rt: rt, jm: jm, dir: dir}
	t.Cleanup(st.close)
	return st
}

func (st *jobStack) close() {
	st.ts.Close()
	st.jm.Close()
	st.rt.Shutdown()
}

const jobModel = `progress:
  endpoint: /progress
  interval: 50ms
validate:
  endpoint: /validate`

type view struct {
	ID             string         `json:"id"`
	Model          string         `json:"model"`
	Path           string         `json:"path"`
	Status         string         `json:"status"`
	Position       *int           `json:"position"`
	Batch          *int           `json:"batch"`
	StartsIn       *int           `json:"starts_in_s"`
	ETA            *int           `json:"eta_s"`
	Started        *float64       `json:"started"`
	Finished       *float64       `json:"finished"`
	Elapsed        *float64       `json:"elapsed_s"`
	Progress       map[string]any `json:"progress"`
	Result         map[string]any `json:"result"`
	File           *string        `json:"file"`
	Error          *string        `json:"error"`
	UpstreamStatus *int           `json:"upstream_status"`
}

func (st *jobStack) do(t *testing.T, method, path, ctype, body string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, st.ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func (st *jobStack) submit(t *testing.T, model, path, body string) view {
	t.Helper()
	code, _, b := st.do(t, "POST", "/jobs/"+model+"/"+path, "application/json", body)
	if code != http.StatusAccepted {
		t.Fatalf("submit %s: %d %s", path, code, b)
	}
	var v view
	json.Unmarshal(b, &v)
	if v.ID == "" || v.Status != "queued" || v.Position == nil {
		t.Fatalf("bad 202 body: %s", b)
	}
	return v
}

func (st *jobStack) wait(t *testing.T, id string) view {
	t.Helper()
	code, _, b := st.do(t, "GET", "/jobs/"+id+"/wait?timeout=20", "", "")
	if code != 200 {
		t.Fatalf("wait: %d %s", code, b)
	}
	var v view
	json.Unmarshal(b, &v)
	return v
}

func (st *jobStack) get(t *testing.T, id string) view {
	t.Helper()
	code, _, b := st.do(t, "GET", "/jobs/"+id, "", "")
	if code != 200 {
		t.Fatalf("get: %d %s", code, b)
	}
	var v view
	json.Unmarshal(b, &v)
	return v
}

func (st *jobStack) stats(t *testing.T, model string) fakeserver.Stats {
	t.Helper()
	code, _, b := st.do(t, "GET", "/u/"+model+"/stats", "", "")
	if code != 200 {
		t.Fatalf("stats: %d %s", code, b)
	}
	var s fakeserver.Stats
	json.Unmarshal(b, &s)
	return s
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestJobSubmitWaitResult(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})

	v := st.submit(t, "a", "job?for=300ms&steps=3", `{"prompt":"hi"}`)
	if *v.Position != 0 {
		t.Fatalf("position = %d", *v.Position)
	}
	done := st.wait(t, v.ID)
	if done.Status != "done" || str(done.File) != "out.txt" || done.Result["server"] != "a" ||
		done.Result["id"] != v.ID || done.Result["bytes"] != float64(15) ||
		done.Result["contentType"] != "application/json" || done.Result["query"] != "for=300ms&steps=3" ||
		done.Path != "/job" || done.Elapsed == nil || *done.Elapsed < 0.25 || done.UpstreamStatus == nil {
		t.Fatalf("done view: %+v", done)
	}
	if step, _ := done.Progress["step"].(float64); step < 1 || done.Progress["busy"] != nil || done.Progress["id"] != nil {
		t.Fatalf("progress not kept in the finished view: %v", done.Progress)
	}

	code, h, b := st.do(t, "GET", "/jobs/"+v.ID+"/result", "", "")
	if code != 200 || string(b) != `a:{"prompt":"hi"}` {
		t.Fatalf("result: %d %q", code, b)
	}
	if ct := h.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type %q", ct)
	}
	if cd := h.Get("Content-Disposition"); !strings.Contains(cd, `filename=out.txt`) {
		t.Fatalf("content disposition %q", cd)
	}

	// The layout agents on the same machine rely on.
	jobDir := filepath.Join(st.dir, "a", v.ID)
	if _, err := os.Stat(filepath.Join(jobDir, "out.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(jobDir, ".router", "job.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(jobDir, ".router", "request.body")); err == nil {
		t.Fatal("request.body kept after the job finished")
	}

	code, _, b = st.do(t, "GET", "/jobs?model=a", "", "")
	var list []view
	json.Unmarshal(b, &list)
	if code != 200 || len(list) != 1 || list[0].ID != v.ID {
		t.Fatalf("list: %d %s", code, b)
	}
	if code, _, b = st.do(t, "GET", "/jobs?model=zzz", "", ""); code != 404 || !strings.Contains(string(b), "detail") {
		t.Fatalf("list unknown model: %d %s", code, b)
	}
	if code, _, _ = st.do(t, "POST", "/jobs/zzz/job", "application/json", "{}"); code != 404 {
		t.Fatalf("submit unknown model: %d", code)
	}
	if code, _, _ = st.do(t, "GET", "/jobs/000000000000", "", ""); code != 404 {
		t.Fatalf("unknown job: %d", code)
	}
}

func TestJobResultBeforeDoneAndProgressWhileRunning(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	v := st.submit(t, "a", "job?for=1s&steps=10", "x")
	time.Sleep(500 * time.Millisecond)
	code, _, b := st.do(t, "GET", "/jobs/"+v.ID+"/result", "", "")
	if code != 409 || !strings.Contains(string(b), "running") {
		t.Fatalf("result while running: %d %s", code, b)
	}
	running := st.get(t, v.ID)
	step, _ := running.Progress["step"].(float64)
	if running.Status != "running" || step < 1 || running.Progress["phase"] != "sampling" || running.ETA == nil {
		t.Fatalf("running view: %+v", running)
	}
	// wait with a short timeout returns the current view.
	code, _, b = st.do(t, "GET", "/jobs/"+v.ID+"/wait?timeout=0.1", "", "")
	var w view
	json.Unmarshal(b, &w)
	if code != 200 || w.Status != "running" {
		t.Fatalf("short wait: %d %s", code, b)
	}
	if code, _, b = st.do(t, "GET", "/jobs/"+v.ID+"/wait?timeout=9999", "", ""); code != 422 {
		t.Fatalf("bad timeout: %d %s", code, b)
	}
	if d := st.wait(t, v.ID); d.Status != "done" {
		t.Fatalf("status %s", d.Status)
	}
}

func TestJobAcceptedWhileOtherModelActiveAndResultOutlivesModel(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}, "b": {}})

	// b holds the GPU with a request in flight.
	pb, releaseB, err := st.rt.Acquire(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	v := st.submit(t, "a", "job?for=100ms", "x")
	time.Sleep(200 * time.Millisecond)
	if cur := st.get(t, v.ID); cur.Status != "queued" {
		t.Fatalf("job ran while b held the GPU: %+v", cur)
	}
	releaseB()
	if d := st.wait(t, v.ID); d.Status != "done" {
		t.Fatalf("status %s: %s", d.Status, str(d.Error))
	}
	if pb.Running() {
		t.Fatal("b still running after the swap to a")
	}

	// Swap back to b: a is stopped, the result is still served.
	_, releaseB, err = st.rt.Acquire(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	releaseB()
	if code, _, b := st.do(t, "GET", "/jobs/"+v.ID+"/result", "", ""); code != 200 || string(b) != "a:x" {
		t.Fatalf("result after a was stopped: %d %q", code, b)
	}
}

func TestJobCancelQueuedAndRunning(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	running := st.submit(t, "a", "job?for=3s&steps=30", "x")
	queued := st.submit(t, "a", "job?for=100ms", "y")
	time.Sleep(300 * time.Millisecond)
	if v := st.get(t, queued.ID); v.Status != "queued" || *v.Position != 0 {
		t.Fatalf("second job: %+v", v)
	}

	code, _, b := st.do(t, "DELETE", "/jobs/"+queued.ID, "", "")
	var v view
	json.Unmarshal(b, &v)
	if code != 200 || v.Status != "cancelled" {
		t.Fatalf("cancel queued: %d %s", code, b)
	}
	if code, _, _ = st.do(t, "DELETE", "/jobs/"+queued.ID, "", ""); code != 409 {
		t.Fatalf("second cancel: %d", code)
	}

	code, _, b = st.do(t, "DELETE", "/jobs/"+running.ID, "", "")
	json.Unmarshal(b, &v)
	if code != 200 || v.Status != "cancelled" {
		t.Fatalf("cancel running: %d %s", code, b)
	}
	deadline := time.Now().Add(3 * time.Second)
	for st.stats(t, "a").Disconnects == 0 {
		if time.Now().After(deadline) {
			t.Fatal("workload never saw the disconnect")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The next job runs on the same, idle, model.
	next := st.submit(t, "a", "job?for=50ms", "z")
	if d := st.wait(t, next.ID); d.Status != "done" {
		t.Fatalf("job after a cancel: %s %s", d.Status, str(d.Error))
	}
	if s := st.stats(t, "a"); s.MaxConcurrent != 1 {
		t.Fatalf("workload ran %d jobs at once", s.MaxConcurrent)
	}
	entries, _ := os.ReadDir(filepath.Join(st.dir, "a", running.ID))
	for _, e := range entries {
		if e.Name() != ".router" {
			t.Fatalf("cancelled job kept %s", e.Name())
		}
	}
	if code, _, _ = st.do(t, "GET", "/jobs/"+running.ID+"/result", "", ""); code != 409 {
		t.Fatalf("result of a cancelled job: %d", code)
	}
}

func TestJobQueueFullAndBodyLimit(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{
		"a": {Extra: jobModel + "\nmaxQueue: 1\nmaxBodySize: 10"},
	})
	first := st.submit(t, "a", "job?for=2s", "x")
	st.submit(t, "a", "job?for=10ms", "y")
	code, h, b := st.do(t, "POST", "/jobs/a/job", "application/json", "z")
	if code != 429 || h.Get("Retry-After") == "" || !strings.Contains(string(b), "queue full") {
		t.Fatalf("third job: %d %s", code, b)
	}
	st.do(t, "DELETE", "/jobs/"+first.ID, "", "")

	if code, _, b = st.do(t, "POST", "/jobs/a/job", "text/plain", strings.Repeat("x", 11)); code != 413 {
		t.Fatalf("11 bytes: %d %s", code, b)
	}
	// Chunked: no Content-Length to check up front.
	pr, pw := io.Pipe()
	go func() {
		pw.Write(bytes.Repeat([]byte("x"), 11))
		pw.Close()
	}()
	req, _ := http.NewRequest("POST", st.ts.URL+"/jobs/a/job", pr)
	req.ContentLength = -1
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("chunked 11 bytes: %d", resp.StatusCode)
	}
	entries, _ := os.ReadDir(filepath.Join(st.dir, "a"))
	if len(entries) != 2 {
		t.Fatalf("refused jobs left folders: %d entries", len(entries))
	}
}

func TestJobValidateOnlyWhenReady(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	// Not loaded: the request is queued unchecked and the job runs.
	v := st.submit(t, "a", "job?for=10ms", "invalid")
	if d := st.wait(t, v.ID); d.Status != "done" {
		t.Fatalf("unchecked job: %s", d.Status)
	}
	if s := st.stats(t, "a"); s.Validates != 0 {
		t.Fatalf("validated %d times before the model was loaded", s.Validates)
	}
	// Loaded: the workload's refusal comes back as it is, and no job exists.
	code, h, b := st.do(t, "POST", "/jobs/a/job", "application/json", `{"x":"invalid"}`)
	if code != 422 || !strings.HasPrefix(h.Get("Content-Type"), "application/json") ||
		!strings.Contains(string(b), "invalid request") {
		t.Fatalf("refused submit: %d %s", code, b)
	}
	list, _, lb := st.do(t, "GET", "/jobs", "", "")
	var views []view
	json.Unmarshal(lb, &views)
	if list != 200 || len(views) != 1 {
		t.Fatalf("a refused request left a job: %s", lb)
	}
	entries, _ := os.ReadDir(filepath.Join(st.dir, "a"))
	if len(entries) != 1 {
		t.Fatalf("refused job left a folder: %d entries", len(entries))
	}
	ok := st.submit(t, "a", "job?for=10ms", "fine")
	if d := st.wait(t, ok.ID); d.Status != "done" {
		t.Fatalf("validated job: %s", d.Status)
	}
	if s := st.stats(t, "a"); s.Validates != 2 {
		t.Fatalf("validates = %d, want 2", s.Validates)
	}
}

func TestJobFailuresFromWorkload(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	for _, tc := range []struct {
		path, wantErr string
		status        int
	}{
		{"fail", "boom from a", 500},
		{"fail?status=422&detail=list", `"msg":"too many"`, 422},
		{"job?for=10ms&file=../x", "plain file name", 200},
		{"job?for=10ms&file=.hidden", "plain file name", 200},
		{"job?for=10ms&nofile=1", "not in the job folder", 200},
	} {
		v := st.submit(t, "a", tc.path, "x")
		d := st.wait(t, v.ID)
		if d.Status != "failed" || !strings.Contains(str(d.Error), tc.wantErr) || d.UpstreamStatus == nil || *d.UpstreamStatus != tc.status {
			t.Fatalf("%s: %+v (error %s)", tc.path, d, str(d.Error))
		}
		entries, _ := os.ReadDir(filepath.Join(st.dir, "a", v.ID))
		if len(entries) != 1 || entries[0].Name() != ".router" {
			t.Fatalf("%s: failed job kept files: %v", tc.path, entries)
		}
	}
}

func TestJobTimeout(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel + "\njobTimeout: 300ms"}})
	v := st.submit(t, "a", "job?for=5s&steps=50", "x")
	d := st.wait(t, v.ID)
	if d.Status != "failed" || !strings.Contains(str(d.Error), "timed out after 300ms") {
		t.Fatalf("%+v %s", d, str(d.Error))
	}
	next := st.submit(t, "a", "job?for=10ms", "y")
	if d := st.wait(t, next.ID); d.Status != "done" {
		t.Fatalf("job after a timeout: %s %s", d.Status, str(d.Error))
	}
	if s := st.stats(t, "a"); s.Disconnects != 1 || s.MaxConcurrent != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestJobCancelRestartsStuckWorkload(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{
		"a": {Env: []string{"FAKE_IGNORE_DISCONNECT=1"}, Extra: jobModel},
	})
	v := st.submit(t, "a", "job?for=10s&steps=100", "x")
	time.Sleep(300 * time.Millisecond)
	var pid int
	for _, info := range st.rt.Infos() {
		pid = info.PID
	}
	st.do(t, "DELETE", "/jobs/"+v.ID, "", "")
	next := st.submit(t, "a", "job?for=10ms", "y")
	start := time.Now()
	if d := st.wait(t, next.ID); d.Status != "done" {
		t.Fatalf("job after a stuck cancel: %s %s", d.Status, str(d.Error))
	}
	if d := time.Since(start); d < time.Second {
		t.Fatalf("next job ran after %s, before the stuck one was stopped", d)
	}
	for _, info := range st.rt.Infos() {
		if info.PID == pid {
			t.Fatal("stuck workload was not restarted")
		}
	}
}

func TestJobRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	st := newJobStack(t, dir, "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	running := st.submit(t, "a", "job?for=5s", "x")
	queued := st.submit(t, "a", "job?for=10ms", "y")
	done := st.submit(t, "a", "job?for=10ms", "z")
	time.Sleep(300 * time.Millisecond)
	st.do(t, "DELETE", "/jobs/"+done.ID, "", "") // a finished job to keep
	st.close()

	st2 := newJobStack(t, dir, "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	if v := st2.get(t, running.ID); v.Status != "failed" || str(v.Error) != jobs.ErrRestarted {
		t.Fatalf("running job after restart: %+v %s", v, str(v.Error))
	}
	if v := st2.wait(t, queued.ID); v.Status != "done" {
		t.Fatalf("queued job after restart: %s %s", v.Status, str(v.Error))
	}
	if v := st2.get(t, done.ID); v.Status != "cancelled" {
		t.Fatalf("finished job after restart: %s", v.Status)
	}
	if code, _, b := st2.do(t, "GET", "/jobs/"+queued.ID+"/result", "", ""); code != 200 || string(b) != "a:y" {
		t.Fatalf("result after restart: %d %q", code, b)
	}
}

func TestJobRetention(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{
		"a": {Extra: jobModel + "\nresultTTL: 1s"},
		"b": {Extra: jobModel + "\nkeepJobs: 1"},
	})
	old := st.submit(t, "a", "job?for=10ms", "x")
	st.wait(t, old.ID)
	orphan := filepath.Join(st.dir, "a", "0123456789ab")
	os.MkdirAll(orphan, 0o755)
	other := filepath.Join(st.dir, "a", "not-a-job")
	os.MkdirAll(other, 0o755)

	st.jm.Sweep(time.Now())
	if code, _, _ := st.do(t, "GET", "/jobs/"+old.ID, "", ""); code != 200 {
		t.Fatal("fresh job swept")
	}
	st.jm.Sweep(time.Now().Add(2 * time.Second))
	if code, _, _ := st.do(t, "GET", "/jobs/"+old.ID, "", ""); code != 404 {
		t.Fatal("old job not swept")
	}
	if _, err := os.Stat(filepath.Join(st.dir, "a", old.ID)); err == nil {
		t.Fatal("old job folder not removed")
	}
	if _, err := os.Stat(orphan); err == nil {
		t.Fatal("orphan folder not removed")
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatal("a folder not named like a job was removed")
	}

	b1 := st.submit(t, "b", "job?for=10ms", "1")
	st.wait(t, b1.ID)
	b2 := st.submit(t, "b", "job?for=10ms", "2")
	st.wait(t, b2.ID)
	if code, _, _ := st.do(t, "GET", "/jobs/"+b1.ID, "", ""); code != 404 {
		t.Fatal("keepJobs kept the older job")
	}
	if code, _, _ := st.do(t, "GET", "/jobs/"+b2.ID, "", ""); code != 200 {
		t.Fatal("keepJobs dropped the newest job")
	}
}

func TestRunningReportsQueueAndJobs(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	v := st.submit(t, "a", "job?for=1s", "x")
	time.Sleep(600 * time.Millisecond)
	code, _, b := st.do(t, "GET", "/running", "", "")
	var running struct {
		Models []struct {
			Name     string
			Active   bool
			Admitted int
			Queued   int
			Jobs     map[string]int
		}
	}
	json.Unmarshal(b, &running)
	if code != 200 || len(running.Models) != 1 {
		t.Fatalf("%d %s", code, b)
	}
	m := running.Models[0]
	if !m.Active || m.Admitted != 1 || m.Queued != 0 || m.Jobs["running"] != 1 {
		t.Fatalf("running: %s", b)
	}
	st.wait(t, v.ID)
}

func TestJobsConfigDefaults(t *testing.T) {
	cfg, err := config.Parse([]byte("models:\n  a:\n    cmd: x\n    maxBodySize: 2MB\n    linger: 3s\n"))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.Models["a"]
	if cfg.Jobs.ResultTTL != 30*24*time.Hour || cfg.Jobs.MaxBodySize != 64<<20 || !filepath.IsAbs(cfg.Jobs.Dir) ||
		m.MaxBodySize != 2<<20 || *m.Linger != 3*time.Second || *cfg.Batch.Linger != 2*time.Second || cfg.Batch.Cycle != 15*time.Minute ||
		m.DefaultEta != time.Minute || m.LoadTime != 30*time.Second || *m.SyncMaxWait != time.Minute || m.MaxWait != 0 || m.ResultTTL != cfg.Jobs.ResultTTL || m.Share != 1 {
		t.Fatalf("defaults: jobs %+v model %+v", cfg.Jobs, m)
	}
	if _, err := config.Parse([]byte("models:\n  a:\n    cmd: x\n    maxBodySize: lots\n")); err == nil {
		t.Fatal("bad size accepted")
	}
	old, err := config.Parse([]byte("timeShare:\n  period: 1m\n  minSlice: 2m\n  linger: 5s\nmodels:\n  a:\n    cmd: x\n"))
	if err != nil || old.Batch.Cycle != time.Minute || *old.Batch.Linger != 5*time.Second || len(old.Warnings) != 3 {
		t.Fatalf("v1 timeShare: %v %+v", err, old)
	}
}

func TestJobPlaceEstimateAndBusy(t *testing.T) {
	planned := jobModel + "\nloadTime: 1ms\ndefaultEta: 100ms"
	st := newJobStack(t, "", "", map[string]fakeserver.Model{
		"a": {Extra: planned},
		"b": {Extra: planned + "\nmaxWait: 1s"},
		"c": {Extra: planned},
	})
	first := st.submit(t, "a", "job?for=3s", `{}`)
	for st.get(t, first.ID).Status != "running" {
		time.Sleep(20 * time.Millisecond)
	}
	// a is loaded: validation runs and its eta_s becomes the job's cost.
	long := st.submit(t, "a", "job?for=100ms", `{"eta": 20}`)
	if v := st.get(t, long.ID); v.Batch == nil || *v.Batch != 1 || v.ETA == nil || *v.ETA < 19 {
		t.Fatalf("a's second job: batch %v eta %v", v.Batch, v.ETA)
	}

	code, h, b := st.do(t, "POST", "/jobs/b/job", "application/json", `{}`)
	var busy struct {
		Detail   string `json:"detail"`
		StartsIn int    `json:"starts_in_s"`
	}
	json.Unmarshal(b, &busy)
	if code != 429 || busy.StartsIn < 19 || h.Get("Retry-After") == "" || !strings.Contains(busy.Detail, "busy") {
		t.Fatalf("b past maxWait: %d %s (Retry-After %q)", code, b, h.Get("Retry-After"))
	}

	c := st.submit(t, "c", "job", `{}`)
	v := st.get(t, c.ID)
	if v.Batch == nil || *v.Batch != 2 || v.StartsIn == nil || *v.StartsIn < 19 {
		t.Fatalf("c: batch %v starts in %v", v.Batch, v.StartsIn)
	}

	code, _, b = st.do(t, "GET", "/running", "", "")
	var running struct {
		Schedule router.Schedule `json:"schedule"`
	}
	json.Unmarshal(b, &running)
	if code != 200 || len(running.Schedule.Batches) < 2 || running.Schedule.CycleS != 900 {
		t.Fatalf("running: %d %s", code, b)
	}
	for _, id := range []string{first.ID, long.ID, c.ID} {
		st.do(t, "DELETE", "/jobs/"+id, "", "")
	}
}

func TestJobValidateMissingEndpointIsSkipped(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{
		"a": {Extra: "validate:\n  endpoint: /no-such-route"},
	})
	warm := st.submit(t, "a", "job?for=10ms", `{}`)
	st.wait(t, warm.ID) // loaded: the next submit is validated
	v := st.submit(t, "a", "job?for=10ms", `{}`)
	if done := st.wait(t, v.ID); done.Status != "done" {
		t.Fatalf("job refused by a missing validate route: %+v", done)
	}
}
