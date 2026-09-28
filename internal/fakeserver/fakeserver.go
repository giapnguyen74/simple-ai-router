// Package fakeserver is a stand-in inference server for tests. Test binaries
// call MaybeRun from TestMain; when FAKE_SERVER=1 the binary becomes the fake
// server instead of running tests, so the router can launch it as a child.
//
// Behavior is controlled with environment variables:
//
//	FAKE_PORT          port to listen on (required)
//	FAKE_NAME          returned in responses so tests can tell servers apart
//	FAKE_START_DELAY   delay before /health returns 200 (e.g. "500ms")
//	FAKE_EXIT_AT_START exit with status 1 right away
//	FAKE_IGNORE_TERM   ignore SIGTERM, forcing a SIGKILL
//	FAKE_IGNORE_DISCONNECT  a job keeps running after its client hung up
//
// As a synchronous workload for the router's jobs it serves:
//
//	POST /job       runs for ?for=300ms, writes ?file= (default out.txt, holding
//	                the server name and the request body) into X-Job-Dir and
//	                answers {"file", "result"}. ?file= may be any name, also a
//	                bad one; ?nofile=1 names a file without writing it;
//	                ?steps=N sets what /progress reports
//	POST /fail      answers ?status= (default 500) with {"detail": ...};
//	                ?detail=list makes the detail a list
//	POST /validate  422 when the body contains "invalid", else 200, with
//	                {"eta_s": N} when the JSON body has "eta": N
//	GET  /progress  {"busy", "id", "phase", "step", "steps", "eta_s"}
//	GET  /stats     counters: jobs, disconnects, validates, maxConcurrent
package fakeserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Stats is what GET /stats returns.
type Stats struct {
	Jobs          int `json:"jobs"`
	Disconnects   int `json:"disconnects"`
	Validates     int `json:"validates"`
	MaxConcurrent int `json:"maxConcurrent"`
}

// workload is the synchronous job side of the fake server.
type workload struct {
	name string

	mu      sync.Mutex
	stats   Stats
	running int
	id      string
	step    int
	steps   int
}

func (wl *workload) register(mux *http.ServeMux) {
	mux.HandleFunc("POST /job", wl.job)
	mux.HandleFunc("POST /fail", func(w http.ResponseWriter, r *http.Request) {
		status, _ := strconv.Atoi(r.URL.Query().Get("status"))
		if status == 0 {
			status = http.StatusInternalServerError
		}
		var detail any = "boom from " + wl.name
		if r.URL.Query().Get("detail") == "list" {
			detail = []map[string]any{{"loc": []string{"body", "steps"}, "msg": "too many"}}
		}
		writeJSON(w, status, map[string]any{"detail": detail})
	})
	mux.HandleFunc("POST /validate", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		wl.mu.Lock()
		wl.stats.Validates++
		wl.mu.Unlock()
		if strings.Contains(string(body), "invalid") {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": "invalid request"})
			return
		}
		var req struct {
			ETA float64 `json:"eta"`
		}
		if json.Unmarshal(body, &req) == nil && req.ETA > 0 {
			writeJSON(w, http.StatusOK, map[string]any{"eta_s": req.ETA})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	})
	mux.HandleFunc("GET /progress", func(w http.ResponseWriter, r *http.Request) {
		wl.mu.Lock()
		defer wl.mu.Unlock()
		if wl.running == 0 {
			writeJSON(w, http.StatusOK, map[string]any{"busy": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"busy": true, "id": wl.id, "phase": "sampling",
			"step": wl.step, "steps": wl.steps, "eta_s": wl.steps - wl.step})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		wl.mu.Lock()
		defer wl.mu.Unlock()
		writeJSON(w, http.StatusOK, wl.stats)
	})
}

func (wl *workload) job(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	body, _ := io.ReadAll(r.Body)
	d, _ := time.ParseDuration(q.Get("for"))
	steps, _ := strconv.Atoi(q.Get("steps"))
	steps = max(steps, 1)
	id, dir := r.Header.Get("X-Job-Id"), r.Header.Get("X-Job-Dir")

	wl.mu.Lock()
	wl.stats.Jobs++
	wl.running++
	wl.stats.MaxConcurrent = max(wl.stats.MaxConcurrent, wl.running)
	wl.id, wl.step, wl.steps = id, 0, steps
	wl.mu.Unlock()
	defer func() {
		wl.mu.Lock()
		wl.running--
		wl.mu.Unlock()
	}()

	ignore := os.Getenv("FAKE_IGNORE_DISCONNECT") == "1"
	gone := false
	for i := range steps {
		select {
		case <-time.After(d / time.Duration(steps)):
		case <-r.Context().Done():
			if !gone {
				gone = true
				wl.mu.Lock()
				wl.stats.Disconnects++
				wl.mu.Unlock()
			}
			if !ignore {
				return
			}
			time.Sleep(d / time.Duration(steps))
		}
		wl.mu.Lock()
		wl.step = i + 1
		wl.mu.Unlock()
	}

	file := q.Get("file")
	if file == "" {
		file = "out.txt"
	}
	if dir == "" {
		// No router: the answer is the file itself.
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "%s:%s", wl.name, body)
		return
	}
	if q.Get("nofile") != "1" {
		path := filepath.Join(dir, file)
		os.MkdirAll(filepath.Dir(path), 0o755)
		if err := os.WriteFile(path, []byte(wl.name+":"+string(body)), 0o644); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"file": file,
		"result": map[string]any{"server": wl.name, "id": id, "bytes": len(body),
			"contentType": r.Header.Get("Content-Type"), "query": r.URL.RawQuery},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func MaybeRun() {
	if os.Getenv("FAKE_SERVER") != "1" {
		return
	}
	run()
	os.Exit(0)
}

func run() {
	if os.Getenv("FAKE_EXIT_AT_START") == "1" {
		fmt.Fprintln(os.Stderr, "fake: exiting at start")
		os.Exit(1)
	}
	if os.Getenv("FAKE_IGNORE_TERM") == "1" {
		signal.Ignore(syscall.SIGTERM)
	}
	name := os.Getenv("FAKE_NAME")
	delay, _ := time.ParseDuration(os.Getenv("FAKE_START_DELAY"))
	readyAt := time.Now().Add(delay)

	var requests atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if time.Now().Before(readyAt) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		chunks, _ := strconv.Atoi(r.URL.Query().Get("chunks"))
		if chunks == 0 {
			fmt.Fprintf(w, `{"server":%q,"pid":%d}`, name, os.Getpid())
			return
		}
		// Stream SSE chunks with a pause between them.
		w.Header().Set("Content-Type", "text/event-stream")
		for i := range chunks {
			fmt.Fprintf(w, "data: {\"server\":%q,\"chunk\":%d}\n\n", name, i)
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	// A job server stand-in: /work?for=500ms returns at once and stays busy in
	// the background; /state reports it like a job queue's status endpoint.
	var busyUntil atomic.Int64
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		d, _ := time.ParseDuration(r.URL.Query().Get("for"))
		busyUntil.Store(time.Now().Add(d).UnixNano())
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		running := time.Now().UnixNano() < busyUntil.Load()
		fmt.Fprintf(w, `{"status":"ok","running":%t,"queued":0}`, running)
	})
	(&workload{name: name}).register(mux)
	mux.HandleFunc("/whoami", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s %s", name, r.URL.Path)
	})

	fmt.Fprintf(os.Stderr, "fake: %s listening on %s\n", name, os.Getenv("FAKE_PORT"))
	err := http.ListenAndServe("127.0.0.1:"+os.Getenv("FAKE_PORT"), mux)
	fmt.Fprintln(os.Stderr, "fake:", err)
	os.Exit(1)
}
