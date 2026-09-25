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
package fakeserver

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

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
	mux.HandleFunc("/whoami", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s %s", name, r.URL.Path)
	})

	fmt.Fprintf(os.Stderr, "fake: %s listening on %s\n", name, os.Getenv("FAKE_PORT"))
	err := http.ListenAndServe("127.0.0.1:"+os.Getenv("FAKE_PORT"), mux)
	fmt.Fprintln(os.Stderr, "fake:", err)
	os.Exit(1)
}
