// Package server exposes the OpenAI-compatible HTTP API and the admin routes.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/artifacts"
	"github.com/giapnguyen74/simple-ai-router/internal/jobs"
	"github.com/giapnguyen74/simple-ai-router/internal/router"
)

// maxBodySize caps request bodies read to find the model name. Audio uploads
// are the largest expected payload.
const maxBodySize = 64 << 20

type Server struct {
	router  *router.Router
	jobs    *jobs.Manager
	proxies map[string]*httputil.ReverseProxy
	mux     *http.ServeMux
}

// New builds the HTTP handler. jm may be nil: the job routes then answer 404.
func New(r *router.Router, jm *jobs.Manager) (*Server, error) {
	s := &Server{router: r, jobs: jm, proxies: make(map[string]*httputil.ReverseProxy), mux: http.NewServeMux()}
	for name, m := range r.Config().Models {
		target, err := url.Parse(m.Proxy)
		if err != nil {
			return nil, err
		}
		s.proxies[name] = newReverseProxy(target)
	}

	for _, path := range []string{
		"/v1/chat/completions",
		"/v1/completions",
		"/v1/embeddings",
		"/v1/rerank",
		"/v1/audio/speech",
		"/v1/audio/transcriptions",
	} {
		s.mux.HandleFunc("POST "+path, s.handleModelRequest)
	}
	s.mux.HandleFunc("GET /v1/models", s.handleModels)
	s.mux.HandleFunc("/u/{model}/{path...}", s.handleUpstream)
	if jm != nil {
		s.mux.HandleFunc("POST /jobs/{model}/{path...}", s.handleJobSubmit)
		s.mux.HandleFunc("GET /jobs", s.handleJobList)
		s.mux.HandleFunc("GET /jobs/{id}", s.handleJobGet)
		s.mux.HandleFunc("GET /jobs/{id}/wait", s.handleJobWait)
		s.mux.HandleFunc("GET /jobs/{id}/result", s.handleJobResult)
		s.mux.HandleFunc("DELETE /jobs/{id}", s.handleJobCancel)
		s.mux.HandleFunc("POST /artifacts", s.handleArtifactPut)
		s.mux.HandleFunc("GET /artifacts/{id}", s.handleArtifactGet)
		s.mux.HandleFunc("DELETE /artifacts/{id}", s.handleArtifactDelete)
	}
	s.mux.HandleFunc("GET /running", s.handleRunning)
	s.mux.HandleFunc("POST /unload", s.handleUnload)
	s.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "OK")
	})
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
		},
		// Flush every write so streamed tokens reach the client immediately.
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Error("upstream error", "path", r.URL.Path, "err", err)
			writeError(w, http.StatusBadGateway, "upstream error: "+err.Error())
		},
	}
}

func (s *Server) handleModelRequest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	model, err := modelFromBody(r.Header.Get("Content-Type"), body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	s.proxy(w, r, model)
}

func (s *Server) handleUpstream(w http.ResponseWriter, r *http.Request) {
	model := r.PathValue("model")
	r.URL.Path = "/" + r.PathValue("path")
	r.URL.RawPath = ""
	s.proxy(w, r, model)
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request, model string) {
	start := time.Now()
	p, release, err := s.router.AcquirePath(r.Context(), model, strings.TrimPrefix(r.URL.Path, "/"))
	if err != nil {
		var busy *router.BusyError
		switch {
		case errors.As(err, &busy):
			w.Header().Set("Retry-After", retryAfter(busy.StartsIn))
			writeError(w, http.StatusServiceUnavailable, busy.Error())
		case errors.Is(err, router.ErrUnknownModel):
			writeError(w, http.StatusNotFound, "model not found: "+model)
		case errors.Is(err, router.ErrQueueFull):
			w.Header().Set("Retry-After", "10")
			writeError(w, http.StatusTooManyRequests, "queue full for model "+model)
		case errors.Is(err, router.ErrQueueTimeout):
			w.Header().Set("Retry-After", "10")
			writeError(w, http.StatusServiceUnavailable, "timed out waiting for model "+model)
		case errors.Is(err, router.ErrShutdown):
			writeError(w, http.StatusServiceUnavailable, err.Error())
		case r.Context().Err() != nil:
			// Client went away; nothing to write.
		default:
			writeError(w, http.StatusBadGateway, err.Error())
		}
		return
	}
	defer release()
	if wait := time.Since(start); wait > time.Second {
		slog.Info("model loaded for request", "model", p.Name(), "wait", wait.Round(time.Millisecond))
	}
	s.proxies[p.Name()].ServeHTTP(w, r)
}

// modelFromBody extracts the "model" field from a JSON or multipart body.
func modelFromBody(contentType string, body []byte) (string, error) {
	mediaType, params, _ := mime.ParseMediaType(contentType)
	if mediaType == "multipart/form-data" {
		mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}
			if part.FormName() == "model" {
				v, err := io.ReadAll(io.LimitReader(part, 1024))
				if err != nil {
					return "", err
				}
				return strings.TrimSpace(string(v)), nil
			}
		}
		return "", errors.New(`missing "model" form field`)
	}

	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errors.New("invalid JSON body: " + err.Error())
	}
	if req.Model == "" {
		return "", errors.New(`missing "model" field`)
	}
	return req.Model, nil
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	var data []model
	for name, m := range s.router.Config().Models {
		data = append(data, model{ID: name, Object: "model", OwnedBy: "simple-ai-router"})
		for _, a := range m.Aliases {
			data = append(data, model{ID: a, Object: "model", OwnedBy: "simple-ai-router"})
		}
	}
	sort.Slice(data, func(i, j int) bool { return data[i].ID < data[j].ID })
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) handleRunning(w http.ResponseWriter, _ *http.Request) {
	infos := s.router.Infos()
	if s.jobs != nil {
		counts := s.jobs.Counts()
		for i := range infos {
			infos[i].Jobs = counts[infos[i].Name]
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"models": infos, "schedule": s.router.Schedule()})
}

func (s *Server) handleJobSubmit(w http.ResponseWriter, r *http.Request) {
	v, err := s.jobs.Submit(jobs.Request{
		Model:         r.PathValue("model"),
		Method:        r.Method,
		Path:          r.PathValue("path"),
		Query:         r.URL.RawQuery,
		ContentType:   r.Header.Get("Content-Type"),
		ContentLength: r.ContentLength,
		Body:          r.Body,
	})
	if err != nil {
		writeJobError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, v)
}

func (s *Server) handleJobList(w http.ResponseWriter, r *http.Request) {
	views, err := s.jobs.List(r.URL.Query().Get("model"))
	if err != nil {
		writeJobError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleJobGet(w http.ResponseWriter, r *http.Request) {
	v, err := s.jobs.Get(r.PathValue("id"))
	if err != nil {
		writeJobError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) handleJobWait(w http.ResponseWriter, r *http.Request) {
	timeout := 300.0
	if q := r.URL.Query().Get("timeout"); q != "" {
		v, err := strconv.ParseFloat(q, 64)
		if err != nil || !(v >= 0 && v <= 3600) {
			writeDetail(w, http.StatusUnprocessableEntity, "timeout must be 0-3600 seconds")
			return
		}
		timeout = v
	}
	v, err := s.jobs.Wait(r.Context(), r.PathValue("id"), time.Duration(timeout*float64(time.Second)))
	if err != nil {
		writeJobError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) handleJobCancel(w http.ResponseWriter, r *http.Request) {
	v, err := s.jobs.Cancel(r.PathValue("id"))
	if err != nil {
		writeJobError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// resultTypes names the content types of the usual outputs; the system's
// MIME table differs between machines.
var resultTypes = map[string]string{
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp",
	".webm": "video/webm", ".mp4": "video/mp4",
	".flac": "audio/flac", ".mp3": "audio/mpeg", ".wav": "audio/wav", ".ogg": "audio/ogg",
	".zip": "application/zip", ".json": "application/json", ".txt": "text/plain; charset=utf-8",
}

func (s *Server) handleJobResult(w http.ResponseWriter, r *http.Request) {
	path, name, err := s.jobs.Result(r.PathValue("id"))
	if err != nil {
		writeJobError(w, err)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeDetail(w, http.StatusNotFound, "the output of this job is gone")
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		writeDetail(w, http.StatusInternalServerError, err.Error())
		return
	}
	ext := strings.ToLower(filepath.Ext(name))
	ctype := resultTypes[ext]
	if ctype == "" {
		ctype = mime.TypeByExtension(ext)
	}
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	http.ServeContent(w, r, "", info.ModTime(), f)
}

// handleArtifactPut stores an upload: the raw body (named by X-Filename), or
// the "file" part of a multipart form.
func (s *Server) handleArtifactPut(w http.ResponseWriter, r *http.Request) {
	limit := int64(s.router.Config().Artifacts.MaxUpload)
	body := http.MaxBytesReader(w, r.Body, limit+1<<20)
	name, ctype := r.Header.Get("X-Filename"), r.Header.Get("Content-Type")
	var src io.Reader = body
	if mt, params, err := mime.ParseMediaType(ctype); err == nil && mt == "multipart/form-data" {
		mr := multipart.NewReader(body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err != nil {
				writeDetail(w, http.StatusBadRequest, `multipart upload without a "file" part`)
				return
			}
			if part.FormName() == "file" {
				src, name, ctype = part, part.FileName(), part.Header.Get("Content-Type")
				break
			}
		}
	}
	meta, err := s.jobs.Artifacts().Put(src, name, ctype, limit)
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, artifacts.ErrTooLarge) || errors.As(err, &tooLarge):
		writeDetail(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload exceeds %d bytes", limit))
		return
	case err != nil:
		writeDetail(w, http.StatusInternalServerError, "store upload: "+err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, meta)
}

func (s *Server) handleArtifactGet(w http.ResponseWriter, r *http.Request) {
	path, meta, err := s.jobs.Artifacts().Open(r.PathValue("id"))
	if err != nil {
		writeDetail(w, http.StatusNotFound, "no artifact "+r.PathValue("id"))
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeDetail(w, http.StatusNotFound, "no artifact "+r.PathValue("id"))
		return
	}
	defer f.Close()
	ctype := meta.ContentType
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": meta.Name}))
	w.Header().Set("ETag", `"`+meta.ID+`"`)
	http.ServeContent(w, r, "", time.Unix(int64(meta.Created), 0), f)
}

func (s *Server) handleArtifactDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.jobs.DeleteArtifact(r.PathValue("id")); err != nil {
		writeJobError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeJobError answers a job route's error: the router's own as
// {"detail": ...}, a workload's refusal as the workload sent it.
func writeJobError(w http.ResponseWriter, err error) {
	var reply *jobs.UpstreamReply
	if errors.As(err, &reply) {
		if reply.ContentType != "" {
			w.Header().Set("Content-Type", reply.ContentType)
		}
		w.WriteHeader(reply.Status)
		w.Write(reply.Body)
		return
	}
	var je *jobs.Error
	if errors.As(err, &je) {
		if je.StartsIn != nil {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, *je.StartsIn)))
			writeJSON(w, je.Status, map[string]any{"detail": je.Detail, "starts_in_s": *je.StartsIn})
			return
		}
		if je.Status == http.StatusTooManyRequests || je.Status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "10")
		}
		writeDetail(w, je.Status, je.Detail)
		return
	}
	writeDetail(w, http.StatusInternalServerError, err.Error())
}

// retryAfter is a Retry-After value in whole seconds, at least 1.
func retryAfter(d time.Duration) string {
	return strconv.Itoa(max(1, int(math.Ceil(d.Seconds()))))
}

func writeDetail(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]any{"detail": detail})
}

func (s *Server) handleUnload(w http.ResponseWriter, r *http.Request) {
	model := r.URL.Query().Get("model")
	if err := s.router.Unload(model); err != nil {
		writeError(w, http.StatusNotFound, "model not found: "+model)
		return
	}
	io.WriteString(w, "OK")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError writes an OpenAI-style error body.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    http.StatusText(status),
			"code":    status,
		},
	})
}
