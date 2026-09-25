// Package server exposes the OpenAI-compatible HTTP API and the admin routes.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/router"
)

// maxBodySize caps request bodies read to find the model name. Audio uploads
// are the largest expected payload.
const maxBodySize = 64 << 20

type Server struct {
	router  *router.Router
	proxies map[string]*httputil.ReverseProxy
	mux     *http.ServeMux
}

func New(r *router.Router) (*Server, error) {
	s := &Server{router: r, proxies: make(map[string]*httputil.ReverseProxy), mux: http.NewServeMux()}
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
	p, release, err := s.router.Acquire(r.Context(), model)
	if err != nil {
		switch {
		case errors.Is(err, router.ErrUnknownModel):
			writeError(w, http.StatusNotFound, "model not found: "+model)
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
	writeJSON(w, http.StatusOK, map[string]any{"models": s.router.Infos()})
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
