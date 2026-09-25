package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/fakeserver"
	"github.com/giapnguyen74/simple-ai-router/internal/router"
)

func TestMain(m *testing.M) {
	fakeserver.MaybeRun()
	os.Exit(m.Run())
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := fakeserver.Config(t, "", map[string]fakeserver.Model{
		"a": {Extra: "aliases: [gpt-4o-mini]"},
		"b": {},
	})
	rt := router.New(cfg, nil)
	t.Cleanup(rt.Shutdown)
	s, err := New(rt)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return ts
}

func post(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRoutesByModelAndSwaps(t *testing.T) {
	ts := newTestServer(t)

	for _, tc := range []struct{ model, want string }{
		{"a", `"server":"a"`},
		{"b", `"server":"b"`},
		{"gpt-4o-mini", `"server":"a"`},
	} {
		code, body := post(t, ts.URL+"/v1/chat/completions", `{"model":"`+tc.model+`"}`)
		if code != 200 || !strings.Contains(body, tc.want) {
			t.Fatalf("model %s: %d %s", tc.model, code, body)
		}
	}

	resp, err := http.Get(ts.URL + "/running")
	if err != nil {
		t.Fatal(err)
	}
	var running struct {
		Models []struct{ Name, State string }
	}
	json.NewDecoder(resp.Body).Decode(&running)
	resp.Body.Close()
	states := map[string]string{}
	for _, m := range running.Models {
		states[m.Name] = m.State
	}
	if states["a"] != "ready" || states["b"] != "stopped" {
		t.Fatalf("unexpected states: %v", states)
	}
}

func TestStreamingIsNotBuffered(t *testing.T) {
	ts := newTestServer(t)
	// Warm the model so load time does not count.
	post(t, ts.URL+"/v1/chat/completions", `{"model":"a"}`)

	start := time.Now()
	resp, err := http.Post(ts.URL+"/v1/chat/completions?chunks=5", "application/json", strings.NewReader(`{"model":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, `"chunk":0`) {
		t.Fatalf("first line = %q", line)
	}
	// The full stream takes ~500ms; the first chunk must arrive well before.
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("first chunk took %s; response is being buffered", d)
	}
}

func TestMultipartModelField(t *testing.T) {
	ts := newTestServer(t)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mw.WriteField("model", "b")
	fw, _ := mw.CreateFormFile("file", "a.wav")
	fw.Write([]byte("RIFF...."))
	mw.Close()

	// The fake has no audio route, so a 404 from upstream proves routing to b worked.
	resp, err := http.Post(ts.URL+"/v1/audio/transcriptions", mw.FormDataContentType(), &buf)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	r, _ := http.Get(ts.URL + "/running")
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !strings.Contains(string(b), `"name":"b","state":"ready"`) {
		t.Fatalf("b not started: %s", b)
	}
}

func TestUpstreamPassthrough(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/u/b/whoami")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "b /whoami" {
		t.Fatalf("got %q", b)
	}
}

func TestErrors(t *testing.T) {
	ts := newTestServer(t)
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"model":"nope"}`, 404},
		{`{}`, 400},
		{`not json`, 400},
	} {
		code, body := post(t, ts.URL+"/v1/chat/completions", tc.body)
		if code != tc.code || !strings.Contains(body, `"error"`) {
			t.Fatalf("%s: got %d %s", tc.body, code, body)
		}
	}
}

func TestModelsList(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Data []struct{ ID string }
	}
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	var ids []string
	for _, m := range list.Data {
		ids = append(ids, m.ID)
	}
	if got := strings.Join(ids, ","); got != "a,b,gpt-4o-mini" {
		t.Fatalf("models = %s", got)
	}
}
