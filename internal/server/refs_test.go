package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/fakeserver"
)

// post submits a job and returns the status and view, whatever the status.
func (st *jobStack) post(t *testing.T, model, path, ctype, body string) (int, view, []byte) {
	t.Helper()
	code, _, b := st.do(t, "POST", "/jobs/"+model+"/"+path, ctype, body)
	var v view
	json.Unmarshal(b, &v)
	return code, v, b
}

func form(t *testing.T, fields map[string]string) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		mw.WriteField(k, v)
	}
	mw.Close()
	return mw.FormDataContentType(), buf.String()
}

type artifact struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
}

func (st *jobStack) upload(t *testing.T, name, data string) artifact {
	t.Helper()
	req, _ := http.NewRequest("POST", st.ts.URL+"/artifacts", strings.NewReader(data))
	req.Header.Set("X-Filename", name)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var a artifact
	json.NewDecoder(resp.Body).Decode(&a)
	if resp.StatusCode != 201 || !strings.HasPrefix(a.ID, "sha256:") {
		t.Fatalf("upload: %d %+v", resp.StatusCode, a)
	}
	return a
}

func TestArtifactUploadGetDelete(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	a := st.upload(t, "face.txt", "hello")
	again := st.upload(t, "other.txt", "hello")
	if again.ID != a.ID || a.Size != 5 || a.Name != "face.txt" {
		t.Fatalf("same bytes: %+v then %+v", a, again)
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	w, _ := mw.CreateFormFile("file", "voice.wav")
	w.Write([]byte("RIFF"))
	mw.Close()
	code, _, b := st.do(t, "POST", "/artifacts", mw.FormDataContentType(), buf.String())
	var m artifact
	json.Unmarshal(b, &m)
	if code != 201 || m.Name != "voice.wav" || m.Size != 4 {
		t.Fatalf("multipart upload: %d %s", code, b)
	}

	code, h, b := st.do(t, "GET", "/artifacts/"+a.ID, "", "")
	if code != 200 || string(b) != "hello" || !strings.Contains(h.Get("Content-Disposition"), "face.txt") {
		t.Fatalf("get: %d %q %v", code, b, h)
	}
	if code, _, _ = st.do(t, "DELETE", "/artifacts/"+a.ID, "", ""); code != 204 {
		t.Fatalf("delete: %d", code)
	}
	if code, _, _ = st.do(t, "GET", "/artifacts/"+a.ID, "", ""); code != 404 {
		t.Fatalf("get after delete: %d", code)
	}
}

func TestJobRefersToJobAndArtifact(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}, "b": {Extra: jobModel}})
	gen := st.submit(t, "a", "job?for=10ms", `{"song":1}`)
	st.wait(t, gen.ID)
	voice := st.upload(t, "voice.txt", "la la")

	// multipart: the @ref parts become file parts.
	ctype, body := form(t, map[string]string{"bundle@ref": "job:" + gen.ID, "voice@ref": "artifact:" + voice.ID, "format": "mp3"})
	code, v, b := st.post(t, "b", "job?for=10ms", ctype, body)
	if code != 202 {
		t.Fatalf("submit: %d %s", code, b)
	}
	done := st.wait(t, v.ID)
	_, _, out := st.do(t, "GET", "/jobs/"+v.ID+"/result", "", "")
	got := string(out)
	if done.Status != "done" || !strings.Contains(got, `a:{"song":1}`) || !strings.Contains(got, "la la") ||
		!strings.Contains(got, `name="bundle"; filename="out.txt"`) || strings.Contains(got, "@ref") ||
		!strings.Contains(got, "mp3") {
		t.Fatalf("workload got:\n%s", got)
	}

	// JSON: {"$ref": ...} becomes base64.
	code, v, b = st.post(t, "b", "job?for=10ms", "application/json", `{"image": {"$ref": "job:`+gen.ID+`"}, "n": 2}`)
	if code != 202 {
		t.Fatalf("submit json: %d %s", code, b)
	}
	st.wait(t, v.ID)
	_, _, out = st.do(t, "GET", "/jobs/"+v.ID+"/result", "", "")
	var sent struct {
		Image string `json:"image"`
		N     int    `json:"n"`
	}
	json.Unmarshal(bytes.TrimPrefix(out, []byte("b:")), &sent)
	img, _ := base64.StdEncoding.DecodeString(sent.Image)
	if string(img) != `a:{"song":1}` || sent.N != 2 {
		t.Fatalf("workload got %s", out)
	}

	for _, bad := range []string{`{"x": {"$ref": "job:000000000000"}}`, `{"x": {"$ref": "artifact:` + strings.Repeat("0", 64) + `"}}`, `{"x": {"$ref": "nope"}}`} {
		if code, _, b := st.post(t, "b", "job", "application/json", bad); code != 422 && code != 400 {
			t.Fatalf("bad reference %s: %d %s", bad, code, b)
		}
	}
}

func TestPipelineSubmittedAtOnce(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}, "b": {Extra: jobModel}})
	gen := st.submit(t, "a", "job?for=400ms", `{"song":2}`)
	code, dec, b := st.post(t, "a", "job?for=10ms", "application/json", `{"latents": {"$ref": "job:`+gen.ID+`"}}`)
	if code != 202 || dec.Status != "blocked" {
		t.Fatalf("dependent: %d %s", code, b)
	}
	ctype, body := form(t, map[string]string{"audio@ref": "job:" + dec.ID})
	code, lyr, b := st.post(t, "b", "job?for=10ms", ctype, body)
	if code != 202 || lyr.Status != "blocked" {
		t.Fatalf("second stage: %d %s", code, b)
	}
	var deps struct {
		DependsOn []string `json:"depends_on"`
	}
	json.Unmarshal(b, &deps)
	if len(deps.DependsOn) != 1 || deps.DependsOn[0] != dec.ID {
		t.Fatalf("depends_on %v", deps.DependsOn)
	}
	if v := st.wait(t, lyr.ID); v.Status != "done" {
		t.Fatalf("pipeline end: %+v", v)
	}
	_, _, out := st.do(t, "GET", "/jobs/"+lyr.ID+"/result", "", "")
	if !strings.Contains(string(out), "a:") || !strings.Contains(string(out), base64.StdEncoding.EncodeToString([]byte(`a:{"song":2}`))) {
		t.Fatalf("pipeline output %s", out)
	}
}

func TestDependencyFailureFailsDependents(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{"a": {Extra: jobModel}})
	bad := st.submit(t, "a", "fail?status=500", `{}`)
	_, dep, _ := st.post(t, "a", "job", "application/json", `{"x": {"$ref": "job:`+bad.ID+`"}}`)
	_, dep2, _ := st.post(t, "a", "job", "application/json", `{"x": {"$ref": "job:`+dep.ID+`"}}`)
	for _, id := range []string{dep.ID, dep2.ID} {
		v := st.wait(t, id)
		if v.Status != "failed" || !strings.Contains(str(v.Error), "dependency") {
			t.Fatalf("dependent %s: %+v %s", id, v, str(v.Error))
		}
	}
	if code, _, b := st.post(t, "a", "job", "application/json", `{"x": {"$ref": "job:`+bad.ID+`"}}`); code != 422 {
		t.Fatalf("referring to a failed job: %d %s", code, b)
	}
}

func TestReferencedJobSurvivesRetention(t *testing.T) {
	st := newJobStack(t, "", "", map[string]fakeserver.Model{
		"a": {Extra: jobModel + "\nresultTTL: 1s"},
		"b": {Extra: jobModel},
	})
	src := st.submit(t, "a", "job?for=10ms", "x")
	st.wait(t, src.ID)
	busy := st.submit(t, "b", "job?for=1s", "y") // keeps the dependent queued
	_, user, _ := st.post(t, "b", "job?for=10ms", "application/json", `{"x": {"$ref": "job:`+src.ID+`"}}`)

	st.jm.Sweep(time.Now().Add(time.Hour))
	if code, _, _ := st.do(t, "GET", "/jobs/"+src.ID, "", ""); code != 200 {
		t.Fatal("a job a queued job refers to was swept")
	}
	if v := st.wait(t, user.ID); v.Status != "done" {
		t.Fatalf("dependent: %+v %s", v, str(v.Error))
	}
	st.wait(t, busy.ID)
	st.jm.Sweep(time.Now().Add(time.Hour))
	if code, _, _ := st.do(t, "GET", "/jobs/"+src.ID, "", ""); code != 404 {
		t.Fatal("job kept after nothing refers to it")
	}

	// The output lives on in the artifact store.
	shas, _ := filepath.Glob(filepath.Join(st.dir, ".artifacts", "*", "*.json"))
	if len(shas) == 0 {
		t.Fatal("no output in the artifact store")
	}
	if _, err := os.Stat(filepath.Join(st.dir, ".artifacts")); err != nil {
		t.Fatal(err)
	}
}
