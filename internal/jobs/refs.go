package jobs

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"os"
	"strings"

	"github.com/giapnguyen74/simple-ai-router/internal/artifacts"
)

// References let a job's request name a file the router holds instead of
// carrying it (docs/time-share-v2-plan.md, B1):
//
//	multipart: a text part "<field>@ref" = "job:<id>" or "artifact:<sha256>"
//	           becomes a file part "<field>" with the file
//	JSON:      a value {"$ref": "job:<id>"} becomes the file as base64
//
// The stored request keeps the references; they are resolved when the job
// is sent to its workload.

const (
	refSuffix  = "@ref"
	refKey     = "$ref"
	maxRefText = 512
)

type refKind int

const (
	refJob refKind = iota
	refArtifact
)

// parseRef reads "job:<id>" or "artifact:<sha256>" (also "artifact:sha256:<hex>").
func parseRef(s string) (refKind, string, error) {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, "job:"):
		id := strings.TrimPrefix(s, "job:")
		if !idRE.MatchString(id) {
			return 0, "", fmt.Errorf("invalid job reference %q", s)
		}
		return refJob, id, nil
	case strings.HasPrefix(s, "artifact:"):
		sha, ok := artifacts.ParseID(strings.TrimPrefix(s, "artifact:"))
		if !ok {
			return 0, "", fmt.Errorf("invalid artifact reference %q", s)
		}
		return refArtifact, sha, nil
	}
	return 0, "", fmt.Errorf("invalid reference %q: want job:<id> or artifact:<sha256>", s)
}

func bodyKind(contentType string) (multipartBoundary string, isJSON bool) {
	mt, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", false
	}
	if mt == "multipart/form-data" {
		return params["boundary"], false
	}
	return "", mt == "application/json" || strings.HasSuffix(mt, "+json")
}

// findRefs returns the references in a stored request body, normalized.
func findRefs(path, contentType string) ([]string, error) {
	boundary, isJSON := bodyKind(contentType)
	if boundary == "" && !isJSON {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var refs []string
	add := func(s string) error {
		kind, id, err := parseRef(s)
		if err != nil {
			return err
		}
		refs = append(refs, refString(kind, id))
		return nil
	}
	if boundary != "" {
		mr := multipart.NewReader(f, boundary)
		for {
			part, err := mr.NextRawPart()
			if errors.Is(err, io.EOF) {
				return refs, nil
			}
			if err != nil {
				return nil, fmt.Errorf("read multipart body: %v", err)
			}
			if name := part.FormName(); strings.HasSuffix(name, refSuffix) && part.FileName() == "" {
				text, err := io.ReadAll(io.LimitReader(part, maxRefText))
				if err != nil {
					return nil, err
				}
				if err := add(string(text)); err != nil {
					return nil, err
				}
			}
			io.Copy(io.Discard, part)
		}
	}
	var v any
	dec := json.NewDecoder(f)
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, nil // not ours to judge; the workload answers it
	}
	_, err = walkRefs(v, func(s string) (any, error) { return nil, add(s) })
	return refs, err
}

func refString(kind refKind, id string) string {
	if kind == refJob {
		return "job:" + id
	}
	return "artifact:" + id
}

// walkRefs calls fn on every {"$ref": "..."} object and puts what it returns
// in its place.
func walkRefs(v any, fn func(ref string) (any, error)) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		if s, ok := x[refKey].(string); ok && len(x) == 1 {
			return fn(s)
		}
		for k, e := range x {
			nv, err := walkRefs(e, fn)
			if err != nil {
				return nil, err
			}
			x[k] = nv
		}
	case []any:
		for i, e := range x {
			nv, err := walkRefs(e, fn)
			if err != nil {
				return nil, err
			}
			x[i] = nv
		}
	}
	return v, nil
}

// file is what a reference points to.
type file struct {
	path, name, contentType string
	size                    int64
}

// rewrite writes src to dst with every reference replaced by its file, and
// returns the size written.
func rewrite(src, dst, contentType string, lookup func(ref string) (file, error), maxInline int64) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	cw := &countWriter{w: bufio.NewWriter(out)}
	boundary, _ := bodyKind(contentType)
	if boundary != "" {
		err = rewriteMultipart(in, cw, boundary, lookup)
	} else {
		err = rewriteJSON(in, cw, lookup, maxInline)
	}
	if err == nil {
		err = cw.w.(*bufio.Writer).Flush()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(dst)
		return 0, err
	}
	return cw.n, nil
}

func rewriteMultipart(in io.Reader, out io.Writer, boundary string, lookup func(string) (file, error)) error {
	mr := multipart.NewReader(in, boundary)
	mw := multipart.NewWriter(out)
	if err := mw.SetBoundary(boundary); err != nil {
		return err
	}
	for {
		part, err := mr.NextRawPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name := part.FormName()
		if !strings.HasSuffix(name, refSuffix) || part.FileName() != "" {
			w, err := mw.CreatePart(part.Header)
			if err != nil {
				return err
			}
			if _, err := io.Copy(w, part); err != nil {
				return err
			}
			continue
		}
		text, err := io.ReadAll(io.LimitReader(part, maxRefText))
		if err != nil {
			return err
		}
		f, err := lookup(strings.TrimSpace(string(text)))
		if err != nil {
			return err
		}
		h := textproto.MIMEHeader{}
		h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			quoteEscaper.Replace(strings.TrimSuffix(name, refSuffix)), quoteEscaper.Replace(f.name)))
		h.Set("Content-Type", f.contentType)
		w, err := mw.CreatePart(h)
		if err != nil {
			return err
		}
		if err := copyFrom(w, f.path); err != nil {
			return err
		}
	}
	return mw.Close()
}

func rewriteJSON(in io.Reader, out io.Writer, lookup func(string) (file, error), maxInline int64) error {
	var v any
	dec := json.NewDecoder(in)
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return err
	}
	v, err := walkRefs(v, func(ref string) (any, error) {
		f, err := lookup(ref)
		if err != nil {
			return nil, err
		}
		if f.size > maxInline {
			return nil, fmt.Errorf("%s is %d bytes, over jobs.maxInlineRef (%d) for a JSON body; send it as a multipart part", ref, f.size, maxInline)
		}
		data, err := os.ReadFile(f.path)
		if err != nil {
			return nil, err
		}
		return base64.StdEncoding.EncodeToString(data), nil
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func copyFrom(w io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}

// quoteEscaper escapes a Content-Disposition parameter as mime/multipart does.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
