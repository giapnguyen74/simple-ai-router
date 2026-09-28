// Package artifacts is the router's content-addressed file store: uploads
// that many jobs refer to, and the outputs of finished jobs, each stored once
// by its SHA-256.
//
//	<jobs.dir>/.artifacts/<sha[:2]>/<sha>        the bytes
//	<jobs.dir>/.artifacts/<sha[:2]>/<sha>.json   name, content type, size, times
package artifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Dir is the store's folder name inside jobs.dir.
const Dir = ".artifacts"

var (
	ErrNotFound = errors.New("no such artifact")
	ErrTooLarge = errors.New("upload too large")

	shaRE = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Meta describes one stored file.
type Meta struct {
	ID          string  `json:"id"` // "sha256:<hex>"
	Name        string  `json:"name"`
	ContentType string  `json:"content_type,omitempty"`
	Size        int64   `json:"size"`
	Created     float64 `json:"created"`
	LastUsed    float64 `json:"last_used"`
}

type Store struct {
	dir     string
	maxSize int64
	ttl     time.Duration
	mu      sync.Mutex // serializes writes of metadata and eviction
}

func New(jobsDir string, maxSize int64, ttl time.Duration) *Store {
	return &Store{dir: filepath.Join(jobsDir, Dir), maxSize: maxSize, ttl: ttl}
}

// ParseID accepts "sha256:<hex>" or a bare hex digest and returns the hex.
func ParseID(id string) (string, bool) {
	sha := strings.TrimPrefix(id, "sha256:")
	return sha, shaRE.MatchString(sha)
}

func (s *Store) path(sha string) string { return filepath.Join(s.dir, sha[:2], sha) }

func now() float64 { return float64(time.Now().UnixMicro()) / 1e6 }

// Put streams r into the store (at most limit bytes when limit > 0) and
// returns its metadata. The same bytes stored twice are one artifact.
func (s *Store) Put(r io.Reader, name, contentType string, limit int64) (*Meta, error) {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(s.dir, "upload.*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	src := r
	if limit > 0 {
		src = io.LimitReader(r, limit+1)
	}
	n, err := io.Copy(io.MultiWriter(tmp, h), src)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if limit > 0 && n > limit {
		return nil, ErrTooLarge
	}
	sha := hex.EncodeToString(h.Sum(nil))
	return s.add(sha, n, name, contentType, func(dst string) error { return os.Rename(tmp.Name(), dst) })
}

// Link adds an existing file (a job's output) by hard link, or by copy when
// the store is on another filesystem.
func (s *Store) Link(src, name, contentType string) (*Meta, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		return nil, err
	}
	sha := hex.EncodeToString(h.Sum(nil))
	return s.add(sha, n, name, contentType, func(dst string) error {
		if err := os.Link(src, dst); err == nil {
			return nil
		}
		return copyFile(src, dst)
	})
}

func (s *Store) add(sha string, size int64, name, contentType string, place func(dst string) error) (*Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dst := s.path(sha)
	if m, err := s.readMeta(sha); err == nil {
		if _, err := os.Stat(dst); err == nil {
			m.LastUsed = now()
			return m, s.writeMeta(m)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return nil, err
	}
	if err := place(dst); err != nil {
		return nil, err
	}
	t := now()
	m := &Meta{ID: "sha256:" + sha, Name: cleanName(name, sha), ContentType: contentType, Size: size, Created: t, LastUsed: t}
	return m, s.writeMeta(m)
}

// cleanName keeps a file name usable as a download name and a multipart
// filename: no path, never empty.
func cleanName(name, sha string) string {
	name = filepath.Base(strings.ReplaceAll(name, `\`, "/"))
	if name == "." || name == "/" || name == "" || strings.ContainsAny(name, "\x00\"\r\n") {
		return sha[:12]
	}
	return name
}

// Open returns the path and metadata of an artifact and marks it used.
func (s *Store) Open(id string) (string, *Meta, error) {
	sha, ok := ParseID(id)
	if !ok {
		return "", nil, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.readMeta(sha)
	if err != nil {
		return "", nil, ErrNotFound
	}
	p := s.path(sha)
	if _, err := os.Stat(p); err != nil {
		return "", nil, ErrNotFound
	}
	m.LastUsed = now()
	s.writeMeta(m)
	return p, m, nil
}

// Has reports whether the artifact exists, without marking it used.
func (s *Store) Has(id string) bool {
	sha, ok := ParseID(id)
	if !ok {
		return false
	}
	_, err := os.Stat(s.path(sha))
	return err == nil
}

// Delete removes an artifact unless pinned says it is in use.
func (s *Store) Delete(id string, pinned func(sha string) bool) error {
	sha, ok := ParseID(id)
	if !ok || !s.Has(id) {
		return ErrNotFound
	}
	if pinned != nil && pinned(sha) {
		return fmt.Errorf("artifact %s is used by a queued job", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remove(sha)
}

func (s *Store) remove(sha string) error {
	os.Remove(s.path(sha) + ".json")
	return os.Remove(s.path(sha))
}

func (s *Store) readMeta(sha string) (*Meta, error) {
	data, err := os.ReadFile(s.path(sha) + ".json")
	if err != nil {
		return nil, err
	}
	m := &Meta{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Store) writeMeta(m *Meta) error {
	sha, _ := ParseID(m.ID)
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	dst := s.path(sha) + ".json"
	tmp, err := os.CreateTemp(filepath.Dir(dst), "meta.*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(data)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), dst)
	}
	if err != nil {
		os.Remove(tmp.Name())
	}
	return err
}

// Sweep removes artifacts not used for ttl, then the least recently used
// ones while the store is over maxSize. Pinned artifacts stay.
func (s *Store) Sweep(at time.Time, pinned func(sha string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*Meta
	var total int64
	dirs, _ := os.ReadDir(s.dir)
	for _, d := range dirs {
		if !d.IsDir() || len(d.Name()) != 2 {
			if strings.HasPrefix(d.Name(), "upload.") {
				if info, err := d.Info(); err == nil && at.Sub(info.ModTime()) > time.Hour {
					os.Remove(filepath.Join(s.dir, d.Name())) // left by a crash
				}
			}
			continue
		}
		files, _ := os.ReadDir(filepath.Join(s.dir, d.Name()))
		for _, f := range files {
			if !shaRE.MatchString(f.Name()) {
				continue
			}
			m, err := s.readMeta(f.Name())
			if err != nil {
				info, ierr := f.Info()
				if ierr != nil {
					continue
				}
				m = &Meta{ID: "sha256:" + f.Name(), Size: info.Size(), LastUsed: float64(info.ModTime().Unix())}
			}
			all = append(all, m)
			total += m.Size
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].LastUsed < all[j].LastUsed })
	cutoff := float64(at.Add(-s.ttl).Unix())
	removed := 0
	for _, m := range all {
		sha, _ := ParseID(m.ID)
		old := s.ttl > 0 && m.LastUsed < cutoff
		full := s.maxSize > 0 && total > s.maxSize
		if !old && !full {
			continue
		}
		if pinned != nil && pinned(sha) {
			continue
		}
		if s.remove(sha) == nil {
			total -= m.Size
			removed++
		}
	}
	if removed > 0 {
		slog.Info("artifacts: removed", "artifacts", removed)
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), "copy.*")
	if err != nil {
		return err
	}
	_, err = io.Copy(tmp, in)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), dst)
	}
	if err != nil {
		os.Remove(tmp.Name())
	}
	return err
}
