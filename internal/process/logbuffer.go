package process

import (
	"bytes"
	"io"
	"sync"
)

// LogBuffer keeps the last Size bytes written to it and optionally tees every
// write to another writer.
type LogBuffer struct {
	mu   sync.Mutex
	buf  []byte
	size int
	tee  io.Writer
}

func NewLogBuffer(size int, tee io.Writer) *LogBuffer {
	return &LogBuffer{size: size, tee: tee}
}

func (b *LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	b.buf = append(b.buf, p...)
	if over := len(b.buf) - b.size; over > 0 {
		b.buf = append(b.buf[:0], b.buf[over:]...)
	}
	b.mu.Unlock()
	if b.tee != nil {
		b.tee.Write(p)
	}
	return len(p), nil
}

// Tail returns up to the last n lines.
func (b *LogBuffer) Tail(n int) string {
	if n <= 0 {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data := bytes.TrimRight(b.buf, "\n")
	for i := len(data) - 1; i >= 0; i-- {
		if data[i] == '\n' {
			n--
			if n == 0 {
				return string(data[i+1:])
			}
		}
	}
	return string(data)
}

// prefixWriter prepends a prefix to every line. Partial lines are buffered
// until their newline arrives.
type prefixWriter struct {
	mu      sync.Mutex
	w       io.Writer
	prefix  []byte
	partial []byte
}

func newPrefixWriter(w io.Writer, prefix string) *prefixWriter {
	return &prefixWriter{w: w, prefix: []byte(prefix)}
}

func (pw *prefixWriter) Write(p []byte) (int, error) {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	pw.partial = append(pw.partial, p...)
	for {
		i := bytes.IndexByte(pw.partial, '\n')
		if i < 0 {
			break
		}
		line := make([]byte, 0, len(pw.prefix)+i+1)
		line = append(line, pw.prefix...)
		line = append(line, pw.partial[:i+1]...)
		pw.w.Write(line)
		pw.partial = pw.partial[i+1:]
	}
	return len(p), nil
}
