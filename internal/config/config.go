// Package config loads and validates the router's YAML or JSON configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen             = ":8080"
	defaultStartPort          = 10001
	defaultHealthCheckTimeout = 120 * time.Second
	defaultStopTimeout        = 10 * time.Second
	defaultDrainTimeout       = 5 * time.Minute
	defaultCheckEndpoint      = "/health"
	defaultBusyInterval       = 2 * time.Second
	defaultBusyGrace          = 15 * time.Second
	defaultMinCycle           = 2 * time.Minute
	defaultMaxCycle           = 30 * time.Minute
	defaultMaxSwitchOverhead  = 0.1
	defaultLinger             = 2 * time.Second
	defaultDefaultEta         = time.Minute
	defaultLoadTime           = 30 * time.Second
	defaultSyncMaxWait        = time.Minute
	defaultJobsDir            = "jobs"
	defaultResultTTL          = 30 * 24 * time.Hour
	defaultMaxBodySize        = 64 << 20
	defaultProgressInterval   = time.Second
	defaultMaxInlineRef       = 32 << 20
	defaultArtifactsMaxSize   = 50 << 30
	defaultArtifactsMaxUpload = 1 << 30
)

type Config struct {
	Listen             string        `yaml:"listen"`
	StartPort          int           `yaml:"startPort"`
	HealthCheckTimeout time.Duration `yaml:"healthCheckTimeout"`
	// DrainTimeout bounds how long a swap waits for in-flight requests on
	// the outgoing model before killing it anyway.
	DrainTimeout time.Duration `yaml:"drainTimeout"`
	Batch        Batch         `yaml:"batch"`
	// TimeShare is the v1 section, still read: period becomes batch.cycle
	// and linger batch.linger.
	TimeShare TimeShare               `yaml:"timeShare"`
	Jobs      Jobs                    `yaml:"jobs"`
	Artifacts Artifacts               `yaml:"artifacts"`
	Models    map[string]*ModelConfig `yaml:"models"`

	// Warnings are problems that do not stop the router, such as deprecated
	// fields. Filled in by Load.
	Warnings []string `yaml:"-"`

	// aliases maps an alias to its real model name. Built by Load.
	aliases map[string]string
}

// Batch configures the batch scheduler (docs/time-share-v2-plan.md). The
// length of each batch adapts: long enough that loading its models costs at
// most MaxSwitchOverhead of it and that its longest job fits, within
// MinCycle and MaxCycle.
type Batch struct {
	MinCycle time.Duration `yaml:"minCycle"`
	MaxCycle time.Duration `yaml:"maxCycle"`
	// MaxSwitchOverhead is the share of a batch its model loads may take.
	MaxSwitchOverhead float64 `yaml:"maxSwitchOverhead"`
	// Cycle is deprecated: read as maxCycle.
	Cycle time.Duration `yaml:"cycle"`
	// Linger is how long an idle model whose turn it is waits for more work
	// before its turn ends. Nil means the default; a pointer so that an
	// explicit 0s is not mistaken for unset.
	Linger *time.Duration `yaml:"linger"`
}

// TimeShare is the deprecated v1 section.
type TimeShare struct {
	Period   time.Duration  `yaml:"period"`
	MinSlice time.Duration  `yaml:"minSlice"`
	Linger   *time.Duration `yaml:"linger"`
}

// Jobs configures the router's job store.
type Jobs struct {
	// Dir holds one folder per job: <dir>/<model>/<job id>/. A relative path
	// is resolved against the config file's directory.
	Dir string `yaml:"dir"`
	// ResultTTL is how long a finished job is kept.
	ResultTTL time.Duration `yaml:"resultTTL"`
	// MaxBodySize caps a stored job request.
	MaxBodySize ByteSize `yaml:"maxBodySize"`
	// MaxInlineRef caps a file put into a JSON body in place of a
	// {"$ref": ...} (as base64).
	MaxInlineRef ByteSize `yaml:"maxInlineRef"`
}

// Artifacts configures the store of files that jobs refer to, in
// <jobs.dir>/.artifacts.
type Artifacts struct {
	// MaxSize: beyond it the least recently used artifacts are removed.
	MaxSize ByteSize `yaml:"maxSize"`
	// TTL removes an artifact not used for this long (default
	// jobs.resultTTL).
	TTL time.Duration `yaml:"ttl"`
	// MaxUpload caps one POST /artifacts.
	MaxUpload ByteSize `yaml:"maxUpload"`
}

// ByteSize is a byte count: a plain number, or one with a KB, MB or GB
// suffix (powers of 1024).
type ByteSize int64

func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*b = v
	return nil
}

func ParseByteSize(s string) (ByteSize, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}} {
		if strings.HasSuffix(t, u.suffix) {
			t, mult = strings.TrimSpace(strings.TrimSuffix(t, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 || n > (1<<62)/mult {
		return 0, fmt.Errorf("invalid size %q (want bytes, or a number with KB, MB or GB)", s)
	}
	return ByteSize(n * mult), nil
}

// Endpoint names one path on the model server.
type Endpoint struct {
	Endpoint string `yaml:"endpoint"`
}

// Progress is polled while one of the model's jobs runs.
type Progress struct {
	Endpoint string        `yaml:"endpoint"`
	Interval time.Duration `yaml:"interval"`
}

type ModelConfig struct {
	Cmd           string        `yaml:"cmd"`
	Proxy         string        `yaml:"proxy"`
	CheckEndpoint string        `yaml:"checkEndpoint"`
	TTL           time.Duration `yaml:"ttl"`
	Aliases       []string      `yaml:"aliases"`
	Env           []string      `yaml:"env"`
	StopTimeout   time.Duration `yaml:"stopTimeout"`
	// DrainTimeout overrides the global drainTimeout for this model.
	DrainTimeout time.Duration `yaml:"drainTimeout"`
	// BusyCheck lets servers that work in the background (job queues)
	// report that they are busy even with no request in flight.
	BusyCheck *BusyCheck `yaml:"busyCheck"`
	// Disabled temporarily removes the model: it is not listed, cannot be
	// resolved and never starts. It keeps its port slot so the other
	// models' ports do not shift.
	Disabled bool `yaml:"disabled"`

	// Share weighs this model's part of a batch against the others
	// (default 1).
	Share int `yaml:"share"`
	// Concurrency caps the requests admitted at the same time (0 = no cap).
	// Jobs run one at a time unless it is above 1.
	Concurrency int `yaml:"concurrency"`
	// MaxQueue caps the requests waiting in the router (0 = no cap).
	MaxQueue int `yaml:"maxQueue"`
	// QueueTimeout bounds how long a request waits to be admitted (0 = none).
	QueueTimeout time.Duration `yaml:"queueTimeout"`
	// JobTimeout bounds one job's run (0 = none).
	JobTimeout time.Duration `yaml:"jobTimeout"`
	// ResultTTL, KeepJobs and MaxBodySize override the jobs section.
	// KeepJobs keeps only the newest N finished jobs (0 = no limit).
	ResultTTL   time.Duration `yaml:"resultTTL"`
	KeepJobs    int           `yaml:"keepJobs"`
	MaxBodySize ByteSize      `yaml:"maxBodySize"`
	// Linger overrides batch.linger.
	Linger *time.Duration `yaml:"linger"`
	// DefaultEta is the cost of a request before anything is learned about
	// its path (default 1m).
	DefaultEta time.Duration `yaml:"defaultEta"`
	// LoadTime is the first guess of the model's load time, until one is
	// measured (default 30s).
	LoadTime time.Duration `yaml:"loadTime"`
	// SyncMaxWait: a request that stays open (/v1/...) and would wait longer
	// than this for its turn is answered 503 at once (default 1m; 0 = wait).
	SyncMaxWait *time.Duration `yaml:"syncMaxWait"`
	// MaxWait: a job whose predicted start is later is refused with 429
	// (0 = no limit).
	MaxWait  time.Duration `yaml:"maxWait"`
	Progress *Progress     `yaml:"progress"`
	Validate *Endpoint     `yaml:"validate"`

	// Filled in by Load.
	Name string   `yaml:"-"`
	Port int      `yaml:"-"`
	Args []string `yaml:"-"`
}

// DefaultPaths are tried in order when no config path is given.
var DefaultPaths = []string{"config.yaml", "config.yml", "config.json"}

// Load reads a config file. Files ending in .json are parsed as JSON,
// anything else as YAML. An empty path tries DefaultPaths.
// BusyCheck polls a JSON endpoint; the model is busy while any of Fields
// (top-level keys) is truthy: not null, false, 0, "" or empty. After work
// ends, a drain also waits until no request has arrived for Grace, giving
// clients time to fetch results.
type BusyCheck struct {
	Endpoint string        `yaml:"endpoint"`
	Fields   []string      `yaml:"fields"`
	Interval time.Duration `yaml:"interval"`
	Grace    time.Duration `yaml:"grace"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		for _, p := range DefaultPaths {
			if _, err := os.Stat(p); err == nil {
				path = p
				break
			}
		}
		if path == "" {
			return nil, fmt.Errorf("no config file found (tried %s)", strings.Join(DefaultPaths, ", "))
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	baseDir := filepath.Dir(path)
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return parseJSON(data, baseDir)
	}
	return parse(data, baseDir)
}

// ParseJSON parses a JSON config. Durations are strings such as "10s", as in
// YAML.
func ParseJSON(data []byte) (*Config, error) { return parseJSON(data, "") }

func parseJSON(data []byte, baseDir string) (*Config, error) {
	// Check JSON syntax with encoding/json for precise errors, then decode
	// through the YAML path so defaults, durations and unknown-field checks
	// behave identically. JSON is valid YAML once re-serialized.
	var v any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		var se *json.SyntaxError
		if errors.As(err, &se) {
			line := bytes.Count(data[:se.Offset], []byte("\n")) + 1
			return nil, fmt.Errorf("parse config: line %d: %w", line, err)
		}
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if dec.More() {
		return nil, errors.New("parse config: trailing data after JSON object")
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return parse(out, baseDir)
}

// Parse parses a YAML config. Relative paths in it are resolved against the
// working directory.
func Parse(data []byte) (*Config, error) { return parse(data, "") }

func parse(data []byte, baseDir string) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.finalize(baseDir); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) finalize(baseDir string) error {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	if c.StartPort == 0 {
		c.StartPort = defaultStartPort
	}
	if c.HealthCheckTimeout == 0 {
		c.HealthCheckTimeout = defaultHealthCheckTimeout
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = defaultDrainTimeout
	}
	if len(c.Models) == 0 {
		return fmt.Errorf("config: no models defined")
	}
	b, ts := &c.Batch, &c.TimeShare
	if ts.Period != 0 {
		c.Warnings = append(c.Warnings, "timeShare.period is deprecated; read as batch.maxCycle")
		if b.MaxCycle == 0 && b.Cycle == 0 {
			b.MaxCycle = ts.Period
		}
	}
	if b.Cycle != 0 {
		c.Warnings = append(c.Warnings, "batch.cycle is deprecated (batches adapt); read as batch.maxCycle")
		if b.MaxCycle == 0 {
			b.MaxCycle = b.Cycle
		}
	}
	if ts.MinSlice != 0 {
		c.Warnings = append(c.Warnings, "timeShare.minSlice is deprecated and ignored")
	}
	if ts.Linger != nil {
		c.Warnings = append(c.Warnings, "timeShare.linger is deprecated; use batch.linger")
		if b.Linger == nil {
			b.Linger = ts.Linger
		}
	}
	if b.MaxCycle == 0 {
		b.MaxCycle = defaultMaxCycle
	}
	if b.MinCycle == 0 {
		b.MinCycle = min(defaultMinCycle, b.MaxCycle)
	}
	if b.MaxSwitchOverhead == 0 {
		b.MaxSwitchOverhead = defaultMaxSwitchOverhead
	}
	if b.Linger == nil {
		d := defaultLinger
		b.Linger = &d
	}
	if b.MinCycle < 0 || b.MaxCycle < 0 || *b.Linger < 0 {
		return fmt.Errorf("config: batch durations must not be negative")
	}
	if b.MinCycle > b.MaxCycle {
		return fmt.Errorf("config: batch.minCycle (%s) is longer than maxCycle (%s)", b.MinCycle, b.MaxCycle)
	}
	if b.MaxSwitchOverhead <= 0 || b.MaxSwitchOverhead >= 1 {
		return fmt.Errorf("config: batch.maxSwitchOverhead must be between 0 and 1")
	}
	if c.Jobs.Dir == "" {
		c.Jobs.Dir = defaultJobsDir
	}
	if !filepath.IsAbs(c.Jobs.Dir) {
		c.Jobs.Dir = filepath.Join(baseDir, c.Jobs.Dir)
	}
	abs, err := filepath.Abs(c.Jobs.Dir)
	if err != nil {
		return fmt.Errorf("config: jobs.dir: %w", err)
	}
	c.Jobs.Dir = abs
	if c.Jobs.ResultTTL == 0 {
		c.Jobs.ResultTTL = defaultResultTTL
	}
	if c.Jobs.MaxBodySize == 0 {
		c.Jobs.MaxBodySize = defaultMaxBodySize
	}
	if c.Jobs.ResultTTL < 0 {
		return fmt.Errorf("config: jobs.resultTTL must not be negative")
	}
	if c.Jobs.MaxInlineRef == 0 {
		c.Jobs.MaxInlineRef = defaultMaxInlineRef
	}
	a := &c.Artifacts
	if a.MaxSize == 0 {
		a.MaxSize = defaultArtifactsMaxSize
	}
	if a.TTL == 0 {
		a.TTL = c.Jobs.ResultTTL
	}
	if a.MaxUpload == 0 {
		a.MaxUpload = defaultArtifactsMaxUpload
	}
	if a.TTL < 0 {
		return fmt.Errorf("config: artifacts.ttl must not be negative")
	}

	// Sort names so port assignment is stable across restarts.
	names := make([]string, 0, len(c.Models))
	for name := range c.Models {
		names = append(names, name)
	}
	sort.Strings(names)

	c.aliases = make(map[string]string)
	for i, name := range names {
		m := c.Models[name]
		if m == nil {
			return fmt.Errorf("model %q: empty definition", name)
		}
		m.Name = name
		m.Port = c.StartPort + i
		if m.Disabled {
			delete(c.Models, name)
			continue
		}
		if m.StopTimeout == 0 {
			m.StopTimeout = defaultStopTimeout
		}
		if m.DrainTimeout == 0 {
			m.DrainTimeout = c.DrainTimeout
		}
		if b := m.BusyCheck; b != nil {
			if b.Endpoint == "" || len(b.Fields) == 0 {
				return fmt.Errorf("model %q: busyCheck needs endpoint and fields", name)
			}
			if b.Interval == 0 {
				b.Interval = defaultBusyInterval
			}
			if b.Grace == 0 {
				b.Grace = defaultBusyGrace
			}
		}
		if m.Share == 0 {
			m.Share = 1
		}
		if m.Share < 0 || m.Concurrency < 0 || m.MaxQueue < 0 || m.KeepJobs < 0 {
			return fmt.Errorf("model %q: share, concurrency, maxQueue and keepJobs must not be negative", name)
		}
		if m.QueueTimeout < 0 || m.JobTimeout < 0 || m.ResultTTL < 0 {
			return fmt.Errorf("model %q: queueTimeout, jobTimeout and resultTTL must not be negative", name)
		}
		if m.ResultTTL == 0 {
			m.ResultTTL = c.Jobs.ResultTTL
		}
		if m.MaxBodySize == 0 {
			m.MaxBodySize = c.Jobs.MaxBodySize
		}
		if m.Linger == nil {
			m.Linger = b.Linger
		} else if *m.Linger < 0 {
			return fmt.Errorf("model %q: linger must not be negative", name)
		}
		if m.DefaultEta == 0 {
			m.DefaultEta = defaultDefaultEta
		}
		if m.LoadTime == 0 {
			m.LoadTime = defaultLoadTime
		}
		if m.SyncMaxWait == nil {
			d := defaultSyncMaxWait
			m.SyncMaxWait = &d
		}
		if m.DefaultEta < 0 || m.LoadTime < 0 || *m.SyncMaxWait < 0 || m.MaxWait < 0 {
			return fmt.Errorf("model %q: defaultEta, loadTime, syncMaxWait and maxWait must not be negative", name)
		}
		if p := m.Progress; p != nil {
			if !strings.HasPrefix(p.Endpoint, "/") {
				return fmt.Errorf("model %q: progress needs an endpoint starting with /", name)
			}
			if p.Interval == 0 {
				p.Interval = defaultProgressInterval
			}
			if p.Interval < 0 {
				return fmt.Errorf("model %q: progress.interval must not be negative", name)
			}
		}
		if v := m.Validate; v != nil && !strings.HasPrefix(v.Endpoint, "/") {
			return fmt.Errorf("model %q: validate needs an endpoint starting with /", name)
		}
		if m.CheckEndpoint == "" {
			m.CheckEndpoint = defaultCheckEndpoint
		}
		if m.Proxy == "" {
			m.Proxy = "http://127.0.0.1:${PORT}"
		}
		m.Proxy = m.expand(m.Proxy)

		cmd := m.expand(m.Cmd)
		args, err := SplitCommand(cmd)
		if err != nil {
			return fmt.Errorf("model %q: cmd: %w", name, err)
		}
		if len(args) == 0 {
			return fmt.Errorf("model %q: cmd is required", name)
		}
		m.Args = args
		for i, e := range m.Env {
			m.Env[i] = m.expand(e)
		}

		for _, a := range m.Aliases {
			if _, ok := c.Models[a]; ok {
				return fmt.Errorf("model %q: alias %q collides with a model name", name, a)
			}
			if other, ok := c.aliases[a]; ok {
				return fmt.Errorf("model %q: alias %q already used by %q", name, a, other)
			}
			c.aliases[a] = name
		}
	}
	if len(c.Models) == 0 {
		return fmt.Errorf("config: all models are disabled")
	}
	return nil
}

func (m *ModelConfig) expand(s string) string {
	return strings.NewReplacer(
		"${PORT}", strconv.Itoa(m.Port),
		"${MODEL_ID}", m.Name,
	).Replace(s)
}

// Resolve returns the model for a name or alias.
func (c *Config) Resolve(name string) (*ModelConfig, bool) {
	if m, ok := c.Models[name]; ok {
		return m, true
	}
	if real, ok := c.aliases[name]; ok {
		return c.Models[real], true
	}
	return nil, false
}

// SplitCommand splits a command line into arguments, honoring single quotes,
// double quotes and backslash escapes. Newlines count as whitespace, so
// multi-line YAML blocks with trailing backslashes work.
func SplitCommand(s string) ([]string, error) {
	var (
		args    []string
		cur     strings.Builder
		inArg   bool
		quote   rune
		escaped bool
	)
	for _, r := range s {
		switch {
		case escaped:
			if r != '\n' {
				cur.WriteRune(r)
				inArg = true
			}
			escaped = false
		case r == '\\' && quote != '\'':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			inArg = true
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			if inArg {
				args = append(args, cur.String())
				cur.Reset()
				inArg = false
			}
		default:
			cur.WriteRune(r)
			inArg = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if inArg {
		args = append(args, cur.String())
	}
	return args, nil
}
