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
)

type Config struct {
	Listen             string        `yaml:"listen"`
	StartPort          int           `yaml:"startPort"`
	HealthCheckTimeout time.Duration `yaml:"healthCheckTimeout"`
	// DrainTimeout bounds how long a swap waits for in-flight requests on
	// the outgoing model before killing it anyway.
	DrainTimeout time.Duration           `yaml:"drainTimeout"`
	Models       map[string]*ModelConfig `yaml:"models"`

	// aliases maps an alias to its real model name. Built by Load.
	aliases map[string]string
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
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return ParseJSON(data)
	}
	return Parse(data)
}

// ParseJSON parses a JSON config. Durations are strings such as "10s", as in
// YAML.
func ParseJSON(data []byte) (*Config, error) {
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
	return Parse(out)
}

// Parse parses a YAML config.
func Parse(data []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.finalize(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) finalize() error {
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
