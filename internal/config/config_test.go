package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	cfg, err := Parse([]byte(`
startPort: 9000
models:
  zeta:
    cmd: |
      llama-server -m "/models/my model.gguf" \
        --port ${PORT} --alias ${MODEL_ID}
    aliases: [gpt-4o]
    ttl: 5m
  alpha:
    cmd: server --port ${PORT}
    proxy: http://10.0.0.5:${PORT}
    env: ["X=${PORT}"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":8080" || cfg.HealthCheckTimeout != 120*time.Second || cfg.DrainTimeout != 5*time.Minute {
		t.Fatalf("defaults not applied: %+v", cfg)
	}

	alpha, zeta := cfg.Models["alpha"], cfg.Models["zeta"]
	if alpha.Port != 9000 || zeta.Port != 9001 {
		t.Fatalf("ports: alpha=%d zeta=%d", alpha.Port, zeta.Port)
	}
	if alpha.Proxy != "http://10.0.0.5:9000" || alpha.Env[0] != "X=9000" {
		t.Fatalf("alpha macros not expanded: %+v", alpha)
	}
	if zeta.Proxy != "http://127.0.0.1:9001" || zeta.CheckEndpoint != "/health" || zeta.StopTimeout != 10*time.Second {
		t.Fatalf("zeta defaults: %+v", zeta)
	}
	wantArgs := []string{"llama-server", "-m", "/models/my model.gguf", "--port", "9001", "--alias", "zeta"}
	if !reflect.DeepEqual(zeta.Args, wantArgs) {
		t.Fatalf("args = %q", zeta.Args)
	}
	if m, ok := cfg.Resolve("gpt-4o"); !ok || m != zeta {
		t.Fatal("alias not resolved")
	}
}

func TestParseErrors(t *testing.T) {
	for _, tc := range []struct{ name, yaml, want string }{
		{"no models", `listen: ":1"`, "no models"},
		{"empty cmd", "models:\n  a:\n    ttl: 1s", "cmd is required"},
		{"unknown field", "models:\n  a:\n    cmd: x\n    bogus: 1", "bogus"},
		{"alias collides", "models:\n  a:\n    cmd: x\n    aliases: [b]\n  b:\n    cmd: y", "collides"},
		{"bad quote", "models:\n  a:\n    cmd: x \"y", "unterminated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestSplitCommand(t *testing.T) {
	for in, want := range map[string][]string{
		`a b  c`:        {"a", "b", "c"},
		`a 'b c' "d e"`: {"a", "b c", "d e"},
		`a b\ c`:        {"a", "b c"},
		"a \\\n  b":     {"a", "b"},
		`a "" b`:        {"a", "", "b"},
		`a '\n' "x\"y"`: {"a", `\n`, `x"y`},
		`--flag="x y"`:  {"--flag=x y"},
	} {
		got, err := SplitCommand(in)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("SplitCommand(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestParseJSON(t *testing.T) {
	cfg, err := ParseJSON([]byte("{\n\t\"startPort\": 9000,\n\t\"drainTimeout\": \"30s\",\n\t\"models\": {\n\t\t\"a\": {\n\t\t\t\"cmd\": \"server --port ${PORT} --tpl '<a & b>'\",\n\t\t\t\"ttl\": \"5m\",\n\t\t\t\"aliases\": [\"gpt-4o\"],\n\t\t\t\"env\": [\"X=${PORT}\"]\n\t\t}\n\t}\n}\n"))
	if err != nil {
		t.Fatal(err)
	}
	a := cfg.Models["a"]
	if cfg.DrainTimeout != 30*time.Second || a.TTL != 5*time.Minute || a.Port != 9000 || a.Env[0] != "X=9000" {
		t.Fatalf("unexpected config: %+v %+v", cfg, a)
	}
	if want := []string{"server", "--port", "9000", "--tpl", "<a & b>"}; !reflect.DeepEqual(a.Args, want) {
		t.Fatalf("args = %q", a.Args)
	}
	if m, ok := cfg.Resolve("gpt-4o"); !ok || m != a {
		t.Fatal("alias not resolved")
	}
}

func TestParseJSONErrors(t *testing.T) {
	for _, tc := range []struct{ name, json, want string }{
		{"syntax", "{\n  \"models\": {,}\n}", "line 2"},
		{"unknown field", `{"models":{"a":{"cmd":"x","bogus":1}}}`, "bogus"},
		{"bad duration", `{"models":{"a":{"cmd":"x","ttl":"soon"}}}`, "soon"},
		{"trailing", `{"models":{"a":{"cmd":"x"}}} {}`, "trailing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseJSON([]byte(tc.json))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadPicksFormatByExtension(t *testing.T) {
	dir := t.TempDir()
	// Valid JSON but indented with tabs, which YAML rejects: proves the JSON path is used.
	jsonPath := filepath.Join(dir, "c.json")
	os.WriteFile(jsonPath, []byte("{\n\t\"models\": {\"a\": {\"cmd\": \"x\"}}\n}"), 0o644)
	if _, err := Load(jsonPath); err != nil {
		t.Fatalf("json: %v", err)
	}
	yamlPath := filepath.Join(dir, "c.yaml")
	os.WriteFile(yamlPath, []byte("models:\n  a:\n    cmd: x\n"), 0o644)
	if _, err := Load(yamlPath); err != nil {
		t.Fatalf("yaml: %v", err)
	}
}

func TestLoadDefaultPaths(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "no config file found") {
		t.Fatalf("got %v", err)
	}
	os.WriteFile("config.json", []byte(`{"models":{"a":{"cmd":"x"}}}`), 0o644)
	if _, err := Load(""); err != nil {
		t.Fatal(err)
	}
}
