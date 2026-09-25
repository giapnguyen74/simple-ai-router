package fakeserver

import (
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
)

// Model describes one fake model in a test config.
type Model struct {
	Env   []string // extra FAKE_* settings
	Extra string   // extra YAML lines for the model, e.g. "ttl: 1s"
}

// Config builds a config whose models all launch the current test binary as
// a fake server. globals is extra top-level YAML.
func Config(t testing.TB, globals string, models map[string]Model) *config.Config {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(models))
	for n := range models {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "startPort: %d\nhealthCheckTimeout: 5s\n%s\nmodels:\n", freePortRange(t, len(models)), globals)
	for _, n := range names {
		m := models[n]
		env := append([]string{"FAKE_SERVER=1", "FAKE_PORT=${PORT}", "FAKE_NAME=" + n}, m.Env...)
		fmt.Fprintf(&b, "  %s:\n    cmd: %q\n    stopTimeout: 1s\n    env:\n", n, exe)
		for _, e := range env {
			fmt.Fprintf(&b, "      - %q\n", e)
		}
		for _, line := range strings.Split(m.Extra, "\n") {
			if line != "" {
				fmt.Fprintf(&b, "    %s\n", line)
			}
		}
	}
	cfg, err := config.Parse([]byte(b.String()))
	if err != nil {
		t.Fatalf("%v\n%s", err, b.String())
	}
	return cfg
}

// freePortRange finds n consecutive ports that are currently free.
func freePortRange(t testing.TB, n int) int {
	t.Helper()
	for range 100 {
		base := 20000 + rand.IntN(30000)
		ok := true
		for i := range n {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				ok = false
				break
			}
			l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("no free port range")
	return 0
}
