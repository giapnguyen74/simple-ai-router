package config

import "testing"

func TestExampleConfigsParse(t *testing.T) {
	for _, p := range []string{"../../config.example.yaml", "../../config.example.json"} {
		if _, err := Load(p); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}
