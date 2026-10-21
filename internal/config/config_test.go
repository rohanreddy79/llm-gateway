package config

import (
	"strings"
	"testing"
	"time"
)

const validYAML = `
listen: ":19080"
providers:
  mock:
    type: mock
    latency: 2ms
  anthropic:
    type: anthropic
    api_key_env: ANTHROPIC_API_KEY
models:
  fast: [mock:mock-fast]
  claude: [anthropic:claude-opus-5-5, mock:mock-fast]
keys:
  gw-demo-key:
    name: demo
    rpm: 600
    tpm: 100000
    models: [fast, claude]
    admin: true
cache:
  ttl: 30s
retry:
  max_attempts: 2
prices:
  mock-fast: {input: 0.5, output: 1.5}
`

func TestParseValid(t *testing.T) {
	cfg, err := Parse([]byte(validYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Listen != ":19080" {
		t.Errorf("listen = %q", cfg.Listen)
	}
	if got := cfg.Routes["claude"]; len(got) != 2 || got[0] != (Target{"anthropic", "claude-opus-5-5"}) || got[1] != (Target{"mock", "mock-fast"}) {
		t.Errorf("routes[claude] = %v", got)
	}
	if cfg.Providers["anthropic"].BaseURL != "https://api.anthropic.com" {
		t.Errorf("anthropic base_url default missing: %q", cfg.Providers["anthropic"].BaseURL)
	}
	if cfg.Providers["mock"].Latency != 2*time.Millisecond {
		t.Errorf("mock latency = %v", cfg.Providers["mock"].Latency)
	}
	if cfg.Providers["mock"].Timeout != 60*time.Second || cfg.Providers["mock"].MaxConcurrency != 64 {
		t.Errorf("provider defaults not applied: %+v", cfg.Providers["mock"])
	}
	// Partial overrides keep the other defaults.
	if cfg.Cache.TTL != 30*time.Second || cfg.Cache.MaxEntries != 1000 {
		t.Errorf("cache = %+v", cfg.Cache)
	}
	if cfg.Retry.MaxAttempts != 2 || cfg.Retry.BaseBackoff != 100*time.Millisecond {
		t.Errorf("retry = %+v", cfg.Retry)
	}
	if cfg.Breaker.Failures != 5 || cfg.Breaker.OpenFor != 30*time.Second {
		t.Errorf("breaker = %+v", cfg.Breaker)
	}
	// Prices: config entries merge over the built-in Claude table.
	if _, ok := cfg.Prices["claude-opus-5-5"]; !ok {
		t.Error("default Claude price missing")
	}
	if usd, priced := cfg.Cost("mock-fast", 1_000_000, 1_000_000); !priced || usd != 2.0 {
		t.Errorf("Cost(mock-fast) = %v, %v", usd, priced)
	}
	if usd, priced := cfg.Cost("unknown-model", 10, 10); priced || usd != 0 {
		t.Errorf("Cost(unknown) = %v, %v", usd, priced)
	}
	if got := cfg.Aliases(); len(got) != 2 || got[0] != "claude" || got[1] != "fast" {
		t.Errorf("Aliases = %v", got)
	}
	k := cfg.Keys["gw-demo-key"]
	if !k.AllowsModel("fast") || k.AllowsModel("other") || !k.Admin {
		t.Errorf("key = %+v", k)
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"unknown provider type", `
providers: {p: {type: weird}}
models: {m: [p:x]}
keys: {k: {name: k}}`, `unknown type "weird"`},
		{"missing api_key_env", `
providers: {p: {type: openai}}
models: {m: [p:x]}
keys: {k: {name: k}}`, "api_key_env is required"},
		{"route to unknown provider", `
providers: {p: {type: mock}}
models: {m: [nope:x]}
keys: {k: {name: k}}`, `unknown provider "nope"`},
		{"malformed target", `
providers: {p: {type: mock}}
models: {m: [justmodel]}
keys: {k: {name: k}}`, "must be provider:model"},
		{"key allows unknown alias", `
providers: {p: {type: mock}}
models: {m: [p:x]}
keys: {k: {name: k, models: [ghost]}}`, `allowed model "ghost"`},
		{"key without name", `
providers: {p: {type: mock}}
models: {m: [p:x]}
keys: {k: {rpm: 1}}`, "name is required"},
		{"no keys", `
providers: {p: {type: mock}}
models: {m: [p:x]}`, "at least one virtual key"},
		{"bad retry", `
providers: {p: {type: mock}}
models: {m: [p:x]}
keys: {k: {name: k}}
retry: {max_attempts: 2, base_backoff: 5s, max_backoff: 1s}`, "max_backoff >= base_backoff"},
		{"negative price", `
providers: {p: {type: mock}}
models: {m: [p:x]}
keys: {k: {name: k}}
prices: {x: {input: -1}}`, "must be >= 0"},
		{"unknown field", `
providers: {p: {type: mock}}
models: {m: [p:x]}
keys: {k: {name: k}}
cach: {ttl: 1s}`, "field cach not found"},
		{"invalid yaml", `providers: [`, "parse config"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateReportsAllErrors(t *testing.T) {
	_, err := Parse([]byte(`
providers: {p: {type: weird}}
models: {m: [ghost:x]}
keys: {k: {}}
`))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"unknown type", "unknown provider", "name is required"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%s", want, err)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/gateway.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}
}
