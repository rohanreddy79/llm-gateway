// Package config loads and validates gateway.yaml. Validation is strict on
// purpose: a gateway that starts with a half-correct routing table fails in
// production at 3am, so every cross-reference (route -> provider, key ->
// alias) is checked at load time.
package config

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole gateway.yaml.
type Config struct {
	Listen    string                    `yaml:"listen"`
	LogLevel  string                    `yaml:"log_level"`
	Providers map[string]ProviderConfig `yaml:"providers"`
	// Models maps a public alias to an ordered fallback chain of
	// "provider:model" targets.
	Models  map[string][]string  `yaml:"models"`
	Keys    map[string]KeyConfig `yaml:"keys"`
	Cache   CacheConfig          `yaml:"cache"`
	Retry   RetryConfig          `yaml:"retry"`
	Breaker BreakerConfig        `yaml:"breaker"`
	// Prices are USD per million tokens, keyed by upstream model id. They are
	// merged over DefaultPrices.
	Prices map[string]Price `yaml:"prices"`

	// Routes is Models parsed into typed targets; filled by Validate.
	Routes map[string][]Target `yaml:"-"`
}

// ProviderConfig describes one upstream.
type ProviderConfig struct {
	Type           string        `yaml:"type"`
	BaseURL        string        `yaml:"base_url"`
	APIKeyEnv      string        `yaml:"api_key_env"`
	Timeout        time.Duration `yaml:"timeout"`
	MaxConcurrency int           `yaml:"max_concurrency"`
	// DefaultMaxTokens is used when a request carries no max_tokens; the
	// Anthropic Messages API requires one.
	DefaultMaxTokens int `yaml:"default_max_tokens"`
	// Mock-only knobs.
	Latency    time.Duration `yaml:"latency"`
	Text       string        `yaml:"text"`
	FailEvery  int           `yaml:"fail_every"`
	FailStatus int           `yaml:"fail_status"`
}

// Target is one hop of a fallback chain.
type Target struct {
	Provider string
	Model    string
}

func (t Target) String() string { return t.Provider + ":" + t.Model }

// KeyConfig is one virtual API key. RPM/TPM of 0 mean unlimited; an empty
// Models list allows every alias.
type KeyConfig struct {
	Name   string   `yaml:"name"`
	RPM    int      `yaml:"rpm"`
	TPM    int      `yaml:"tpm"`
	Models []string `yaml:"models"`
	Admin  bool     `yaml:"admin"`
}

// AllowsModel reports whether the key may use the alias.
func (k KeyConfig) AllowsModel(alias string) bool {
	if len(k.Models) == 0 {
		return true
	}
	for _, m := range k.Models {
		if m == alias {
			return true
		}
	}
	return false
}

// CacheConfig controls the response cache; MaxEntries 0 disables it.
type CacheConfig struct {
	TTL        time.Duration `yaml:"ttl"`
	MaxEntries int           `yaml:"max_entries"`
}

// RetryConfig controls per-target retries.
type RetryConfig struct {
	MaxAttempts int           `yaml:"max_attempts"`
	BaseBackoff time.Duration `yaml:"base_backoff"`
	MaxBackoff  time.Duration `yaml:"max_backoff"`
}

// BreakerConfig controls the per-provider circuit breaker.
type BreakerConfig struct {
	Failures int           `yaml:"failures"`
	OpenFor  time.Duration `yaml:"open_for"`
}

// Price is USD per million tokens.
type Price struct {
	Input  float64 `yaml:"input"`
	Output float64 `yaml:"output"`
}

// Provider types.
const (
	TypeOpenAI    = "openai"
	TypeAnthropic = "anthropic"
	TypeMock      = "mock"
)

// Default returns a Config with every default applied and no providers.
func Default() Config {
	return Config{
		Listen:   ":8080",
		LogLevel: "info",
		Cache:    CacheConfig{TTL: 5 * time.Minute, MaxEntries: 1000},
		Retry:    RetryConfig{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond, MaxBackoff: 2 * time.Second},
		Breaker:  BreakerConfig{Failures: 5, OpenFor: 30 * time.Second},
		Prices:   DefaultPrices(),
	}
}

// Load reads, parses and validates a YAML file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes YAML over the defaults and validates the result.
func Parse(b []byte) (*Config, error) {
	cfg := Default()
	var in Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&in); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	merge(&cfg, &in)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// merge copies the set fields of in over the defaults in dst.
func merge(dst, in *Config) {
	if in.Listen != "" {
		dst.Listen = in.Listen
	}
	if in.LogLevel != "" {
		dst.LogLevel = in.LogLevel
	}
	dst.Providers = in.Providers
	dst.Models = in.Models
	dst.Keys = in.Keys
	if in.Cache.TTL != 0 {
		dst.Cache.TTL = in.Cache.TTL
	}
	if in.Cache.MaxEntries != 0 {
		dst.Cache.MaxEntries = in.Cache.MaxEntries
	}
	if in.Retry.MaxAttempts != 0 {
		dst.Retry.MaxAttempts = in.Retry.MaxAttempts
	}
	if in.Retry.BaseBackoff != 0 {
		dst.Retry.BaseBackoff = in.Retry.BaseBackoff
	}
	if in.Retry.MaxBackoff != 0 {
		dst.Retry.MaxBackoff = in.Retry.MaxBackoff
	}
	if in.Breaker.Failures != 0 {
		dst.Breaker.Failures = in.Breaker.Failures
	}
	if in.Breaker.OpenFor != 0 {
		dst.Breaker.OpenFor = in.Breaker.OpenFor
	}
	for model, p := range in.Prices {
		dst.Prices[model] = p
	}
}

// Validate applies per-provider defaults, parses routes and checks every
// cross-reference. It is safe to call on a hand-built Config.
func (c *Config) Validate() error {
	var errs []string
	fail := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }

	if c.Listen == "" {
		fail("listen must not be empty")
	}
	if len(c.Providers) == 0 {
		fail("at least one provider is required")
	}
	for name, p := range c.Providers {
		switch p.Type {
		case TypeOpenAI:
			if p.BaseURL == "" {
				p.BaseURL = "https://api.openai.com/v1"
			}
			if p.APIKeyEnv == "" {
				fail("provider %q: api_key_env is required for type openai", name)
			}
		case TypeAnthropic:
			if p.BaseURL == "" {
				p.BaseURL = "https://api.anthropic.com"
			}
			if p.APIKeyEnv == "" {
				fail("provider %q: api_key_env is required for type anthropic", name)
			}
		case TypeMock:
		case "":
			fail("provider %q: type is required", name)
		default:
			fail("provider %q: unknown type %q (want openai, anthropic or mock)", name, p.Type)
		}
		if p.Timeout == 0 {
			p.Timeout = 60 * time.Second
		}
		if p.MaxConcurrency == 0 {
			p.MaxConcurrency = 64
		}
		if p.DefaultMaxTokens == 0 {
			p.DefaultMaxTokens = 1024
		}
		if p.Timeout < 0 || p.MaxConcurrency < 0 || p.DefaultMaxTokens < 0 || p.Latency < 0 || p.FailEvery < 0 {
			fail("provider %q: negative values are not allowed", name)
		}
		c.Providers[name] = p
	}

	if len(c.Models) == 0 {
		fail("at least one model alias is required")
	}
	c.Routes = make(map[string][]Target, len(c.Models))
	for alias, chain := range c.Models {
		if len(chain) == 0 {
			fail("model %q: fallback chain is empty", alias)
			continue
		}
		targets := make([]Target, 0, len(chain))
		for _, s := range chain {
			prov, model, ok := strings.Cut(s, ":")
			if !ok || prov == "" || model == "" {
				fail("model %q: target %q must be provider:model", alias, s)
				continue
			}
			if _, exists := c.Providers[prov]; !exists {
				fail("model %q: target %q references unknown provider %q", alias, s, prov)
				continue
			}
			targets = append(targets, Target{Provider: prov, Model: model})
		}
		c.Routes[alias] = targets
	}

	if len(c.Keys) == 0 {
		fail("at least one virtual key is required")
	}
	for key, k := range c.Keys {
		if k.Name == "" {
			fail("key %q: name is required", redact(key))
		}
		if k.RPM < 0 || k.TPM < 0 {
			fail("key %q: rpm and tpm must be >= 0", redact(key))
		}
		for _, m := range k.Models {
			if _, ok := c.Models[m]; !ok {
				fail("key %q: allowed model %q is not a configured alias", redact(key), m)
			}
		}
	}

	if c.Cache.TTL < 0 || c.Cache.MaxEntries < 0 {
		fail("cache: ttl and max_entries must be >= 0")
	}
	if c.Retry.MaxAttempts < 1 {
		fail("retry: max_attempts must be >= 1")
	}
	if c.Retry.BaseBackoff <= 0 || c.Retry.MaxBackoff < c.Retry.BaseBackoff {
		fail("retry: base_backoff must be > 0 and max_backoff >= base_backoff")
	}
	if c.Breaker.Failures < 1 || c.Breaker.OpenFor <= 0 {
		fail("breaker: failures must be >= 1 and open_for > 0")
	}
	for model, p := range c.Prices {
		if p.Input < 0 || p.Output < 0 {
			fail("prices: %q must be >= 0", model)
		}
	}

	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("invalid config:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// Aliases returns the model aliases in sorted order.
func (c *Config) Aliases() []string {
	out := make([]string, 0, len(c.Routes))
	for a := range c.Routes {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// redact keeps the first few characters of a virtual key for error messages.
func redact(key string) string {
	if len(key) <= 6 {
		return key
	}
	return key[:6] + "..."
}
