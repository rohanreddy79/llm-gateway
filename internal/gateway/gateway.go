// Package gateway wires config, providers and the resilience layers into an
// http.Handler that speaks the OpenAI chat-completions API.
//
// Request flow for POST /v1/chat/completions:
//
//	request id + logging -> auth (virtual key) -> per-key rate limit ->
//	decode + validate -> model allowlist -> cache lookup -> router
//	(fallback chain -> breaker -> concurrency cap -> timeout -> retry) ->
//	provider -> cache store -> usage + metrics
package gateway

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/cache"
	"github.com/rohanreddy79/llm-gateway/internal/config"
	"github.com/rohanreddy79/llm-gateway/internal/metrics"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
	"github.com/rohanreddy79/llm-gateway/internal/provider/anthropic"
	"github.com/rohanreddy79/llm-gateway/internal/provider/mock"
	"github.com/rohanreddy79/llm-gateway/internal/provider/openai"
	"github.com/rohanreddy79/llm-gateway/internal/ratelimit"
	"github.com/rohanreddy79/llm-gateway/internal/retry"
	"github.com/rohanreddy79/llm-gateway/internal/router"
	"github.com/rohanreddy79/llm-gateway/internal/usage"
)

// Options are the injection points; every field is optional.
type Options struct {
	Logger *slog.Logger
	Now    func() time.Time
	// Providers overrides construction for the named providers, so tests can
	// inject mocks with failure hooks.
	Providers map[string]provider.Provider
	// HTTPClient is used for the real upstreams.
	HTTPClient *http.Client
	// Getenv resolves api_key_env; defaults to os.Getenv.
	Getenv func(string) string
}

// Gateway is the assembled service.
type Gateway struct {
	cfg     *config.Config
	router  *router.Router
	limiter *ratelimit.Limiter
	cache   *cache.Cache
	usage   *usage.Tracker
	metrics *metrics.Metrics
	log     *slog.Logger
	now     func() time.Time
	started time.Time
	handler http.Handler
}

// New assembles a Gateway from a validated config.
func New(cfg *config.Config, opts Options) (*Gateway, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Getenv == nil {
		opts.Getenv = os.Getenv
	}
	if opts.HTTPClient == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConnsPerHost = 100
		opts.HTTPClient = &http.Client{Transport: t}
	}
	m := metrics.New()
	entries, err := buildProviders(cfg, opts)
	if err != nil {
		return nil, err
	}
	rt, err := router.New(router.Options{
		Routes:    cfg.Routes,
		Providers: entries,
		Retry:     retry.Policy{MaxAttempts: cfg.Retry.MaxAttempts, BaseBackoff: cfg.Retry.BaseBackoff, MaxBackoff: cfg.Retry.MaxBackoff},
		Breaker:   cfg.Breaker,
		Metrics:   m,
		Logger:    opts.Logger,
		Now:       opts.Now,
	})
	if err != nil {
		return nil, err
	}
	g := &Gateway{
		cfg:     cfg,
		router:  rt,
		limiter: ratelimit.New(opts.Now),
		cache:   cache.New(cfg.Cache.TTL, cfg.Cache.MaxEntries, opts.Now),
		usage:   usage.New(cfg.Cost),
		metrics: m,
		log:     opts.Logger,
		now:     opts.Now,
		started: opts.Now(),
	}
	g.handler = g.routes()
	return g, nil
}

func buildProviders(cfg *config.Config, opts Options) (map[string]router.Entry, error) {
	entries := make(map[string]router.Entry, len(cfg.Providers))
	for name, pc := range cfg.Providers {
		p, ok := opts.Providers[name]
		if !ok {
			var err error
			if p, err = newProvider(name, pc, opts); err != nil {
				return nil, err
			}
		}
		entries[name] = router.Entry{Provider: p, Timeout: pc.Timeout, MaxConcurrency: pc.MaxConcurrency}
	}
	return entries, nil
}

func newProvider(name string, pc config.ProviderConfig, opts Options) (provider.Provider, error) {
	switch pc.Type {
	case config.TypeMock:
		return mock.New(name, mock.Options{Latency: pc.Latency, Text: pc.Text, FailEvery: pc.FailEvery, FailStatus: pc.FailStatus, Now: opts.Now}), nil
	case config.TypeOpenAI, config.TypeAnthropic:
		key := opts.Getenv(pc.APIKeyEnv)
		if key == "" {
			return nil, fmt.Errorf("provider %q: environment variable %s is not set", name, pc.APIKeyEnv)
		}
		if pc.Type == config.TypeOpenAI {
			return openai.New(name, openai.Options{BaseURL: pc.BaseURL, APIKey: key, HTTPClient: opts.HTTPClient}), nil
		}
		return anthropic.New(name, anthropic.Options{BaseURL: pc.BaseURL, APIKey: key, DefaultMaxTokens: pc.DefaultMaxTokens, HTTPClient: opts.HTTPClient, Now: opts.Now}), nil
	default:
		return nil, fmt.Errorf("provider %q: unknown type %q", name, pc.Type)
	}
}

// Handler is the fully wrapped HTTP handler.
func (g *Gateway) Handler() http.Handler { return g.handler }

// Router exposes the routing layer (breaker states) for health and tests.
func (g *Gateway) Router() *router.Router { return g.router }

// Metrics exposes the instruments for tests.
func (g *Gateway) Metrics() *metrics.Metrics { return g.metrics }

func (g *Gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", g.handleHealth)
	mux.Handle("GET /metrics", g.metrics.Handler())
	mux.Handle("POST /v1/chat/completions", g.auth(g.rateLimit(http.HandlerFunc(g.handleChat))))
	mux.Handle("GET /v1/models", g.auth(http.HandlerFunc(g.handleModels)))
	mux.Handle("GET /admin/usage", g.auth(g.adminOnly(http.HandlerFunc(g.handleUsage))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "not_found", fmt.Sprintf("unknown route %s %s", r.Method, r.URL.Path))
	})
	return g.requestID(g.logging(mux))
}
