// Package router picks an upstream for a model alias and drives the
// resilience layers in order: fallback chain -> per-provider circuit breaker
// -> per-provider concurrency cap -> per-attempt timeout -> retry.
//
// The breaker sits inside the retry loop on purpose: every failed attempt
// counts towards opening it, and once it opens the remaining retries are
// skipped and the request falls through to the next target immediately.
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/breaker"
	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/config"
	"github.com/rohanreddy79/llm-gateway/internal/metrics"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
	"github.com/rohanreddy79/llm-gateway/internal/retry"
)

// ErrUnknownModel is returned for an alias with no route.
var ErrUnknownModel = errors.New("unknown model alias")

// Entry describes one configured provider.
type Entry struct {
	Provider       provider.Provider
	Timeout        time.Duration
	MaxConcurrency int
}

// Options configures a Router.
type Options struct {
	Routes    map[string][]config.Target
	Providers map[string]Entry
	Retry     retry.Policy
	Breaker   config.BreakerConfig
	Metrics   *metrics.Metrics
	Logger    *slog.Logger
	Now       func() time.Time
}

// Result says which target served the request and how hard it was.
type Result struct {
	Provider  string
	Model     string
	Attempts  int
	Fallbacks int
}

type entry struct {
	name    string
	p       provider.Provider
	timeout time.Duration
	sem     chan struct{}
	br      *breaker.Breaker
}

// Router is safe for concurrent use.
type Router struct {
	routes  map[string][]config.Target
	entries map[string]*entry
	retry   retry.Policy
	metrics *metrics.Metrics
	log     *slog.Logger
}

// New builds a router; every target's provider must be present in Providers.
func New(opts Options) (*Router, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	r := &Router{routes: opts.Routes, entries: make(map[string]*entry), retry: opts.Retry, metrics: opts.Metrics, log: opts.Logger}
	for name, e := range opts.Providers {
		name := name
		limit := e.MaxConcurrency
		if limit <= 0 {
			limit = 64
		}
		var onChange func(breaker.State)
		if opts.Metrics != nil {
			onChange = func(s breaker.State) { opts.Metrics.BreakerState.WithLabelValues(name).Set(float64(s)) }
			opts.Metrics.BreakerState.WithLabelValues(name).Set(0)
		}
		r.entries[name] = &entry{
			name:    name,
			p:       e.Provider,
			timeout: e.Timeout,
			sem:     make(chan struct{}, limit),
			br:      breaker.New(opts.Breaker.Failures, opts.Breaker.OpenFor, opts.Now, onChange),
		}
	}
	for alias, chain := range opts.Routes {
		for _, t := range chain {
			if _, ok := r.entries[t.Provider]; !ok {
				return nil, fmt.Errorf("route %q: provider %q is not configured", alias, t.Provider)
			}
		}
	}
	return r, nil
}

// BreakerState exposes a provider's circuit state (for /healthz and tests).
func (r *Router) BreakerState(providerName string) breaker.State {
	if e, ok := r.entries[providerName]; ok {
		return e.br.State()
	}
	return breaker.Closed
}

// Providers lists the configured provider names.
func (r *Router) Providers() []string {
	out := make([]string, 0, len(r.entries))
	for name := range r.entries {
		out = append(out, name)
	}
	return out
}

// attemptFunc performs one upstream call inside a held concurrency slot.
// Returning ownsRelease=true means the attempt will call release itself
// later (a stream that is still being consumed); otherwise the router
// releases the slot as soon as the attempt returns.
type attemptFunc func(ctx context.Context, e *entry, target config.Target, release func()) (ownsRelease bool, err error)

// Complete runs a non-streaming completion through the chain for alias.
func (r *Router) Complete(ctx context.Context, alias string, req *chat.Request) (*chat.Response, Result, error) {
	var resp *chat.Response
	res, err := r.route(ctx, alias, func(ctx context.Context, e *entry, target config.Target, _ func()) (bool, error) {
		tr := *req
		tr.Model = target.Model
		var err error
		resp, err = e.p.Complete(ctx, &tr)
		return false, err
	})
	return resp, res, err
}

// Stream runs a streaming completion. The concurrency slot and attempt
// timeout are held until the returned channel is drained or ctx ends.
func (r *Router) Stream(ctx context.Context, alias string, req *chat.Request) (<-chan provider.Chunk, Result, error) {
	var out <-chan provider.Chunk
	res, err := r.route(ctx, alias, func(ctx context.Context, e *entry, target config.Target, release func()) (bool, error) {
		tr := *req
		tr.Model = target.Model
		ch, err := e.p.Stream(ctx, &tr)
		if err != nil {
			return false, err
		}
		wrapped := make(chan provider.Chunk, 16)
		go func() {
			defer release()
			defer close(wrapped)
			for c := range ch {
				select {
				case wrapped <- c:
				case <-ctx.Done():
					return
				}
			}
		}()
		out = wrapped
		return true, nil
	})
	return out, res, err
}

// route walks the fallback chain. Non-retryable upstream errors (a 400 from
// a healthy provider) are returned immediately: the same request would fail
// on the next provider too, and the caller must see the real message.
func (r *Router) route(ctx context.Context, alias string, attempt attemptFunc) (Result, error) {
	chain, ok := r.routes[alias]
	if !ok {
		return Result{}, ErrUnknownModel
	}
	var res Result
	var lastErr error
	for i, target := range chain {
		e := r.entries[target.Provider]
		res.Provider, res.Model = target.Provider, target.Model
		attempts, err := r.tryTarget(ctx, e, target, attempt)
		res.Attempts += attempts
		if err == nil {
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil || !shouldFallback(err) {
			return res, err
		}
		if i < len(chain)-1 {
			res.Fallbacks++
			r.log.Warn("falling back", "model", alias, "from", target.String(), "to", chain[i+1].String(), "error", err)
		}
	}
	return res, lastErr
}

func shouldFallback(err error) bool {
	return errors.Is(err, breaker.ErrOpen) || provider.IsRetryable(err)
}

// tryTarget runs the retry loop for one target; parent is the caller's
// context, each attempt gets its own timeout-bounded child.
func (r *Router) tryTarget(parent context.Context, e *entry, target config.Target, attempt attemptFunc) (int, error) {
	return r.retry.Do(parent, func(parent context.Context, n int) error {
		if !e.br.Allow() {
			r.count(e.name, "circuit_open")
			return breaker.ErrOpen
		}
		select {
		case e.sem <- struct{}{}:
		case <-parent.Done():
			return parent.Err()
		}
		if r.metrics != nil {
			r.metrics.InFlight.WithLabelValues(e.name).Inc()
		}
		actx, cancel := context.WithTimeout(parent, e.timeout)
		var once sync.Once
		release := func() {
			once.Do(func() {
				cancel()
				<-e.sem
				if r.metrics != nil {
					r.metrics.InFlight.WithLabelValues(e.name).Dec()
				}
			})
		}
		owns, err := attempt(actx, e, target, release)
		if !owns {
			release()
		}
		r.feedBreaker(parent, e, err)
		if err != nil && n > 0 {
			r.log.Debug("attempt failed", "provider", e.name, "model", target.Model, "attempt", n+1, "error", err)
		}
		return err
	})
}

// feedBreaker records the attempt outcome. A healthy upstream rejecting a bad
// request is a success for breaker purposes; a client that went away is no
// signal at all.
func (r *Router) feedBreaker(parent context.Context, e *entry, err error) {
	switch {
	case err == nil:
		e.br.Success()
		r.count(e.name, "success")
	case parent.Err() != nil:
	case provider.IsRetryable(err):
		e.br.Failure()
		r.count(e.name, "retryable_error")
	default:
		e.br.Success()
		r.count(e.name, "client_error")
	}
}

func (r *Router) count(providerName, outcome string) {
	if r.metrics != nil {
		r.metrics.UpstreamAttempts.WithLabelValues(providerName, outcome).Inc()
	}
}
