// Package metrics defines the gateway's Prometheus instruments on a private
// registry, so several gateways can live in one process (tests) without
// duplicate-registration panics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every instrument. Label "key" is always the virtual key's
// configured name, never the secret.
type Metrics struct {
	Registry         *prometheus.Registry
	Requests         *prometheus.CounterVec
	Duration         *prometheus.HistogramVec
	Tokens           *prometheus.CounterVec
	Cost             *prometheus.CounterVec
	BreakerState     *prometheus.GaugeVec
	CacheEvents      *prometheus.CounterVec
	RateLimited      *prometheus.CounterVec
	UpstreamAttempts *prometheus.CounterVec
	InFlight         *prometheus.GaugeVec
}

// New creates and registers the instruments.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Chat completion requests by virtual key, model alias, serving provider and HTTP status.",
		}, []string{"key", "model", "provider", "status"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "End-to-end request latency as seen by the gateway.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"model", "provider"}),
		Tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_tokens_total",
			Help: "Tokens consumed upstream, by direction (input or output).",
		}, []string{"key", "model", "direction"}),
		Cost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_cost_usd_total",
			Help: "Estimated upstream spend in USD from the configured price table.",
		}, []string{"key", "model"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_breaker_state",
			Help: "Circuit breaker state per provider: 0 closed, 1 half-open, 2 open.",
		}, []string{"provider"}),
		CacheEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_cache_events_total",
			Help: "Response cache lookups by result (hit, miss, bypass).",
		}, []string{"result"}),
		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_ratelimit_rejections_total",
			Help: "Requests rejected with 429 by virtual key and limit kind (rpm or tpm).",
		}, []string{"key", "kind"}),
		UpstreamAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_upstream_attempts_total",
			Help: "Individual upstream attempts by provider and outcome (success, retryable_error, client_error, circuit_open).",
		}, []string{"provider", "outcome"}),
		InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_upstream_inflight",
			Help: "Upstream requests currently in flight per provider.",
		}, []string{"provider"}),
	}
	reg.MustRegister(m.Requests, m.Duration, m.Tokens, m.Cost, m.BreakerState, m.CacheEvents, m.RateLimited, m.UpstreamAttempts, m.InFlight)
	reg.MustRegister(collectors.NewGoCollector())
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
