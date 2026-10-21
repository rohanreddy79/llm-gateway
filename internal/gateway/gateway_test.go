package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/rohanreddy79/llm-gateway/internal/breaker"
	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/config"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
	"github.com/rohanreddy79/llm-gateway/internal/provider/mock"
	"github.com/rohanreddy79/llm-gateway/internal/sse"
	"github.com/rohanreddy79/llm-gateway/internal/usage"
)

// clock is a mutable fake clock shared by the breaker, limiter and cache.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_760_000_000, 0)} }
func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const baseYAML = `
providers:
  a: {type: mock, timeout: 2s}
  b: {type: mock, timeout: 2s}
models:
  primary: [a:model-a]
  chain: [a:model-a, b:model-b]
keys:
  key-full: {name: full, admin: true}
  key-limited: {name: limited, rpm: 2, tpm: 1000, models: [primary]}
  key-tpm: {name: tpm, tpm: 10}
cache: {ttl: 1m, max_entries: 100}
retry: {max_attempts: 2, base_backoff: 1ms, max_backoff: 2ms}
breaker: {failures: 2, open_for: 10s}
prices:
  model-a: {input: 1.0, output: 2.0}
`

type harness struct {
	t     *testing.T
	srv   *httptest.Server
	g     *Gateway
	clock *clock
	a, b  *mock.Mock
}

func newHarness(t *testing.T, yaml string, aOpts, bOpts mock.Options) *harness {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	c := newClock()
	a := mock.New("a", aOpts)
	b := mock.New("b", bOpts)
	g, err := New(cfg, Options{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:       c.now,
		Providers: map[string]provider.Provider{"a": a, "b": b},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(g.Handler())
	t.Cleanup(srv.Close)
	return &harness{t: t, srv: srv, g: g, clock: c, a: a, b: b}
}

func (h *harness) do(method, path, key string, body any) (*http.Response, []byte) {
	h.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, rd)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func chatReq(model, text string, extra map[string]any) map[string]any {
	m := map[string]any{"model": model, "messages": []map[string]any{{"role": "user", "content": text}}}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func errCode(t *testing.T, body []byte) string {
	t.Helper()
	var er chat.ErrorResponse
	if err := json.Unmarshal(body, &er); err != nil {
		t.Fatalf("not an error envelope: %s", body)
	}
	if er.Error.Code == nil {
		return ""
	}
	return *er.Error.Code
}

func TestAuth(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	resp, body := h.do("POST", "/v1/chat/completions", "", chatReq("primary", "hi", nil))
	if resp.StatusCode != 401 || errCode(t, body) != "invalid_api_key" {
		t.Fatalf("no key: %d %s", resp.StatusCode, body)
	}
	resp, _ = h.do("POST", "/v1/chat/completions", "nope", chatReq("primary", "hi", nil))
	if resp.StatusCode != 401 {
		t.Fatalf("unknown key: %d", resp.StatusCode)
	}
	resp, body = h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", nil))
	if resp.StatusCode != 200 {
		t.Fatalf("valid key: %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Error("X-Request-ID missing")
	}
	// x-api-key is accepted too.
	req, _ := http.NewRequest("GET", h.srv.URL+"/v1/models", nil)
	req.Header.Set("x-api-key", "key-full")
	r2, _ := http.DefaultClient.Do(req)
	r2.Body.Close()
	if r2.StatusCode != 200 {
		t.Fatalf("x-api-key: %d", r2.StatusCode)
	}
}

func TestModelAllowlistAndUnknownModel(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	resp, body := h.do("POST", "/v1/chat/completions", "key-limited", chatReq("chain", "hi", nil))
	if resp.StatusCode != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("disallowed: %d %s", resp.StatusCode, body)
	}
	resp, body = h.do("POST", "/v1/chat/completions", "key-full", chatReq("ghost", "hi", nil))
	if resp.StatusCode != 404 || errCode(t, body) != "model_not_found" {
		t.Fatalf("unknown: %d %s", resp.StatusCode, body)
	}
	if h.a.Calls() != 0 {
		t.Error("rejected requests must not reach a provider")
	}
}

func TestValidation(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	cases := []struct {
		name string
		body string
	}{
		{"invalid json", `{"model": `},
		{"no model", `{"messages":[{"role":"user","content":"x"}]}`},
		{"no messages", `{"model":"primary","messages":[]}`},
		{"n=2", `{"model":"primary","messages":[{"role":"user","content":"x"}],"n":2}`},
		{"bad content", `{"model":"primary","messages":[{"role":"user","content":5}]}`},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest("POST", h.srv.URL+"/v1/chat/completions", strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer key-full")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", tc.name, resp.StatusCode)
		}
	}
}

func TestRateLimitRPM(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	for i := 0; i < 2; i++ {
		resp, body := h.do("POST", "/v1/chat/completions", "key-limited", chatReq("primary", "hi", map[string]any{"temperature": 0.5}))
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: %d %s", i, resp.StatusCode, body)
		}
		if resp.Header.Get("X-RateLimit-Limit-Requests") != "2" || resp.Header.Get("X-RateLimit-Remaining-Requests") != fmt.Sprint(1-i) {
			t.Errorf("request %d headers: %v", i, resp.Header)
		}
	}
	resp, body := h.do("POST", "/v1/chat/completions", "key-limited", chatReq("primary", "hi", nil))
	if resp.StatusCode != 429 || errCode(t, body) != "rate_limit_exceeded" {
		t.Fatalf("third request: %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("Retry-After") != "30" || resp.Header.Get("X-RateLimit-Remaining-Requests") != "0" {
		t.Errorf("429 headers: %v", resp.Header)
	}
	if got := testutil.ToFloat64(h.g.Metrics().RateLimited.WithLabelValues("limited", "rpm")); got != 1 {
		t.Errorf("rejection metric = %v", got)
	}
	h.clock.advance(31 * time.Second)
	if resp, _ := h.do("POST", "/v1/chat/completions", "key-limited", chatReq("primary", "hi", nil)); resp.StatusCode != 200 {
		t.Fatalf("after refill: %d", resp.StatusCode)
	}
}

func TestRateLimitTPMFromUsage(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	resp, _ := h.do("POST", "/v1/chat/completions", "key-tpm", chatReq("primary", "hello there friend", map[string]any{"temperature": 1}))
	if resp.StatusCode != 200 {
		t.Fatalf("first: %d", resp.StatusCode)
	}
	resp, body := h.do("POST", "/v1/chat/completions", "key-tpm", chatReq("primary", "hello", map[string]any{"temperature": 1}))
	if resp.StatusCode != 429 || !strings.Contains(string(body), "tokens per minute") {
		t.Fatalf("second should be token-limited: %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("X-RateLimit-Remaining-Tokens") != "0" || resp.Header.Get("Retry-After") == "" {
		t.Errorf("headers: %v", resp.Header)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	h.a.FailNext(1, 503)
	resp, body := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", nil))
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if h.a.Calls() != 2 {
		t.Fatalf("calls = %d, want 2 (one retry)", h.a.Calls())
	}
	if got := testutil.ToFloat64(h.g.Metrics().UpstreamAttempts.WithLabelValues("a", "retryable_error")); got != 1 {
		t.Errorf("retryable_error attempts = %v", got)
	}
}

func TestFallbackToSecondProvider(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	h.a.FailNext(5, 503)
	resp, body := h.do("POST", "/v1/chat/completions", "key-full", chatReq("chain", "hi", nil))
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	var cr chat.Response
	json.Unmarshal(body, &cr)
	if cr.Model != "model-b" {
		t.Errorf("served by %q, want model-b", cr.Model)
	}
	if h.a.Calls() != 2 || h.b.Calls() != 1 {
		t.Errorf("calls a=%d b=%d, want 2 and 1", h.a.Calls(), h.b.Calls())
	}
	// Exhausting the whole chain surfaces the last error (a sampled request,
	// so the cached fallback response above is not replayed).
	h.a.FailNext(5, 503)
	h.b.FailNext(5, 500)
	resp, body = h.do("POST", "/v1/chat/completions", "key-full", chatReq("chain", "hi", map[string]any{"temperature": 1}))
	if resp.StatusCode != 502 || errCode(t, body) != "upstream_error" {
		t.Fatalf("exhausted chain: %d %s", resp.StatusCode, body)
	}
}

func TestNonRetryableErrorDoesNotFallBack(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	h.a.FailNext(1, 400)
	resp, body := h.do("POST", "/v1/chat/completions", "key-full", chatReq("chain", "hi", nil))
	if resp.StatusCode != 400 || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if h.a.Calls() != 1 || h.b.Calls() != 0 {
		t.Errorf("calls a=%d b=%d: a 400 must not be retried or re-routed", h.a.Calls(), h.b.Calls())
	}
}

func TestCircuitBreaker(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	gauge := h.g.Metrics().BreakerState.WithLabelValues("a")
	h.a.FailNext(2, 503) // two attempts (one request with one retry) open the breaker
	resp, _ := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", map[string]any{"temperature": 1}))
	if resp.StatusCode != 503 {
		t.Fatalf("first: %d", resp.StatusCode)
	}
	if h.g.Router().BreakerState("a") != breaker.Open || testutil.ToFloat64(gauge) != 2 {
		t.Fatalf("breaker should be open: %v gauge=%v", h.g.Router().BreakerState("a"), testutil.ToFloat64(gauge))
	}
	calls := h.a.Calls()
	resp, body := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", map[string]any{"temperature": 1}))
	if resp.StatusCode != 503 || errCode(t, body) != "circuit_open" {
		t.Fatalf("open breaker must reject fast: %d %s", resp.StatusCode, body)
	}
	if h.a.Calls() != calls {
		t.Fatal("open breaker must not call the provider")
	}
	// With a healthy fallback the open breaker is skipped transparently.
	resp, body = h.do("POST", "/v1/chat/completions", "key-full", chatReq("chain", "hi", map[string]any{"temperature": 1}))
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"model":"model-b"`) {
		t.Fatalf("fallback around open breaker: %d %s", resp.StatusCode, body)
	}
	h.clock.advance(11 * time.Second)
	resp, _ = h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", map[string]any{"temperature": 1}))
	if resp.StatusCode != 200 || h.g.Router().BreakerState("a") != breaker.Closed || testutil.ToFloat64(gauge) != 0 {
		t.Fatalf("half-open probe should close the breaker: %d state=%v", resp.StatusCode, h.g.Router().BreakerState("a"))
	}
}

func TestCacheHit(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	req := chatReq("primary", "cache me", map[string]any{"temperature": 0})
	resp1, body1 := h.do("POST", "/v1/chat/completions", "key-full", req)
	resp2, body2 := h.do("POST", "/v1/chat/completions", "key-full", req)
	if resp1.Header.Get("X-Cache") != "MISS" || resp2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("X-Cache: %q then %q", resp1.Header.Get("X-Cache"), resp2.Header.Get("X-Cache"))
	}
	if !bytes.Equal(body1, body2) || h.a.Calls() != 1 {
		t.Fatalf("hit must replay the body without an upstream call (calls=%d)", h.a.Calls())
	}
	// Different prompt, sampled request and streams all miss.
	h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "other", map[string]any{"temperature": 0}))
	h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "cache me", map[string]any{"temperature": 0.9}))
	r, _ := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "cache me", map[string]any{"temperature": 0.9}))
	if r.Header.Get("X-Cache") != "" || h.a.Calls() != 4 {
		t.Fatalf("sampled requests must bypass the cache: X-Cache=%q calls=%d", r.Header.Get("X-Cache"), h.a.Calls())
	}
	h.clock.advance(2 * time.Minute)
	r, _ = h.do("POST", "/v1/chat/completions", "key-full", req)
	if r.Header.Get("X-Cache") != "MISS" {
		t.Fatal("entry must expire after ttl")
	}
	m := h.g.Metrics().CacheEvents
	if testutil.ToFloat64(m.WithLabelValues("hit")) != 1 || testutil.ToFloat64(m.WithLabelValues("miss")) != 3 || testutil.ToFloat64(m.WithLabelValues("bypass")) != 2 {
		t.Errorf("cache metrics hit=%v miss=%v bypass=%v", testutil.ToFloat64(m.WithLabelValues("hit")), testutil.ToFloat64(m.WithLabelValues("miss")), testutil.ToFloat64(m.WithLabelValues("bypass")))
	}
}

func TestUsageAccounting(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{Text: "one two three four"}, mock.Options{})
	for i := 0; i < 3; i++ {
		h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hello", map[string]any{"temperature": 1}))
	}
	resp, body := h.do("GET", "/admin/usage", "key-full", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("usage: %d %s", resp.StatusCode, body)
	}
	var rep usage.Report
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Keys) != 1 || rep.Keys[0].Key != "full" {
		t.Fatalf("report = %+v", rep)
	}
	k := rep.Keys[0]
	// mock usage: prompt = (5+3)/4 + 4 = 6 per message, completion = 4 words.
	if k.Requests != 3 || k.InputTokens != 18 || k.OutputTokens != 12 {
		t.Errorf("totals = %+v", k.Record)
	}
	wantCost := 18*1.0/1e6 + 12*2.0/1e6
	if diff := k.CostUSD - wantCost; diff > 1e-12 || diff < -1e-12 {
		t.Errorf("cost = %v, want %v", k.CostUSD, wantCost)
	}
	if k.Models["a:model-a"] == nil || k.Models["a:model-a"].Requests != 3 {
		t.Errorf("models = %+v", k.Models)
	}
	if got := testutil.ToFloat64(h.g.Metrics().Tokens.WithLabelValues("full", "primary", "output")); got != 12 {
		t.Errorf("token metric = %v", got)
	}
	// Admin endpoint is gated.
	if resp, _ := h.do("GET", "/admin/usage", "key-limited", nil); resp.StatusCode != 403 {
		t.Errorf("non-admin key: %d", resp.StatusCode)
	}
}

// readSSE parses a streamed response into its data payloads.
func readSSE(t *testing.T, body io.Reader) []string {
	t.Helper()
	rd := sse.NewReader(body)
	var out []string
	for {
		ev, err := rd.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("sse: %v", err)
		}
		out = append(out, ev.Data)
	}
}

func TestStreaming(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{Text: "alpha beta gamma"}, mock.Options{})
	b, _ := json.Marshal(chatReq("primary", "hi", map[string]any{"stream": true}))
	req, _ := http.NewRequest("POST", h.srv.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer key-full")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status=%d content-type=%q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	events := readSSE(t, resp.Body)
	if events[len(events)-1] != "[DONE]" {
		t.Fatalf("stream must end with [DONE], got %q", events[len(events)-1])
	}
	var text string
	var last chat.Chunk
	for _, e := range events[:len(events)-1] {
		var c chat.Chunk
		if err := json.Unmarshal([]byte(e), &c); err != nil {
			t.Fatalf("chunk %q: %v", e, err)
		}
		if c.Object != "chat.completion.chunk" {
			t.Errorf("object = %q", c.Object)
		}
		if d := c.Choices[0].Delta.Content; d != nil {
			text += *d
		}
		last = c
	}
	if text != "alpha beta gamma" || last.Usage == nil || *last.Choices[0].FinishReason != "stop" {
		t.Errorf("text=%q last=%+v", text, last)
	}
	// Usage from the final chunk is accounted.
	_, body := h.do("GET", "/admin/usage", "key-full", nil)
	if !strings.Contains(string(body), `"output_tokens":3`) {
		t.Errorf("stream usage not recorded: %s", body)
	}
}

func TestStreamErrorBeforeFirstByteIsJSON(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	h.a.FailNext(5, 503)
	resp, body := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", map[string]any{"stream": true}))
	if resp.StatusCode != 503 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("%d %q %s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
	// And a stream falls back like a completion does.
	h.a.FailNext(5, 503)
	resp, body = h.do("POST", "/v1/chat/completions", "key-full", chatReq("chain", "hi", map[string]any{"stream": true}))
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"model":"model-b"`) {
		t.Fatalf("stream fallback: %d %s", resp.StatusCode, body)
	}
}

func TestTimeoutSurfacesAs504(t *testing.T) {
	yaml := strings.Replace(baseYAML, "a: {type: mock, timeout: 2s}", "a: {type: mock, timeout: 30ms}", 1)
	h := newHarness(t, yaml, mock.Options{Hang: true}, mock.Options{})
	resp, body := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", nil))
	if resp.StatusCode != 504 || errCode(t, body) != "upstream_timeout" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if h.a.Calls() != 2 {
		t.Errorf("timeouts are retryable: calls = %d", h.a.Calls())
	}
}

func TestConcurrencyCapSerialises(t *testing.T) {
	yaml := strings.Replace(baseYAML, "a: {type: mock, timeout: 2s}", "a: {type: mock, timeout: 2s, max_concurrency: 1}", 1)
	h := newHarness(t, yaml, mock.Options{Latency: 30 * time.Millisecond}, mock.Options{})
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, _ := h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", map[string]any{"temperature": 1})); resp.StatusCode != 200 {
				t.Errorf("status %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	if el := time.Since(start); el < 85*time.Millisecond {
		t.Fatalf("3 x 30ms with max_concurrency 1 took only %v", el)
	}
}

func TestModelsHealthMetrics(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{}, mock.Options{})
	_, body := h.do("GET", "/v1/models", "key-limited", nil)
	var list chat.ModelList
	json.Unmarshal(body, &list)
	if len(list.Data) != 1 || list.Data[0].ID != "primary" || list.Data[0].OwnedBy != "a" {
		t.Errorf("models for limited key = %s", body)
	}
	_, body = h.do("GET", "/v1/models", "key-full", nil)
	json.Unmarshal(body, &list)
	if len(list.Data) != 2 {
		t.Errorf("models for full key = %s", body)
	}
	resp, body := h.do("GET", "/healthz", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"a":"closed"`) {
		t.Errorf("healthz: %d %s", resp.StatusCode, body)
	}
	h.do("POST", "/v1/chat/completions", "key-full", chatReq("primary", "hi", nil))
	resp, body = h.do("GET", "/metrics", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(string(body), `gateway_requests_total{key="full",model="primary",provider="a",status="200"} 1`) {
		t.Errorf("metrics: %d\n%s", resp.StatusCode, body)
	}
	resp, body = h.do("GET", "/nope", "", nil)
	if resp.StatusCode != 404 || errCode(t, body) != "not_found" {
		t.Errorf("unknown route: %d %s", resp.StatusCode, body)
	}
}

// TestAnthropicEndToEnd routes through a real anthropic-type provider built
// from config against an httptest upstream serving the golden fixtures.
func TestAnthropicEndToEnd(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-anthropic-key" {
			w.WriteHeader(401)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			b, _ := os.ReadFile("../provider/anthropic/testdata/stream/basic.sse")
			w.Write(b)
			return
		}
		b, _ := os.ReadFile("../provider/anthropic/testdata/response/basic.anthropic.json")
		w.Write(b)
	}))
	defer up.Close()
	t.Setenv("TEST_ANTHROPIC_KEY", "test-anthropic-key")
	cfg, err := config.Parse([]byte(fmt.Sprintf(`
providers:
  anthropic: {type: anthropic, base_url: %q, api_key_env: TEST_ANTHROPIC_KEY}
models:
  claude: [anthropic:claude-opus-5-5]
keys:
  k: {name: k}
`, up.URL)))
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(cfg, Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(g.Handler())
	defer srv.Close()
	h := &harness{t: t, srv: srv, g: g}

	resp, body := h.do("POST", "/v1/chat/completions", "k", chatReq("claude", "Say hi", nil))
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"content":"Hi there!"`) {
		t.Fatalf("complete: %d %s", resp.StatusCode, body)
	}
	resp, body = h.do("POST", "/v1/chat/completions", "k", chatReq("claude", "Say hi", map[string]any{"stream": true}))
	if resp.StatusCode != 200 {
		t.Fatalf("stream: %d %s", resp.StatusCode, body)
	}
	events := readSSE(t, bytes.NewReader(body))
	if len(events) != 5 || events[4] != "[DONE]" || !strings.Contains(events[3], `"completion_tokens":12`) {
		t.Fatalf("stream events = %q", events)
	}
	_, body = h.do("GET", "/admin/usage", "k", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("%s", body)
	}
}

func TestNewRejectsMissingUpstreamKey(t *testing.T) {
	cfg, _ := config.Parse([]byte(`
providers:
  openai: {type: openai, api_key_env: DEFINITELY_UNSET_VAR_123}
models: {m: [openai:gpt]}
keys: {k: {name: k}}
`))
	if _, err := New(cfg, Options{Getenv: func(string) string { return "" }}); err == nil || !strings.Contains(err.Error(), "DEFINITELY_UNSET_VAR_123") {
		t.Fatalf("want missing-env error, got %v", err)
	}
}

func TestClientDisconnectDuringStream(t *testing.T) {
	h := newHarness(t, baseYAML, mock.Options{Text: strings.Repeat("w ", 5000), Latency: 5 * time.Millisecond}, mock.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	b, _ := json.Marshal(chatReq("primary", "hi", map[string]any{"stream": true}))
	req, _ := http.NewRequestWithContext(ctx, "POST", h.srv.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer key-full")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	resp.Body.Read(buf)
	cancel()
	resp.Body.Close()
	// The concurrency slot must be released; a follow-up request succeeds.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(h.g.Metrics().InFlight.WithLabelValues("a")) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("upstream slot was not released after the client disconnected")
}
