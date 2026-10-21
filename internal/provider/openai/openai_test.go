package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

func parseReq(t *testing.T, s string) *chat.Request {
	t.Helper()
	var r chat.Request
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

const upstreamBody = `{"id":"chatcmpl-abc","object":"chat.completion","created":1,"model":"gpt-x-2026","choices":[{"index":0,"message":{"role":"assistant","content":"hey","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":9,"completion_tokens":1,"total_tokens":10},"system_fingerprint":"fp_1"}`

func TestCompletePassthrough(t *testing.T) {
	var mu sync.Mutex
	var gotBody map[string]json.RawMessage
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(upstreamBody))
	}))
	defer srv.Close()
	p := New("openai", Options{BaseURL: srv.URL + "/v1", APIKey: "sk-test", HTTPClient: srv.Client()})
	req := parseReq(t, `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true,"response_format":{"type":"json_object"},"temperature":0.3}`)
	resp, err := p.Complete(context.Background(), req)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "Bearer sk-test" || gotPath != "/v1/chat/completions" {
		t.Errorf("auth=%q path=%q", gotAuth, gotPath)
	}
	if _, ok := gotBody["response_format"]; !ok {
		t.Error("unknown fields must be passed through to the upstream")
	}
	if _, ok := gotBody["stream"]; ok {
		t.Error("Complete must not send stream:true")
	}
	if string(resp.Raw) != upstreamBody {
		t.Error("Raw must be the upstream body verbatim")
	}
	if resp.ID != "chatcmpl-abc" || resp.Usage.TotalTokens != 10 || resp.Choices[0].Message.Content.Text != "hey" {
		t.Errorf("parsed = %+v", resp)
	}
}

func TestCompleteErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(503)
		w.Write([]byte(`{"error":{"message":"engine overloaded","type":"server_error","param":null,"code":null}}`))
	}))
	defer srv.Close()
	p := New("openai", Options{BaseURL: srv.URL, APIKey: "k", HTTPClient: srv.Client()})
	_, err := p.Complete(context.Background(), parseReq(t, `{"model":"m","messages":[]}`))
	var pe *provider.Error
	if !errors.As(err, &pe) || pe.Status != 503 || pe.Message != "engine overloaded" || pe.Type != "server_error" || pe.RetryAfter.Seconds() != 1 {
		t.Fatalf("err = %+v", err)
	}
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer srv2.Close()
	p2 := New("openai", Options{BaseURL: srv2.URL, APIKey: "k", HTTPClient: srv2.Client()})
	if _, err := p2.Complete(context.Background(), parseReq(t, `{"model":"m","messages":[]}`)); provider.StatusOf(err) != 502 {
		t.Fatalf("bad json: %v", err)
	}
}

func TestCompleteUnreachable(t *testing.T) {
	p := New("openai", Options{BaseURL: "http://127.0.0.1:1", APIKey: "k"})
	_, err := p.Complete(context.Background(), parseReq(t, `{"model":"m","messages":[]}`))
	if provider.StatusOf(err) != 502 || !provider.IsRetryable(err) {
		t.Fatalf("want retryable 502 for connection refused, got %v", err)
	}
}

const streamBody = `data: {"id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null}]}

data: {"id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}]}

data: {"id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":null}]}

data: {"id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}

data: [DONE]

`

func TestStreamPassthrough(t *testing.T) {
	var mu sync.Mutex
	var gotBody chat.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		json.NewDecoder(r.Body).Decode(&gotBody)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(streamBody))
	}))
	defer srv.Close()
	p := New("openai", Options{BaseURL: srv.URL, APIKey: "k", HTTPClient: srv.Client()})
	ch, err := p.Stream(context.Background(), parseReq(t, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var chunks []*chat.Chunk
	for c := range ch {
		if c.Err != nil {
			t.Fatalf("chunk err: %v", c.Err)
		}
		chunks = append(chunks, c.Data)
	}
	mu.Lock()
	defer mu.Unlock()
	if !gotBody.Stream || gotBody.StreamOptions == nil || !gotBody.StreamOptions.IncludeUsage {
		t.Errorf("upstream request must force stream + include_usage: %+v", gotBody)
	}
	if len(chunks) != 5 {
		t.Fatalf("got %d chunks, want 5", len(chunks))
	}
	if !strings.Contains(string(chunks[0].Raw), `"logprobs":null`) {
		t.Error("Raw must preserve upstream fields the gateway does not model")
	}
	if chunks[4].Usage == nil || chunks[4].Usage.TotalTokens != 6 || len(chunks[4].Choices) != 0 {
		t.Errorf("usage chunk = %+v", chunks[4])
	}
}

func TestStreamErrors(t *testing.T) {
	cases := map[string]string{
		"mid-stream error": "data: {\"id\":\"x\",\"choices\":[]}\n\ndata: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\"}}\n\n",
		"no DONE":          "data: {\"id\":\"x\",\"choices\":[]}\n\n",
		"malformed":        "data: {oops\n\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(body))
			}))
			defer srv.Close()
			p := New("openai", Options{BaseURL: srv.URL, APIKey: "k", HTTPClient: srv.Client()})
			ch, err := p.Stream(context.Background(), parseReq(t, `{"model":"m","messages":[]}`))
			if err != nil {
				t.Fatal(err)
			}
			var last provider.Chunk
			for c := range ch {
				last = c
			}
			if last.Err == nil || provider.StatusOf(last.Err) != 502 {
				t.Fatalf("want a 502 chunk error, got %+v", last)
			}
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		w.Write([]byte(`{"error":{"message":"rate","type":"rate_limit_error"}}`))
	}))
	defer srv.Close()
	p := New("openai", Options{BaseURL: srv.URL, APIKey: "k", HTTPClient: srv.Client()})
	if _, err := p.Stream(context.Background(), parseReq(t, `{"model":"m","messages":[]}`)); provider.StatusOf(err) != 429 {
		t.Fatalf("status before data must be returned synchronously: %v", err)
	}
}
