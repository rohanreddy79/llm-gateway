package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

func userReq(text string) *chat.Request {
	return &chat.Request{Model: "claude-opus-5-5", Messages: []chat.Message{{Role: "user", Content: chat.Content{Text: text}}}}
}

func newProvider(t *testing.T, srv *httptest.Server) *Provider {
	t.Helper()
	return New("anthropic", Options{BaseURL: srv.URL, APIKey: "test-key", HTTPClient: srv.Client(), Now: func() time.Time { return fixedNow }})
}

func TestCompleteHTTP(t *testing.T) {
	var mu sync.Mutex
	var gotReq messagesRequest
	var gotHeader http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotHeader = r.Header.Clone()
		if r.URL.Path != "/v1/messages" || r.Method != http.MethodPost {
			http.Error(w, "wrong route", 404)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		b, _ := os.ReadFile("testdata/response/basic.anthropic.json")
		w.Write(b)
	}))
	defer srv.Close()

	p := newProvider(t, srv)
	resp, err := p.Complete(context.Background(), userReq("Say hi"))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotHeader.Get("x-api-key") != "test-key" || gotHeader.Get("anthropic-version") != DefaultVersion || gotHeader.Get("Content-Type") != "application/json" {
		t.Errorf("headers = %v", gotHeader)
	}
	if gotReq.MaxTokens != 1024 || gotReq.Stream || gotReq.Model != "claude-opus-5-5" {
		t.Errorf("upstream request = %+v", gotReq)
	}
	if resp.Choices[0].Message.Content.Text != "Hi there!" || resp.Usage.TotalTokens != 17 || resp.Created != fixedNow.Unix() {
		t.Errorf("response = %+v", resp)
	}
}

func TestCompleteErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		retryAfter string
		body       string
		wantType   string
		wantMsg    string
		wantRetry  time.Duration
		retryable  bool
	}{
		{"rate limited", 429, "2", `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, "rate_limit_error", "slow down", 2 * time.Second, true},
		{"overloaded", 529, "", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, "overloaded_error", "Overloaded", 0, true},
		{"html 502", 502, "", `<html>bad gateway</html>`, "upstream_error", "<html>bad gateway</html>", 0, true},
		{"bad request", 400, "", `{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: too large"}}`, "invalid_request_error", "max_tokens: too large", 0, false},
		{"auth", 401, "", `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`, "authentication_error", "invalid x-api-key", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			_, err := newProvider(t, srv).Complete(context.Background(), userReq("x"))
			var pe *provider.Error
			if !errors.As(err, &pe) {
				t.Fatalf("want *provider.Error, got %v", err)
			}
			if pe.Status != tc.status || pe.Type != tc.wantType || pe.Message != tc.wantMsg || pe.RetryAfter != tc.wantRetry || pe.Provider != "anthropic" {
				t.Errorf("error = %+v", pe)
			}
			if provider.IsRetryable(err) != tc.retryable {
				t.Errorf("retryable = %v, want %v", provider.IsRetryable(err), tc.retryable)
			}
		})
	}
}

func TestCompleteBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	_, err := newProvider(t, srv).Complete(context.Background(), userReq("x"))
	if provider.StatusOf(err) != 502 {
		t.Fatalf("want 502, got %v", err)
	}
}

func TestCompleteTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the body: Go's HTTP/1 server only notices a closed client
		// connection (and cancels r.Context()) after the body is consumed.
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := newProvider(t, srv).Complete(ctx, userReq("x"))
	if !errors.Is(err, context.DeadlineExceeded) || !provider.IsRetryable(err) {
		t.Fatalf("want retryable deadline error, got %v", err)
	}
}

func TestStreamHTTP(t *testing.T) {
	var mu sync.Mutex
	var gotReq messagesRequest
	var accept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		accept = r.Header.Get("Accept")
		json.NewDecoder(r.Body).Decode(&gotReq)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		b, _ := os.ReadFile("testdata/stream/basic.sse")
		w.Write(b)
	}))
	defer srv.Close()
	ch, err := newProvider(t, srv).Stream(context.Background(), userReq("hi"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var text string
	var last *chat.Chunk
	for c := range ch {
		if c.Err != nil {
			t.Fatalf("chunk error: %v", c.Err)
		}
		if d := c.Data.Choices[0].Delta.Content; d != nil {
			text += *d
		}
		last = c.Data
	}
	mu.Lock()
	defer mu.Unlock()
	if !gotReq.Stream || accept != "text/event-stream" {
		t.Errorf("stream=%v accept=%q", gotReq.Stream, accept)
	}
	if text != "Hello, world" || last.Usage == nil || last.Usage.CompletionTokens != 12 || *last.Choices[0].FinishReason != "stop" {
		t.Errorf("text=%q last=%+v", text, last)
	}
}

func TestStreamErrorStatusBeforeData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(529)
		w.Write([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`))
	}))
	defer srv.Close()
	ch, err := newProvider(t, srv).Stream(context.Background(), userReq("hi"))
	if ch != nil || provider.StatusOf(err) != 529 {
		t.Fatalf("want 529 before any chunk, got ch=%v err=%v", ch, err)
	}
}

func TestStreamCancelMidway(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"x\",\"usage\":{\"input_tokens\":1}}}\n\n"))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := newProvider(t, srv).Stream(ctx, userReq("hi"))
	if err != nil {
		t.Fatal(err)
	}
	first := <-ch
	if first.Err != nil || first.Data.Choices[0].Delta.Role != "assistant" {
		t.Fatalf("first = %+v", first)
	}
	<-started
	cancel()
	var last provider.Chunk
	for c := range ch {
		last = c
	}
	if !errors.Is(last.Err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %+v", last)
	}
}
