package mock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

func req() *chat.Request {
	return &chat.Request{Model: "mock-1", Messages: []chat.Message{{Role: "user", Content: chat.Content{Text: "hello there"}}}}
}

func TestCompleteDeterministic(t *testing.T) {
	m := New("mock", Options{Text: "one two three", Now: func() time.Time { return time.Unix(5, 0) }})
	a, err := m.Complete(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := m.Complete(context.Background(), req())
	if a.Choices[0].Message.Content.Text != "one two three" || *a.Choices[0].FinishReason != "stop" || a.Created != 5 {
		t.Errorf("resp = %+v", a)
	}
	if a.Usage.CompletionTokens != 3 || a.Usage.PromptTokens != b.Usage.PromptTokens || a.Usage.TotalTokens != a.Usage.PromptTokens+3 {
		t.Errorf("usage = %+v", a.Usage)
	}
	if m.Calls() != 2 {
		t.Errorf("calls = %d", m.Calls())
	}
}

func TestStreamReassembles(t *testing.T) {
	m := New("mock", Options{Text: "alpha beta gamma"})
	ch, err := m.Stream(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	var n int
	var last *chat.Chunk
	for c := range ch {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
		n++
		if d := c.Data.Choices[0].Delta.Content; d != nil {
			sb.WriteString(*d)
		}
		last = c.Data
	}
	if sb.String() != "alpha beta gamma" || n != 5 {
		t.Errorf("text=%q chunks=%d", sb.String(), n)
	}
	if last.Usage == nil || last.Usage.CompletionTokens != 3 || *last.Choices[0].FinishReason != "stop" {
		t.Errorf("final = %+v", last)
	}
}

func TestFailureInjection(t *testing.T) {
	m := New("mock", Options{})
	m.FailNext(2, 503)
	for i := 0; i < 2; i++ {
		_, err := m.Complete(context.Background(), req())
		if provider.StatusOf(err) != 503 {
			t.Fatalf("call %d: want 503, got %v", i, err)
		}
	}
	if _, err := m.Complete(context.Background(), req()); err != nil {
		t.Fatalf("third call should succeed: %v", err)
	}
	every := New("mock", Options{FailEvery: 2, FailStatus: 429})
	var fails int
	for i := 0; i < 6; i++ {
		if _, err := every.Stream(context.Background(), req()); err != nil {
			if provider.StatusOf(err) != 429 {
				t.Fatal(err)
			}
			fails++
		}
	}
	if fails != 3 {
		t.Errorf("FailEvery=2 over 6 calls failed %d times", fails)
	}
}

func TestHangAndLatencyRespectContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := New("mock", Options{Hang: true}).Complete(ctx, req()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hang: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if _, err := New("mock", Options{Latency: time.Second}).Stream(ctx2, req()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("latency: %v", err)
	}
}

func TestStreamStopsWhenCancelled(t *testing.T) {
	m := New("mock", Options{Text: strings.Repeat("w ", 1000)})
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := m.Stream(ctx, req())
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel()
	for range ch { // must drain and close promptly
	}
}
