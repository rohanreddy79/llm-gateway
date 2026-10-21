// Package mock is a deterministic in-process provider used by the tests, the
// demo config and the benchmark. It never touches the network.
package mock

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

// DefaultText is the completion returned when Options.Text is empty.
const DefaultText = "This is a deterministic mock completion from llm-gateway. " +
	"It exists so the gateway can be tested and benchmarked without any network or API key."

// Options configures a Mock.
type Options struct {
	// Latency is slept before the first byte of every completion.
	Latency time.Duration
	// Text is the completion text; DefaultText when empty.
	Text string
	// FailEvery makes every Nth call fail with FailStatus (0 disables).
	FailEvery int
	// FailStatus is the HTTP status of injected failures (default 503).
	FailStatus int
	// Hang makes every call block until the context is done, to exercise
	// timeouts.
	Hang bool
	// Now overrides the clock used for the created timestamp.
	Now func() time.Time
}

// Mock implements provider.Provider.
type Mock struct {
	name  string
	opts  Options
	calls atomic.Int64
	// failNext counts down the number of upcoming calls to fail.
	failNext   atomic.Int64
	failStatus atomic.Int64
}

// New returns a mock provider with the given name.
func New(name string, opts Options) *Mock {
	if opts.Text == "" {
		opts.Text = DefaultText
	}
	if opts.FailStatus == 0 {
		opts.FailStatus = 503
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Mock{name: name, opts: opts}
}

// Name implements provider.Provider.
func (m *Mock) Name() string { return m.name }

// Calls returns how many completions (streaming or not) were attempted.
func (m *Mock) Calls() int64 { return m.calls.Load() }

// FailNext makes the next n calls fail with the given status.
func (m *Mock) FailNext(n int, status int) {
	m.failStatus.Store(int64(status))
	m.failNext.Store(int64(n))
}

// begin applies latency and failure injection shared by both entry points.
func (m *Mock) begin(ctx context.Context) error {
	n := m.calls.Add(1)
	if m.opts.Hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if m.opts.Latency > 0 {
		t := time.NewTimer(m.opts.Latency)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	if m.failNext.Load() > 0 && m.failNext.Add(-1) >= 0 {
		return provider.NewError(m.name, int(m.failStatus.Load()), "mock_error", "injected failure")
	}
	if m.opts.FailEvery > 0 && n%int64(m.opts.FailEvery) == 0 {
		return provider.NewError(m.name, m.opts.FailStatus, "mock_error", "injected periodic failure")
	}
	return nil
}

// Complete implements provider.Provider.
func (m *Mock) Complete(ctx context.Context, req *chat.Request) (*chat.Response, error) {
	if err := m.begin(ctx); err != nil {
		return nil, err
	}
	usage := m.usage(req)
	return &chat.Response{
		ID:      "chatcmpl-mock",
		Object:  "chat.completion",
		Created: m.opts.Now().Unix(),
		Model:   req.Model,
		Choices: []chat.Choice{{
			Index:        0,
			Message:      chat.Message{Role: "assistant", Content: chat.Content{Text: m.opts.Text}},
			FinishReason: chat.StrPtr("stop"),
		}},
		Usage: &usage,
	}, nil
}

// Stream implements provider.Provider: one chunk per word, then a final chunk
// carrying finish_reason and usage.
func (m *Mock) Stream(ctx context.Context, req *chat.Request) (<-chan provider.Chunk, error) {
	if err := m.begin(ctx); err != nil {
		return nil, err
	}
	words := strings.Fields(m.opts.Text)
	usage := m.usage(req)
	created := m.opts.Now().Unix()
	out := make(chan provider.Chunk, 16)
	go func() {
		defer close(out)
		send := func(c *chat.Chunk) bool {
			select {
			case out <- provider.Chunk{Data: c}:
				return true
			case <-ctx.Done():
				return false
			}
		}
		base := func() *chat.Chunk {
			return &chat.Chunk{ID: "chatcmpl-mock", Object: "chat.completion.chunk", Created: created, Model: req.Model}
		}
		first := base()
		first.Choices = []chat.ChunkChoice{{Delta: chat.Delta{Role: "assistant", Content: chat.StrPtr("")}}}
		if !send(first) {
			return
		}
		for i, w := range words {
			c := base()
			text := w
			if i < len(words)-1 {
				text += " "
			}
			c.Choices = []chat.ChunkChoice{{Delta: chat.Delta{Content: chat.StrPtr(text)}}}
			if !send(c) {
				return
			}
		}
		last := base()
		last.Choices = []chat.ChunkChoice{{FinishReason: chat.StrPtr("stop")}}
		last.Usage = &usage
		send(last)
	}()
	return out, nil
}

// usage estimates tokens deterministically: ~4 characters per prompt token
// plus a per-message overhead, and one completion token per word.
func (m *Mock) usage(req *chat.Request) chat.Usage {
	prompt := 0
	for _, msg := range req.Messages {
		text, _ := msg.Content.PlainText()
		prompt += (len(text)+3)/4 + 4
	}
	completion := len(strings.Fields(m.opts.Text))
	return chat.Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion}
}
