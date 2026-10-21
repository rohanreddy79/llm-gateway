// Package anthropic translates OpenAI chat completions to and from the
// Anthropic Messages API, including the streaming event format.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

// DefaultVersion is the anthropic-version header value.
const DefaultVersion = "2023-06-01"

// Options configures a Provider.
type Options struct {
	BaseURL string
	APIKey  string
	// Version is the anthropic-version header; DefaultVersion when empty.
	Version string
	// DefaultMaxTokens is used when the request has no max_tokens.
	DefaultMaxTokens int
	HTTPClient       *http.Client
	Now              func() time.Time
}

// Provider implements provider.Provider.
type Provider struct {
	name string
	opts Options
}

// New returns a provider named name.
func New(name string, opts Options) *Provider {
	if opts.Version == "" {
		opts.Version = DefaultVersion
	}
	if opts.DefaultMaxTokens <= 0 {
		opts.DefaultMaxTokens = 1024
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Provider{name: name, opts: opts}
}

// Name implements provider.Provider.
func (p *Provider) Name() string { return p.name }

func (p *Provider) newRequest(ctx context.Context, body any) (*http.Request, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.opts.BaseURL+"/v1/messages", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", p.opts.APIKey)
	req.Header.Set("anthropic-version", p.opts.Version)
	return req, nil
}

func (p *Provider) parseError(status int, body []byte) *provider.Error {
	var er errorResponse
	if err := json.Unmarshal(body, &er); err == nil && er.Error.Message != "" {
		return provider.NewError(p.name, status, er.Error.Type, er.Error.Message)
	}
	return provider.NewError(p.name, status, "upstream_error", truncate(string(body)))
}

// Complete implements provider.Provider.
func (p *Provider) Complete(ctx context.Context, req *chat.Request) (*chat.Response, error) {
	mr, err := toMessagesRequest(p.name, req, p.opts.DefaultMaxTokens)
	if err != nil {
		return nil, err
	}
	mr.Stream = false
	hreq, err := p.newRequest(ctx, mr)
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	resp, err := provider.DoJSON(p.opts.HTTPClient, hreq, p.name, p.parseError)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out messagesResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&out); err != nil {
		return nil, provider.NewError(p.name, 502, "bad_response", fmt.Sprintf("decoding response: %v", err))
	}
	return fromMessagesResponse(&out, p.opts.Now()), nil
}

// Stream implements provider.Provider.
func (p *Provider) Stream(ctx context.Context, req *chat.Request) (<-chan provider.Chunk, error) {
	mr, err := toMessagesRequest(p.name, req, p.opts.DefaultMaxTokens)
	if err != nil {
		return nil, err
	}
	mr.Stream = true
	hreq, err := p.newRequest(ctx, mr)
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	hreq.Header.Set("Accept", "text/event-stream")
	resp, err := provider.DoJSON(p.opts.HTTPClient, hreq, p.name, p.parseError)
	if err != nil {
		return nil, err
	}
	out := make(chan provider.Chunk, 16)
	go translateStream(ctx, p.name, resp.Body, p.opts.Now(), out)
	return out, nil
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
