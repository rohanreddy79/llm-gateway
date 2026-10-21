// Package openai forwards chat completions to any OpenAI-compatible endpoint.
// Bodies are passed through: unknown request fields reach the upstream and
// upstream response bytes reach the client verbatim.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
	"github.com/rohanreddy79/llm-gateway/internal/sse"
)

// Options configures a Provider.
type Options struct {
	// BaseURL includes the version prefix, e.g. https://api.openai.com/v1.
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// Provider implements provider.Provider.
type Provider struct {
	name string
	opts Options
}

// New returns a provider named name.
func New(name string, opts Options) *Provider {
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	return &Provider{name: name, opts: opts}
}

// Name implements provider.Provider.
func (p *Provider) Name() string { return p.name }

func (p *Provider) newRequest(ctx context.Context, req *chat.Request) (*http.Request, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.opts.BaseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Authorization", "Bearer "+p.opts.APIKey)
	return hreq, nil
}

func (p *Provider) parseError(status int, body []byte) *provider.Error {
	var er chat.ErrorResponse
	if err := json.Unmarshal(body, &er); err == nil && er.Error.Message != "" {
		return provider.NewError(p.name, status, er.Error.Type, er.Error.Message)
	}
	s := string(body)
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return provider.NewError(p.name, status, "upstream_error", s)
}

// Complete implements provider.Provider.
func (p *Provider) Complete(ctx context.Context, req *chat.Request) (*chat.Response, error) {
	r := *req
	r.Stream, r.StreamOptions = false, nil
	hreq, err := p.newRequest(ctx, &r)
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	resp, err := provider.DoJSON(p.opts.HTTPClient, hreq, p.name, p.parseError)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, provider.NewError(p.name, 502, "bad_response", fmt.Sprintf("reading response: %v", err))
	}
	var out chat.Response
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, provider.NewError(p.name, 502, "bad_response", fmt.Sprintf("decoding response: %v", err))
	}
	out.Raw = body
	return &out, nil
}

// Stream implements provider.Provider. stream_options.include_usage is forced
// on so the final chunk carries token counts for accounting.
func (p *Provider) Stream(ctx context.Context, req *chat.Request) (<-chan provider.Chunk, error) {
	r := *req
	r.Stream = true
	r.StreamOptions = &chat.StreamOptions{IncludeUsage: true}
	hreq, err := p.newRequest(ctx, &r)
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	hreq.Header.Set("Accept", "text/event-stream")
	resp, err := provider.DoJSON(p.opts.HTTPClient, hreq, p.name, p.parseError)
	if err != nil {
		return nil, err
	}
	out := make(chan provider.Chunk, 16)
	go p.forward(ctx, resp.Body, out)
	return out, nil
}

// forward relays upstream chunks until [DONE].
func (p *Provider) forward(ctx context.Context, body io.ReadCloser, out chan<- provider.Chunk) {
	defer close(out)
	defer body.Close()
	rd := sse.NewReader(body)
	// send prefers delivery when the buffer has room so a final error chunk
	// still reaches a consumer after ctx is cancelled, and never blocks a
	// consumer that has gone away.
	send := func(c provider.Chunk) bool {
		select {
		case out <- c:
			return true
		default:
		}
		select {
		case out <- c:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		ev, err := rd.Next()
		if err != nil {
			if ctx.Err() != nil {
				send(provider.Chunk{Err: ctx.Err()})
				return
			}
			send(provider.Chunk{Err: provider.NewError(p.name, 502, "bad_stream", fmt.Sprintf("upstream stream ended before [DONE]: %v", err))})
			return
		}
		if ev.Data == "[DONE]" {
			return
		}
		var c chat.Chunk
		if err := json.Unmarshal([]byte(ev.Data), &c); err != nil {
			send(provider.Chunk{Err: provider.NewError(p.name, 502, "bad_stream", fmt.Sprintf("malformed chunk: %v", err))})
			return
		}
		var er chat.ErrorResponse
		if json.Unmarshal([]byte(ev.Data), &er) == nil && er.Error.Message != "" {
			send(provider.Chunk{Err: provider.NewError(p.name, 502, er.Error.Type, er.Error.Message)})
			return
		}
		c.Raw = json.RawMessage(ev.Data)
		if !send(provider.Chunk{Data: &c}) {
			return
		}
	}
}
