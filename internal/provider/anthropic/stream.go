package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
	"github.com/rohanreddy79/llm-gateway/internal/sse"
)

// streamTranslator folds Messages API SSE events into OpenAI chunks.
//
// Event sequence: message_start (id, model, input usage) -> content_block_*
// (text deltas) -> message_delta (stop_reason, cumulative output usage) ->
// message_stop. The OpenAI shape needs usage on the final chunk, which is why
// usage is accumulated across events and only emitted at message_stop.
type streamTranslator struct {
	name    string
	created int64
	id      string
	model   string
	usage   usage
	stop    string
	started bool
	done    bool
}

func newStreamTranslator(name string, now time.Time) *streamTranslator {
	return &streamTranslator{name: name, created: now.Unix()}
}

func (t *streamTranslator) chunk() *chat.Chunk {
	return &chat.Chunk{ID: t.id, Object: "chat.completion.chunk", Created: t.created, Model: t.model}
}

// handle translates one event into zero or more chunks. done becomes true at
// message_stop; err carries a mid-stream upstream error.
func (t *streamTranslator) handle(ev sse.Event) (out []*chat.Chunk, err error) {
	var e streamEvent
	if err := json.Unmarshal([]byte(ev.Data), &e); err != nil {
		return nil, provider.NewError(t.name, 502, "bad_stream", fmt.Sprintf("malformed stream event: %v", err))
	}
	switch e.Type {
	case "message_start":
		if e.Message != nil {
			t.id, t.model, t.usage = e.Message.ID, e.Message.Model, e.Message.Usage
		}
		t.started = true
		c := t.chunk()
		c.Choices = []chat.ChunkChoice{{Delta: chat.Delta{Role: "assistant", Content: chat.StrPtr("")}}}
		return []*chat.Chunk{c}, nil
	case "content_block_start":
		if e.ContentBlock != nil && e.ContentBlock.Type == "text" && e.ContentBlock.Text != "" {
			return []*chat.Chunk{t.text(e.ContentBlock.Text)}, nil
		}
	case "content_block_delta":
		if e.Delta != nil && e.Delta.Type == "text_delta" && e.Delta.Text != "" {
			return []*chat.Chunk{t.text(e.Delta.Text)}, nil
		}
	case "message_delta":
		if e.Delta != nil && e.Delta.StopReason != "" {
			t.stop = e.Delta.StopReason
		}
		if e.Usage != nil {
			t.usage.OutputTokens = e.Usage.OutputTokens
			// Newer API versions repeat cumulative input usage here; prefer it.
			if e.Usage.InputTokens > 0 {
				t.usage.InputTokens = e.Usage.InputTokens
				t.usage.CacheCreationInputTokens = e.Usage.CacheCreationInputTokens
				t.usage.CacheReadInputTokens = e.Usage.CacheReadInputTokens
			}
		}
	case "message_stop":
		t.done = true
		return []*chat.Chunk{t.final()}, nil
	case "error":
		msg := "unknown stream error"
		typ := "api_error"
		if e.Error != nil {
			msg, typ = e.Error.Message, e.Error.Type
		}
		return nil, provider.NewError(t.name, statusForType(typ), typ, msg)
	}
	// ping, content_block_stop and unknown block types carry nothing to forward.
	return nil, nil
}

func (t *streamTranslator) text(s string) *chat.Chunk {
	c := t.chunk()
	c.Choices = []chat.ChunkChoice{{Delta: chat.Delta{Content: chat.StrPtr(s)}}}
	return c
}

func (t *streamTranslator) final() *chat.Chunk {
	c := t.chunk()
	c.Choices = []chat.ChunkChoice{{FinishReason: chat.StrPtr(finishReason(t.stop))}}
	c.Usage = toUsage(t.usage)
	return c
}

// statusForType maps error types seen in the stream to HTTP statuses.
func statusForType(typ string) int {
	switch typ {
	case "overloaded_error":
		return 529
	case "rate_limit_error":
		return 429
	case "invalid_request_error":
		return 400
	case "authentication_error":
		return 401
	case "permission_error":
		return 403
	case "not_found_error":
		return 404
	default:
		return 500
	}
}

// translateStream reads SSE from body and sends OpenAI chunks on out until
// message_stop, an error, or ctx is done. It closes out and body.
func translateStream(ctx context.Context, name string, body io.ReadCloser, now time.Time, out chan<- provider.Chunk) {
	defer close(out)
	defer body.Close()
	t := newStreamTranslator(name, now)
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
			if err == io.EOF && t.done {
				return
			}
			if ctx.Err() != nil {
				send(provider.Chunk{Err: ctx.Err()})
				return
			}
			if t.started && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				// Upstream hung up without message_stop: close the client
				// stream honestly with what we know rather than hang.
				send(provider.Chunk{Err: provider.NewError(name, 502, "bad_stream", "upstream closed the stream before message_stop")})
				return
			}
			send(provider.Chunk{Err: provider.NewError(name, 502, "bad_stream", fmt.Sprintf("reading stream: %v", err))})
			return
		}
		chunks, herr := t.handle(ev)
		if herr != nil {
			send(provider.Chunk{Err: herr})
			return
		}
		for _, c := range chunks {
			if !send(provider.Chunk{Data: c}) {
				return
			}
		}
		if t.done {
			return
		}
	}
}
