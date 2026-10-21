// Package chat defines the OpenAI chat-completions wire format. The gateway
// speaks it on its public edge and every provider translates to and from it,
// so the types here are the lingua franca of the whole binary.
package chat

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Request is POST /v1/chat/completions. Fields the gateway does not interpret
// are kept verbatim in Extra so an OpenAI-compatible upstream still receives
// them (tools, response_format, logit_bias, ...).
type Request struct {
	Model               string         `json:"model"`
	Messages            []Message      `json:"messages"`
	MaxTokens           *int           `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int           `json:"max_completion_tokens,omitempty"`
	Temperature         *float64       `json:"temperature,omitempty"`
	TopP                *float64       `json:"top_p,omitempty"`
	N                   *int           `json:"n,omitempty"`
	Stop                Stop           `json:"stop,omitempty"`
	Stream              bool           `json:"stream,omitempty"`
	StreamOptions       *StreamOptions `json:"stream_options,omitempty"`
	User                string         `json:"user,omitempty"`

	Extra map[string]json.RawMessage `json:"-"`
}

// StreamOptions mirrors OpenAI's stream_options object.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// knownRequestKeys are the JSON keys mapped onto Request's typed fields.
var knownRequestKeys = map[string]bool{
	"model": true, "messages": true, "max_tokens": true, "max_completion_tokens": true,
	"temperature": true, "top_p": true, "n": true, "stop": true, "stream": true,
	"stream_options": true, "user": true,
}

type requestAlias Request

// UnmarshalJSON decodes the typed fields and stashes everything else in Extra.
func (r *Request) UnmarshalJSON(b []byte) error {
	var a requestAlias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(b, &all); err != nil {
		return err
	}
	for k := range all {
		if knownRequestKeys[k] {
			delete(all, k)
		}
	}
	a.Extra = nil
	if len(all) > 0 {
		a.Extra = all
	}
	*r = Request(a)
	return nil
}

// MarshalJSON emits the typed fields plus Extra. Typed fields win on conflict.
func (r Request) MarshalJSON() ([]byte, error) {
	typed, err := json.Marshal(requestAlias(r))
	if err != nil {
		return nil, err
	}
	if len(r.Extra) == 0 {
		return typed, nil
	}
	var merged map[string]json.RawMessage
	if err := json.Unmarshal(typed, &merged); err != nil {
		return nil, err
	}
	for k, v := range r.Extra {
		if _, ok := merged[k]; !ok {
			merged[k] = v
		}
	}
	return json.Marshal(merged)
}

// MaxOutputTokens returns the caller's output cap, preferring the newer
// max_completion_tokens field, or 0 when neither is set.
func (r *Request) MaxOutputTokens() int {
	if r.MaxCompletionTokens != nil {
		return *r.MaxCompletionTokens
	}
	if r.MaxTokens != nil {
		return *r.MaxTokens
	}
	return 0
}

// Stop accepts OpenAI's "string or array of strings" and always marshals as an
// array, which every upstream accepts.
type Stop []string

// UnmarshalJSON accepts a JSON string, an array of strings or null.
func (s *Stop) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*s = nil
		return nil
	}
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*s = Stop{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return fmt.Errorf("stop: expected string or array of strings")
	}
	*s = Stop(many)
	return nil
}

// Message is one turn. Content may be a plain string or an array of typed
// parts; see Content.
type Message struct {
	Role       string          `json:"role"`
	Content    Content         `json:"content"`
	Name       string          `json:"name,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

// Content is either a string or an array of content parts. The gateway only
// understands text parts; other part types are preserved for passthrough and
// rejected by translating providers.
type Content struct {
	Text  string
	Parts []Part
	// IsParts records that the original JSON was an array so a round trip
	// re-emits the same shape even when Parts is empty.
	IsParts bool
}

// Part is one element of an array-form content field.
type Part struct {
	Type string          `json:"type"`
	Text string          `json:"text,omitempty"`
	Raw  json.RawMessage `json:"-"`
}

// UnmarshalJSON accepts a string, null, or an array of parts.
func (c *Content) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*c = Content{}
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*c = Content{Text: s}
		return nil
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(b, &raws); err != nil {
		return errors.New("content: expected string or array of parts")
	}
	parts := make([]Part, 0, len(raws))
	for _, raw := range raws {
		var p Part
		if err := json.Unmarshal(raw, &p); err != nil {
			return fmt.Errorf("content part: %w", err)
		}
		p.Raw = raw
		parts = append(parts, p)
	}
	*c = Content{Parts: parts, IsParts: true}
	return nil
}

// MarshalJSON re-emits the original shape: raw parts when present, else text.
func (c Content) MarshalJSON() ([]byte, error) {
	if !c.IsParts {
		return json.Marshal(c.Text)
	}
	raws := make([]json.RawMessage, 0, len(c.Parts))
	for _, p := range c.Parts {
		if p.Raw != nil {
			raws = append(raws, p.Raw)
			continue
		}
		b, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		raws = append(raws, b)
	}
	return json.Marshal(raws)
}

// PlainText returns the textual content: the string form, or all text parts
// joined. ok is false when a non-text part is present.
func (c Content) PlainText() (text string, ok bool) {
	if !c.IsParts {
		return c.Text, true
	}
	var sb strings.Builder
	ok = true
	for _, p := range c.Parts {
		if p.Type != "text" {
			ok = false
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(p.Text)
	}
	return sb.String(), ok
}

// Response is a non-streaming chat completion.
type Response struct {
	ID                string   `json:"id"`
	Object            string   `json:"object"`
	Created           int64    `json:"created"`
	Model             string   `json:"model"`
	Choices           []Choice `json:"choices"`
	Usage             *Usage   `json:"usage,omitempty"`
	SystemFingerprint string   `json:"system_fingerprint,omitempty"`

	// Raw, when set by a passthrough provider, is the upstream body verbatim
	// and is what the gateway writes to the client.
	Raw json.RawMessage `json:"-"`
}

// Choice is one completion candidate.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason *string `json:"finish_reason"`
}

// Chunk is one streamed chat.completion.chunk object.
type Chunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`

	Raw json.RawMessage `json:"-"`
}

// ChunkChoice is one streamed delta.
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// Delta carries the incremental fields of a streamed choice.
type Delta struct {
	Role      string          `json:"role,omitempty"`
	Content   *string         `json:"content,omitempty"`
	ToolCalls json.RawMessage `json:"tool_calls,omitempty"`
}

// Usage is OpenAI's token accounting object.
type Usage struct {
	PromptTokens        int                  `json:"prompt_tokens"`
	CompletionTokens    int                  `json:"completion_tokens"`
	TotalTokens         int                  `json:"total_tokens"`
	PromptTokensDetails *PromptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

// PromptTokensDetails breaks prompt tokens down; CachedTokens were served from
// the provider's prompt cache.
type PromptTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

// ErrorResponse is the OpenAI error envelope.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody is the inner error object.
type ErrorBody struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    *string `json:"code"`
}

// Model is one entry of GET /v1/models.
type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ModelList is GET /v1/models.
type ModelList struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}

// StrPtr is a small helper for the pointer-typed finish_reason fields.
func StrPtr(s string) *string { return &s }
