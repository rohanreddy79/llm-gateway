// Package provider defines the upstream abstraction: anything that can turn an
// OpenAI-format chat request into a response or a stream of chunks.
package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
)

// Provider is one upstream. Implementations must be safe for concurrent use.
type Provider interface {
	// Name is the configured provider name (the key in gateway.yaml).
	Name() string
	// Complete performs a non-streaming completion.
	Complete(ctx context.Context, req *chat.Request) (*chat.Response, error)
	// Stream starts a streaming completion. An error here means nothing was
	// sent to the caller yet and the request may be retried or re-routed.
	// After a successful return the channel yields chunks in order and is
	// closed after the final chunk (the one carrying finish_reason) or after a
	// chunk whose Err is set.
	Stream(ctx context.Context, req *chat.Request) (<-chan Chunk, error)
}

// Chunk is one element of a stream: exactly one of Data or Err is set.
type Chunk struct {
	Data *chat.Chunk
	Err  error
}

// Error is an upstream failure that maps to an HTTP status.
type Error struct {
	Provider   string
	Status     int
	Type       string
	Message    string
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: upstream status %d: %s", e.Provider, e.Status, e.Message)
}

// Retryable reports whether the same request may succeed if repeated: rate
// limits, timeouts and server-side failures (including Anthropic's 529).
func (e *Error) Retryable() bool {
	return e.Status == 429 || e.Status == 408 || e.Status >= 500
}

// NewError builds an Error for a provider.
func NewError(provider string, status int, typ, msg string) *Error {
	return &Error{Provider: provider, Status: status, Type: typ, Message: msg}
}

// BadRequest is the error providers return for input they cannot translate.
func BadRequest(provider, msg string) *Error {
	return NewError(provider, 400, "invalid_request_error", msg)
}

// IsRetryable classifies any error: a retryable *Error, an attempt timeout,
// a network timeout, or a connection dropped mid-response.
func IsRetryable(err error) bool {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Retryable()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

// StatusOf returns the HTTP status an error should surface as, defaulting to
// 502 for anything that is not a typed upstream error.
func StatusOf(err error) int {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Status
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 504
	}
	return 502
}
