package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/rohanreddy79/llm-gateway/internal/breaker"
	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
	"github.com/rohanreddy79/llm-gateway/internal/router"
)

// writeJSON encodes v with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

// writeError emits the OpenAI error envelope.
func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	var codePtr *string
	if code != "" {
		codePtr = &code
	}
	writeJSON(w, status, chat.ErrorResponse{Error: chat.ErrorBody{Message: msg, Type: typ, Code: codePtr}})
}

// upstreamError maps a routing failure onto a client-facing status, type
// and code. Upstream credential problems are the gateway operator's fault,
// so they surface as 502 rather than confusing the caller with a 401.
func upstreamError(err error) (status int, typ, code, msg string) {
	var pe *provider.Error
	hasPE := errors.As(err, &pe)
	detail := err.Error()
	if hasPE {
		detail = fmt.Sprintf("[%s] %s", pe.Provider, pe.Message)
	}
	switch {
	case errors.Is(err, router.ErrUnknownModel):
		return http.StatusNotFound, "invalid_request_error", "model_not_found", detail
	case errors.Is(err, breaker.ErrOpen):
		return http.StatusServiceUnavailable, "server_error", "circuit_open", "All providers for this model are unavailable (circuit breaker open). Retry later."
	case errors.Is(err, context.DeadlineExceeded) || (hasPE && pe.Status == 504):
		return http.StatusGatewayTimeout, "server_error", "upstream_timeout", "The upstream provider did not respond in time: " + detail
	case errors.Is(err, context.Canceled):
		return 499, "server_error", "client_closed_request", "client closed request"
	case !hasPE:
		return http.StatusBadGateway, "server_error", "upstream_error", detail
	case pe.Status == 400:
		return http.StatusBadRequest, "invalid_request_error", "upstream_rejected", detail
	case pe.Status == 429:
		return http.StatusTooManyRequests, "rate_limit_error", "upstream_rate_limited", "The upstream provider is rate limiting this gateway: " + detail
	case pe.Status == 401 || pe.Status == 403:
		return http.StatusBadGateway, "server_error", "upstream_auth", "The upstream provider rejected the gateway's credentials: " + detail
	case pe.Status == 404:
		return http.StatusBadGateway, "server_error", "upstream_model_not_found", detail
	case pe.Status == 529 || pe.Status == 503:
		return http.StatusServiceUnavailable, "server_error", "upstream_overloaded", detail
	case pe.Status >= 500:
		return http.StatusBadGateway, "server_error", "upstream_error", detail
	default:
		return pe.Status, "invalid_request_error", "upstream_rejected", detail
	}
}
