package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/config"
	"github.com/rohanreddy79/llm-gateway/internal/ratelimit"
)

// reqInfo is filled in progressively by the layers and read by the logger
// once the request is done. It is only touched by the handler goroutine.
type reqInfo struct {
	ID        string
	keySecret string
	Key       config.KeyConfig
	KeyName   string
	Model     string
	Provider  string
	Attempts  int
	Fallbacks int
	Cache     string
	Usage     *chat.Usage
	Err       string
}

type ctxKey struct{}

func infoFrom(ctx context.Context) *reqInfo {
	if ri, ok := ctx.Value(ctxKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{}
}

// statusWriter records the status for logging and keeps Flush working for
// streaming responses (http.ResponseController finds it through Unwrap).
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// requestID honours an incoming X-Request-ID or mints one.
func (g *Gateway) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			var b [8]byte
			rand.Read(b[:])
			id = hex.EncodeToString(b[:])
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), ctxKey{}, &reqInfo{ID: id})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// logging emits one structured line per request and feeds the request
// counter and latency histogram for chat completions.
func (g *Gateway) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := g.now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		info := infoFrom(r.Context())
		elapsed := g.now().Sub(start)
		attrs := []any{
			"method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration_ms", float64(elapsed.Microseconds()) / 1000, "request_id", info.ID,
		}
		if info.KeyName != "" {
			attrs = append(attrs, "key", info.KeyName)
		}
		if info.Model != "" {
			attrs = append(attrs, "model", info.Model, "provider", orUnknown(info.Provider),
				"attempts", info.Attempts, "fallbacks", info.Fallbacks)
		}
		if info.Cache != "" {
			attrs = append(attrs, "cache", info.Cache)
		}
		if info.Usage != nil {
			attrs = append(attrs, "tokens_in", info.Usage.PromptTokens, "tokens_out", info.Usage.CompletionTokens)
		}
		if info.Err != "" {
			attrs = append(attrs, "error", info.Err)
		}
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
			g.log.Debug("request", attrs...)
		} else {
			g.log.Info("request", attrs...)
		}
		if r.URL.Path == "/v1/chat/completions" {
			g.metrics.Requests.WithLabelValues(orUnknown(info.KeyName), orUnknown(info.Model), orUnknown(info.Provider), strconv.Itoa(sw.status)).Inc()
			g.metrics.Duration.WithLabelValues(orUnknown(info.Model), orUnknown(info.Provider)).Observe(elapsed.Seconds())
		}
	})
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// auth resolves the virtual key from Authorization: Bearer or x-api-key.
func (g *Gateway) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer"))
		if secret == "" {
			secret = r.Header.Get("x-api-key")
		}
		k, ok := g.cfg.Keys[secret]
		if secret == "" || !ok {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "Incorrect API key provided. Pass a configured virtual key as Authorization: Bearer <key>.")
			return
		}
		info := infoFrom(r.Context())
		info.keySecret, info.Key, info.KeyName = secret, k, k.Name
		next.ServeHTTP(w, r)
	})
}

// rateLimit enforces the key's RPM bucket and TPM debt before any work.
func (g *Gateway) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := infoFrom(r.Context())
		lim := ratelimit.Limit{RPM: info.Key.RPM, TPM: info.Key.TPM}
		d := g.limiter.Allow(info.keySecret, lim)
		setRateLimitHeaders(w.Header(), d, lim)
		if !d.Allowed {
			secs := int(math.Ceil(d.RetryAfter.Seconds()))
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			g.metrics.RateLimited.WithLabelValues(info.KeyName, d.Kind).Inc()
			info.Err = "rate limited (" + d.Kind + ")"
			writeError(w, http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded",
				fmt.Sprintf("Rate limit exceeded for key %q (%s). Retry after %s.", info.KeyName, limitDescription(d.Kind, lim), d.RetryAfter.Round(time.Millisecond)))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func limitDescription(kind string, lim ratelimit.Limit) string {
	if kind == "tpm" {
		return fmt.Sprintf("%d tokens per minute", lim.TPM)
	}
	return fmt.Sprintf("%d requests per minute", lim.RPM)
}

// setRateLimitHeaders mirrors the OpenAI header names.
func setRateLimitHeaders(h http.Header, d ratelimit.Decision, lim ratelimit.Limit) {
	if lim.RPM > 0 {
		h.Set("X-RateLimit-Limit-Requests", strconv.Itoa(lim.RPM))
		h.Set("X-RateLimit-Remaining-Requests", strconv.Itoa(d.RemainingRequests))
		h.Set("X-RateLimit-Reset-Requests", d.ResetRequests.Round(time.Millisecond).String())
	}
	if lim.TPM > 0 {
		h.Set("X-RateLimit-Limit-Tokens", strconv.Itoa(lim.TPM))
		h.Set("X-RateLimit-Remaining-Tokens", strconv.Itoa(d.RemainingTokens))
		h.Set("X-RateLimit-Reset-Tokens", d.ResetTokens.Round(time.Millisecond).String())
	}
}

// adminOnly gates the admin endpoints on the key's admin flag.
func (g *Gateway) adminOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !infoFrom(r.Context()).Key.Admin {
			writeError(w, http.StatusForbidden, "permission_error", "admin_required", "This endpoint requires a virtual key with admin: true.")
			return
		}
		next.ServeHTTP(w, r)
	})
}
