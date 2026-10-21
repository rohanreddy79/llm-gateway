package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// DoJSON sends req with the given client and returns the response when the
// status is 2xx. Any other status is turned into *Error using parseErr to
// read the body; transport failures are wrapped so IsRetryable can see
// timeouts.
func DoJSON(client *http.Client, req *http.Request, providerName string, parseErr func(status int, body []byte) *Error) (*http.Response, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, wrapTransport(req.Context(), providerName, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	pe := parseErr(resp.StatusCode, body)
	if pe == nil {
		pe = NewError(providerName, resp.StatusCode, "upstream_error", http.StatusText(resp.StatusCode))
	}
	pe.Provider = providerName
	pe.Status = resp.StatusCode
	pe.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	return nil, pe
}

func wrapTransport(ctx context.Context, providerName string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return &Error{Provider: providerName, Status: 504, Type: "timeout", Message: err.Error()}
	}
	return &Error{Provider: providerName, Status: 502, Type: "connection_error", Message: fmt.Sprintf("upstream unreachable: %v", err)}
}

// parseRetryAfter understands the delay-seconds form; HTTP-date is rare from
// LLM APIs and is ignored.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	secs, err := strconv.ParseFloat(v, 64)
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(secs * float64(time.Second))
}
