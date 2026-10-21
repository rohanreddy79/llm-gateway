package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

func fixedPolicy(sleeps *[]time.Duration) Policy {
	return Policy{
		MaxAttempts: 3,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  time.Second,
		Rand:        func() float64 { return 0.5 },
		Sleep: func(ctx context.Context, d time.Duration) error {
			*sleeps = append(*sleeps, d)
			return ctx.Err()
		},
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	p := Policy{BaseBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, Rand: func() float64 { return 0 }}
	want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond, 500 * time.Millisecond}
	for i, w := range want {
		if got := p.Backoff(i, 0); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", i, got, w)
		}
	}
	p.Rand = func() float64 { return 0.999 }
	if got := p.Backoff(0, 0); got < 99*time.Millisecond || got > 100*time.Millisecond {
		t.Errorf("max jitter Backoff(0) = %v", got)
	}
	// Retry-After raises the wait but is capped.
	p.Rand = func() float64 { return 0 }
	if got := p.Backoff(0, 300*time.Millisecond); got != 300*time.Millisecond {
		t.Errorf("Retry-After hint ignored: %v", got)
	}
	if got := p.Backoff(0, time.Hour); got != time.Second {
		t.Errorf("Retry-After must be capped: %v", got)
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	var sleeps []time.Duration
	p := fixedPolicy(&sleeps)
	calls := 0
	n, err := p.Do(context.Background(), func(ctx context.Context, attempt int) error {
		calls++
		if attempt < 2 {
			return provider.NewError("p", 503, "api_error", "down")
		}
		return nil
	})
	if err != nil || n != 3 || calls != 3 {
		t.Fatalf("n=%d calls=%d err=%v", n, calls, err)
	}
	if len(sleeps) != 2 || sleeps[0] != 75*time.Millisecond || sleeps[1] != 150*time.Millisecond {
		t.Fatalf("sleeps = %v", sleeps)
	}
}

func TestDoGivesUpAfterMax(t *testing.T) {
	var sleeps []time.Duration
	p := fixedPolicy(&sleeps)
	n, err := p.Do(context.Background(), func(context.Context, int) error {
		return provider.NewError("p", 429, "rate_limit_error", "slow down")
	})
	if n != 3 || provider.StatusOf(err) != 429 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestDoStopsOnNonRetryable(t *testing.T) {
	var sleeps []time.Duration
	p := fixedPolicy(&sleeps)
	n, err := p.Do(context.Background(), func(context.Context, int) error {
		return provider.BadRequest("p", "nope")
	})
	if n != 1 || provider.StatusOf(err) != 400 || len(sleeps) != 0 {
		t.Fatalf("n=%d err=%v sleeps=%v", n, err, sleeps)
	}
}

func TestDoStopsWhenContextCancelled(t *testing.T) {
	var sleeps []time.Duration
	p := fixedPolicy(&sleeps)
	ctx, cancel := context.WithCancel(context.Background())
	n, err := p.Do(ctx, func(context.Context, int) error {
		cancel()
		return provider.NewError("p", 500, "api_error", "boom")
	})
	if n != 1 || provider.StatusOf(err) != 500 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestDoHonoursRetryAfter(t *testing.T) {
	var sleeps []time.Duration
	p := fixedPolicy(&sleeps)
	e := provider.NewError("p", 429, "rate_limit_error", "slow")
	e.RetryAfter = 400 * time.Millisecond
	p.Do(context.Background(), func(context.Context, int) error { return e })
	if len(sleeps) != 2 || sleeps[0] != 400*time.Millisecond {
		t.Fatalf("sleeps = %v", sleeps)
	}
}

func TestIsRetryable(t *testing.T) {
	cases := map[error]bool{
		provider.NewError("p", 503, "", ""):   true,
		provider.NewError("p", 529, "", ""):   true,
		provider.NewError("p", 429, "", ""):   true,
		provider.NewError("p", 408, "", ""):   true,
		provider.NewError("p", 400, "", ""):   false,
		provider.NewError("p", 401, "", ""):   false,
		context.DeadlineExceeded:              true,
		context.Canceled:                      false,
		errors.New("something else"):          false,
		provider.NewError("p", 404, "nf", ""): false,
	}
	for err, want := range cases {
		if got := provider.IsRetryable(err); got != want {
			t.Errorf("IsRetryable(%v) = %v, want %v", err, got, want)
		}
	}
	if provider.StatusOf(errors.New("x")) != 502 || provider.StatusOf(context.DeadlineExceeded) != 504 {
		t.Error("StatusOf defaults")
	}
}
