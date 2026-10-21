// Package retry runs an attempt function with capped exponential backoff and
// jitter. Only errors that provider.IsRetryable accepts are retried, and a
// cancelled parent context always stops the loop.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

// Policy configures Do. Rand and Sleep are injection points for tests.
type Policy struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// Rand returns a float in [0,1); defaults to math/rand/v2.
	Rand func() float64
	// Sleep waits d or until ctx is done; defaults to a timer.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Backoff computes the wait before retrying after the given zero-based
// attempt: base*2^attempt capped at max, with "equal jitter" (half fixed,
// half random) so synchronised clients spread out. A server Retry-After hint
// raises the wait, up to the cap.
func (p Policy) Backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := p.BaseBackoff
	for i := 0; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	if d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	r := rand.Float64
	if p.Rand != nil {
		r = p.Rand
	}
	d = d/2 + time.Duration(r()*float64(d/2))
	if retryAfter > d {
		d = retryAfter
		if d > p.MaxBackoff {
			d = p.MaxBackoff
		}
	}
	return d
}

// Do calls fn until it succeeds, returns a non-retryable error, the parent
// context ends, or MaxAttempts is reached. It returns the number of attempts
// made and the last error.
func (p Policy) Do(ctx context.Context, fn func(ctx context.Context, attempt int) error) (attempts int, err error) {
	max := p.MaxAttempts
	if max < 1 {
		max = 1
	}
	sleep := p.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for attempt := 0; attempt < max; attempt++ {
		attempts = attempt + 1
		err = fn(ctx, attempt)
		if err == nil {
			return attempts, nil
		}
		if ctx.Err() != nil || !provider.IsRetryable(err) || attempt == max-1 {
			return attempts, err
		}
		var pe *provider.Error
		var hint time.Duration
		if errors.As(err, &pe) {
			hint = pe.RetryAfter
		}
		if serr := sleep(ctx, p.Backoff(attempt, hint)); serr != nil {
			return attempts, err
		}
	}
	return attempts, err
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
