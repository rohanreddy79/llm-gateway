package ratelimit

import (
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestBucketTakeAndRefill(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := NewBucket(2, 60, c.now) // 1 token per second
	for i := 0; i < 2; i++ {
		if ok, _ := b.Take(1); !ok {
			t.Fatalf("take %d should succeed", i)
		}
	}
	ok, wait := b.Take(1)
	if ok || wait != time.Second {
		t.Fatalf("empty bucket: ok=%v wait=%v", ok, wait)
	}
	c.advance(500 * time.Millisecond)
	if ok, wait := b.Take(1); ok || wait != 500*time.Millisecond {
		t.Fatalf("half refilled: ok=%v wait=%v", ok, wait)
	}
	c.advance(500 * time.Millisecond)
	if ok, _ := b.Take(1); !ok {
		t.Fatal("should have refilled one token")
	}
	c.advance(time.Hour)
	if r := b.Remaining(); r != 2 {
		t.Fatalf("refill must cap at capacity, got %v", r)
	}
}

func TestBucketDebitGoesNegative(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := NewBucket(100, 6000, c.now) // 100 tokens/s
	b.Debit(250)
	if r := b.Remaining(); r != 0 {
		t.Fatalf("Remaining = %v, want 0", r)
	}
	if w := b.Wait(1); w != 1510*time.Millisecond {
		t.Fatalf("Wait = %v, want 1.51s", w)
	}
	c.advance(2 * time.Second)
	if w := b.Wait(1); w != 0 {
		t.Fatalf("after refill Wait = %v", w)
	}
}

func TestLimiterRPM(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := New(c.now)
	lim := Limit{RPM: 2}
	for i := 0; i < 2; i++ {
		if d := l.Allow("k", lim); !d.Allowed {
			t.Fatalf("request %d rejected", i)
		}
	}
	d := l.Allow("k", lim)
	if d.Allowed || d.Kind != "rpm" || d.RetryAfter != 30*time.Second || d.RemainingRequests != 0 {
		t.Fatalf("decision = %+v", d)
	}
	if d.RemainingTokens != -1 {
		t.Fatalf("unlimited TPM should report -1, got %d", d.RemainingTokens)
	}
	// Other keys are independent.
	if d := l.Allow("other", lim); !d.Allowed {
		t.Fatal("other key must have its own bucket")
	}
}

func TestLimiterTPMDebt(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := New(c.now)
	lim := Limit{RPM: 100, TPM: 600} // 10 tokens/s
	if d := l.Allow("k", lim); !d.Allowed {
		t.Fatal("first request allowed")
	}
	l.RecordTokens("k", lim, 650) // 50 in debt
	d := l.Allow("k", lim)
	if d.Allowed || d.Kind != "tpm" || d.RetryAfter != 5100*time.Millisecond {
		t.Fatalf("decision = %+v", d)
	}
	if d.RemainingRequests != 99 {
		t.Fatalf("a TPM rejection must not consume an RPM token: %+v", d)
	}
	c.advance(6 * time.Second)
	if d := l.Allow("k", lim); !d.Allowed {
		t.Fatalf("should be allowed after refill: %+v", d)
	}
}

func TestLimiterUnlimited(t *testing.T) {
	l := New(nil)
	for i := 0; i < 1000; i++ {
		if d := l.Allow("k", Limit{}); !d.Allowed {
			t.Fatal("unlimited key rejected")
		}
	}
	l.RecordTokens("k", Limit{}, 1<<30)
	if d := l.Snapshot("k", Limit{}); !d.Allowed || d.RemainingRequests != -1 {
		t.Fatalf("snapshot = %+v", d)
	}
}

func TestLimiterConcurrent(t *testing.T) {
	l := New(nil)
	lim := Limit{RPM: 50, TPM: 1 << 20}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("k", lim).Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
			l.RecordTokens("k", lim, 10)
		}()
	}
	wg.Wait()
	// Refill during the test can admit at most a handful extra.
	if allowed < 50 || allowed > 55 {
		t.Fatalf("allowed = %d, want ~50", allowed)
	}
}
