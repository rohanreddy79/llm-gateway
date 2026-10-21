// Package ratelimit implements per-key token buckets for requests per minute
// and tokens per minute. Token usage is only known after a completion
// finishes, so the TPM bucket is debited after the fact and may go negative;
// a key in debt is rejected until the bucket refills past zero.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

// Bucket is a token bucket with continuous refill.
type Bucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	rate     float64 // tokens per second
	last     time.Time
	now      func() time.Time
}

// NewBucket creates a full bucket holding capacity tokens refilled at
// perMinute/60 per second.
func NewBucket(capacity float64, perMinute float64, now func() time.Time) *Bucket {
	if now == nil {
		now = time.Now
	}
	return &Bucket{capacity: capacity, tokens: capacity, rate: perMinute / 60, last: now(), now: now}
}

func (b *Bucket) refill() {
	t := b.now()
	if elapsed := t.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.rate)
		b.last = t
	}
}

// Take removes n tokens if at least n are available. When it cannot, wait is
// how long until it could.
func (b *Bucket) Take(n float64) (ok bool, wait time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= n {
		b.tokens -= n
		return true, 0
	}
	return false, b.waitFor(n)
}

// Debit removes n tokens unconditionally; the balance may become negative.
func (b *Bucket) Debit(n float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.tokens -= n
}

// Remaining is the non-negative balance.
func (b *Bucket) Remaining() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return math.Max(0, b.tokens)
}

// Wait is how long until n tokens are available (0 if they already are).
func (b *Bucket) Wait(n float64) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.waitFor(n)
}

func (b *Bucket) waitFor(n float64) time.Duration {
	if b.tokens >= n {
		return 0
	}
	if b.rate <= 0 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(math.Ceil((n-b.tokens)/b.rate*1e3)) * time.Millisecond
}

// Limit is a key's quota; zero means unlimited.
type Limit struct {
	RPM int
	TPM int
}

// Decision is the outcome of Allow plus the numbers for X-RateLimit headers.
type Decision struct {
	Allowed bool
	// Kind is "rpm" or "tpm" when rejected.
	Kind              string
	RetryAfter        time.Duration
	RemainingRequests int
	RemainingTokens   int
	ResetRequests     time.Duration
	ResetTokens       time.Duration
}

type keyBuckets struct {
	rpm *Bucket
	tpm *Bucket
}

// Limiter holds one pair of buckets per virtual key, created lazily.
type Limiter struct {
	mu   sync.Mutex
	keys map[string]*keyBuckets
	now  func() time.Time
}

// New returns a Limiter; now may be nil.
func New(now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{keys: make(map[string]*keyBuckets), now: now}
}

func (l *Limiter) buckets(key string, lim Limit) *keyBuckets {
	l.mu.Lock()
	defer l.mu.Unlock()
	kb, ok := l.keys[key]
	if !ok {
		kb = &keyBuckets{}
		if lim.RPM > 0 {
			kb.rpm = NewBucket(float64(lim.RPM), float64(lim.RPM), l.now)
		}
		if lim.TPM > 0 {
			kb.tpm = NewBucket(float64(lim.TPM), float64(lim.TPM), l.now)
		}
		l.keys[key] = kb
	}
	return kb
}

// Allow consumes one request from the key's RPM bucket and checks that the
// TPM bucket is not in debt.
func (l *Limiter) Allow(key string, lim Limit) Decision {
	kb := l.buckets(key, lim)
	d := Decision{Allowed: true, RemainingRequests: -1, RemainingTokens: -1}
	if kb.tpm != nil {
		// Check TPM first so a key in token debt does not burn an RPM token.
		if wait := kb.tpm.Wait(1); wait > 0 {
			d.Allowed, d.Kind, d.RetryAfter = false, "tpm", wait
		}
	}
	if d.Allowed && kb.rpm != nil {
		if ok, wait := kb.rpm.Take(1); !ok {
			d.Allowed, d.Kind, d.RetryAfter = false, "rpm", wait
		}
	}
	l.fill(&d, kb)
	return d
}

// RecordTokens debits a finished completion's tokens from the TPM bucket.
func (l *Limiter) RecordTokens(key string, lim Limit, tokens int) {
	if tokens <= 0 {
		return
	}
	if kb := l.buckets(key, lim); kb.tpm != nil {
		kb.tpm.Debit(float64(tokens))
	}
}

// Snapshot reports the current header values without consuming anything.
func (l *Limiter) Snapshot(key string, lim Limit) Decision {
	d := Decision{Allowed: true, RemainingRequests: -1, RemainingTokens: -1}
	l.fill(&d, l.buckets(key, lim))
	return d
}

func (l *Limiter) fill(d *Decision, kb *keyBuckets) {
	if kb.rpm != nil {
		d.RemainingRequests = int(kb.rpm.Remaining())
		d.ResetRequests = kb.rpm.Wait(1)
	}
	if kb.tpm != nil {
		d.RemainingTokens = int(kb.tpm.Remaining())
		d.ResetTokens = kb.tpm.Wait(1)
	}
}
