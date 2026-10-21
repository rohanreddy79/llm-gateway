// Package breaker is a per-provider circuit breaker. After N consecutive
// retryable failures the circuit opens and requests are rejected without
// touching the upstream; after the open window a single probe is let through
// (half-open) and its result decides whether to close or re-open.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// State of the circuit.
type State int

// Circuit states, ordered so the Prometheus gauge is meaningful.
const (
	Closed State = iota
	HalfOpen
	Open
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half-open"
	case Open:
		return "open"
	}
	return "unknown"
}

// ErrOpen is returned by callers when Allow refuses a request.
var ErrOpen = errors.New("circuit breaker is open")

// Breaker is safe for concurrent use.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	openFor   time.Duration
	now       func() time.Time

	state       State
	consecutive int
	openedAt    time.Time
	// probeAt is when the half-open probe was admitted; a probe that never
	// reports back (client disconnect) is abandoned after openFor.
	probeAt  time.Time
	onChange func(State)
}

// New returns a closed breaker. onChange, if non-nil, is called with the lock
// held on every transition (keep it cheap: it feeds a gauge).
func New(threshold int, openFor time.Duration, now func() time.Time, onChange func(State)) *Breaker {
	if now == nil {
		now = time.Now
	}
	if threshold < 1 {
		threshold = 1
	}
	return &Breaker{threshold: threshold, openFor: openFor, now: now, onChange: onChange}
}

func (b *Breaker) setState(s State) {
	if b.state == s {
		return
	}
	b.state = s
	if b.onChange != nil {
		b.onChange(s)
	}
}

// Allow reports whether a request may proceed. Every admitted request must be
// followed by exactly one Success or Failure.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case Closed:
		return true
	case Open:
		if now.Sub(b.openedAt) < b.openFor {
			return false
		}
		b.setState(HalfOpen)
		b.probeAt = now
		return true
	default: // HalfOpen: one probe in flight
		if now.Sub(b.probeAt) < b.openFor {
			return false
		}
		b.probeAt = now
		return true
	}
}

// Success records a healthy response and closes the circuit.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.setState(Closed)
}

// Failure records a retryable failure; the circuit opens at the threshold or
// immediately if a half-open probe fails.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive++
	if b.state == HalfOpen || b.consecutive >= b.threshold {
		b.openedAt = b.now()
		b.setState(Open)
	}
}

// State returns the current state without side effects.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
