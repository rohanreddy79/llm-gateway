package breaker

import (
	"sync"
	"testing"
	"time"
)

func TestLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	var transitions []State
	b := New(3, 10*time.Second, func() time.Time { return now }, func(s State) { transitions = append(transitions, s) })

	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatal("closed breaker must allow")
		}
		b.Failure()
	}
	if b.State() != Closed {
		t.Fatalf("state after 2 failures = %v", b.State())
	}
	b.Allow()
	b.Failure()
	if b.State() != Open {
		t.Fatalf("state after 3 failures = %v", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker must reject")
	}
	now = now.Add(10 * time.Second)
	if !b.Allow() {
		t.Fatal("after open window one probe must pass")
	}
	if b.State() != HalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
	if b.Allow() {
		t.Fatal("second probe must be rejected while one is in flight")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("failed probe must re-open")
	}
	now = now.Add(10 * time.Second)
	b.Allow()
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatal("successful probe must close")
	}
	want := []State{Open, HalfOpen, Open, HalfOpen, Closed}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", transitions, want)
		}
	}
}

func TestSuccessResetsCount(t *testing.T) {
	b := New(2, time.Second, nil, nil)
	b.Failure()
	b.Success()
	b.Failure()
	if b.State() != Closed {
		t.Fatal("a success must reset the consecutive-failure count")
	}
}

func TestAbandonedProbe(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(1, time.Second, func() time.Time { return now }, nil)
	b.Failure()
	now = now.Add(time.Second)
	if !b.Allow() || b.Allow() {
		t.Fatal("exactly one probe")
	}
	now = now.Add(time.Second) // the probe never reported
	if !b.Allow() {
		t.Fatal("an abandoned probe must not wedge the breaker half-open")
	}
}

func TestConcurrent(t *testing.T) {
	b := New(5, time.Millisecond, nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if b.Allow() {
				if i%3 == 0 {
					b.Failure()
				} else {
					b.Success()
				}
			}
		}(i)
	}
	wg.Wait()
	_ = b.State()
}

func TestStateString(t *testing.T) {
	if Closed.String() != "closed" || HalfOpen.String() != "half-open" || Open.String() != "open" || State(9).String() != "unknown" {
		t.Fatal("String()")
	}
}
