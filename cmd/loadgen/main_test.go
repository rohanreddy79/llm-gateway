package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fakeGateway(t *testing.T, fail int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.Write([]byte(`{"status":"ok"}`))
			return
		}
		n := calls.Add(1)
		if fail > 0 && n%int64(fail) == 0 {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("X-Cache", "HIT")
		w.Write([]byte(`{"id":"x","choices":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestRunLevelCountsRequestsAndErrors(t *testing.T) {
	srv, calls := fakeGateway(t, 4)
	o := options{url: srv.URL, key: "k", model: "m", prompt: "p", requests: 40, warmup: 4, unique: true}
	r := runLevel(context.Background(), srv.Client(), o, 4)
	if calls.Load() != 44 {
		t.Fatalf("calls = %d, want 44 (40 + 4 warm-up)", calls.Load())
	}
	if r.errors != 10 || len(r.latencies) != 30 || r.cacheHits != 30 {
		t.Fatalf("errors=%d latencies=%d hits=%d", r.errors, len(r.latencies), r.cacheHits)
	}
	if r.rps() <= 0 {
		t.Fatal("rps must be positive")
	}
}

func TestPercentile(t *testing.T) {
	d := []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if percentile(d, 50) != 5 || percentile(d, 99) != 10 || percentile(d, 100) != 10 || percentile(d, 0) != 1 {
		t.Fatalf("percentiles: %v %v %v", percentile(d, 50), percentile(d, 99), percentile(d, 0))
	}
	if percentile(nil, 50) != 0 {
		t.Fatal("empty")
	}
}

func TestRunPrintsTable(t *testing.T) {
	srv, _ := fakeGateway(t, 0)
	var out, errb bytes.Buffer
	code := run([]string{"--url", srv.URL, "--requests", "10", "--concurrency", "1,2", "--warmup", "1", "--stream"}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[2], "| stream | 1 | 10 |") || !strings.HasPrefix(lines[3], "| stream | 2 | 10 |") {
		t.Fatalf("table:\n%s", out.String())
	}
	if code := run([]string{"--concurrency", "x"}, &out, &errb); code != 2 {
		t.Fatalf("bad concurrency exit %d", code)
	}
	if code := run([]string{"--url", "http://127.0.0.1:1"}, &out, &errb); code != 1 {
		t.Fatalf("unreachable exit %d", code)
	}
}
