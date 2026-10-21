package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/config"
	"github.com/rohanreddy79/llm-gateway/internal/gateway"
)

// syncBuffer is a bytes.Buffer safe to read while a server goroutine logs to it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const okYAML = `
providers: {mock: {type: mock}}
models: {fast: [mock:m]}
keys: {k: {name: k}}
`

func TestCheck(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"check", "--config", writeConfig(t, okYAML)}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "config ok: 1 providers, 1 model aliases, 1 keys") {
		t.Fatalf("stdout = %q", out.String())
	}
	errb.Reset()
	if code := run(context.Background(), []string{"check", "--config", writeConfig(t, "providers: {p: {type: nope}}")}, &out, &errb); code != 1 || !strings.Contains(errb.String(), "invalid config") {
		t.Fatalf("invalid config: exit %d stderr=%q", code, errb.String())
	}
	if code := run(context.Background(), []string{"check", "--config", "/no/such/file.yaml"}, &out, &errb); code != 1 {
		t.Fatalf("missing file: exit %d", code)
	}
}

func TestUsageErrors(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), nil, &out, &errb); code != 2 {
		t.Fatalf("no args: %d", code)
	}
	if code := run(context.Background(), []string{"frobnicate"}, &out, &errb); code != 2 {
		t.Fatalf("unknown command: %d", code)
	}
	if code := run(context.Background(), []string{"serve", "--bogus"}, &out, &errb); code != 2 {
		t.Fatalf("bad flag: %d", code)
	}
}

func TestServeAndShutdown(t *testing.T) {
	cfg, err := config.Parse([]byte(okYAML))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	g, err := gateway.New(cfg, gateway.Options{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, g.Handler(), logger) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestServeWithListenOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, errb syncBuffer
	cfgPath := writeConfig(t, okYAML)
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"serve", "--config", cfgPath, "--listen", "127.0.0.1:0", "--log-level", "debug"}, &out, &errb)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(errb.String(), `"listening"`) {
			cancel()
			if code := <-done; code != 0 {
				t.Fatalf("exit %d: %s", code, errb.String())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server never logged listening: %s", errb.String())
}

func TestNewLoggerLevels(t *testing.T) {
	var b bytes.Buffer
	newLogger(&b, "warn").Info("hidden")
	newLogger(&b, "garbage").Info("shown")
	if strings.Contains(b.String(), "hidden") || !strings.Contains(b.String(), "shown") {
		t.Fatalf("log output = %q", b.String())
	}
}
