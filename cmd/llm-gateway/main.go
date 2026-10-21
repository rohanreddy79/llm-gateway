// Command llm-gateway runs the OpenAI-compatible LLM gateway.
//
//	llm-gateway serve --config gateway.yaml [--listen :8080]
//	llm-gateway check --config gateway.yaml
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/config"
	"github.com/rohanreddy79/llm-gateway/internal/gateway"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  llm-gateway serve --config gateway.yaml [--listen :8080] [--log-level info]")
	fmt.Fprintln(w, "  llm-gateway check --config gateway.yaml")
}

// run dispatches subcommands and returns the process exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "gateway.yaml", "path to the YAML config")
	listen := fs.String("listen", "", "override the listen address from the config")
	logLevel := fs.String("log-level", "", "override log_level (debug, info, warn, error)")
	switch args[0] {
	case "serve", "check":
	default:
		usage(stderr)
		return 2
	}
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if args[0] == "check" {
		fmt.Fprintf(stdout, "config ok: %d providers, %d model aliases, %d keys\n", len(cfg.Providers), len(cfg.Routes), len(cfg.Keys))
		return 0
	}
	logger := newLogger(stderr, cfg.LogLevel)
	g, err := gateway.New(cfg, gateway.Options{Logger: logger})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	logger.Info("listening", "addr", ln.Addr().String(), "models", cfg.Aliases())
	if err := serve(ctx, ln, g.Handler(), logger); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func newLogger(w io.Writer, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl}))
}

// serve runs the HTTP server until ctx is cancelled, then drains in-flight
// requests for up to 10 seconds. No WriteTimeout is set because streamed
// completions can legitimately run for minutes; per-attempt upstream
// timeouts bound them instead.
func serve(ctx context.Context, ln net.Listener, h http.Handler, logger *slog.Logger) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
