// Command loadgen fires concurrent chat completions at a gateway and prints
// a markdown table of throughput and latency percentiles per concurrency
// level. It is the tool behind the README benchmark.
//
//	loadgen --url http://127.0.0.1:8080 --key gw-demo-key --model fast \
//	        --requests 2000 --concurrency 1,8,32,64 [--stream] [--unique]
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	url         string
	key         string
	model       string
	prompt      string
	requests    int
	concurrency []int
	stream      bool
	unique      bool
	warmup      int
	// nonce salts unique prompts so neither other concurrency levels nor a
	// previous loadgen run can serve them from the gateway cache.
	nonce int64
}

// result is one concurrency level's measurement.
type result struct {
	mode        string
	concurrency int
	requests    int
	elapsed     time.Duration
	latencies   []time.Duration
	errors      int
	cacheHits   int
}

func (r result) rps() float64 { return float64(len(r.latencies)) / r.elapsed.Seconds() }

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted))*p/100+0.999999) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d.Microseconds())/1000) }

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	var conc string
	fs.StringVar(&o.url, "url", "http://127.0.0.1:8080", "gateway base URL")
	fs.StringVar(&o.key, "key", "gw-demo-key", "virtual API key")
	fs.StringVar(&o.model, "model", "fast", "model alias")
	fs.StringVar(&o.prompt, "prompt", "Explain token buckets in one sentence.", "user prompt")
	fs.IntVar(&o.requests, "requests", 1000, "requests per concurrency level")
	fs.StringVar(&conc, "concurrency", "1,8,32,64", "comma-separated concurrency levels")
	fs.BoolVar(&o.stream, "stream", false, "use streaming completions")
	fs.BoolVar(&o.unique, "unique", true, "make every prompt unique (defeats the cache); false measures cache hits")
	fs.IntVar(&o.warmup, "warmup", 20, "untimed requests before each level")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	o.nonce = time.Now().UnixNano()
	for _, s := range strings.Split(conc, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			fmt.Fprintf(stderr, "bad concurrency %q\n", s)
			return 2
		}
		o.concurrency = append(o.concurrency, n)
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 0}}
	if err := ping(client, o); err != nil {
		fmt.Fprintf(stderr, "gateway not reachable: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "| mode | concurrency | requests | RPS | p50 ms | p95 ms | p99 ms | max ms | errors | cache hits |")
	fmt.Fprintln(stdout, "|---|---|---|---|---|---|---|---|---|---|")
	for _, c := range o.concurrency {
		r := runLevel(context.Background(), client, o, c)
		sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
		fmt.Fprintf(stdout, "| %s | %d | %d | %.0f | %s | %s | %s | %s | %d | %d |\n",
			r.mode, r.concurrency, r.requests, r.rps(),
			ms(percentile(r.latencies, 50)), ms(percentile(r.latencies, 95)), ms(percentile(r.latencies, 99)),
			ms(percentile(r.latencies, 100)), r.errors, r.cacheHits)
	}
	return 0
}

func ping(client *http.Client, o options) error {
	resp, err := client.Get(o.url + "/healthz")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("healthz returned %d", resp.StatusCode)
	}
	return nil
}

// runLevel sends o.requests requests through c workers and times each one
// from request start to the last byte of the body.
func runLevel(ctx context.Context, client *http.Client, o options, c int) result {
	mode := "complete"
	if o.stream {
		mode = "stream"
	}
	if !o.unique {
		mode += "+cache"
	}
	r := result{mode: mode, concurrency: c, requests: o.requests}
	var seq atomic.Int64
	one := func(timed bool) (time.Duration, bool, error) {
		n := seq.Add(1)
		prompt := o.prompt
		if o.unique {
			prompt = fmt.Sprintf("%s (run %d, c=%d, request %d)", o.prompt, o.nonce, c, n)
		}
		body, _ := json.Marshal(map[string]any{
			"model":       o.model,
			"messages":    []map[string]string{{"role": "user", "content": prompt}},
			"stream":      o.stream,
			"temperature": 0,
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return 0, false, err
		}
		req.Header.Set("Authorization", "Bearer "+o.key)
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			return 0, false, err
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		el := time.Since(start)
		if err != nil {
			return el, false, err
		}
		if resp.StatusCode != http.StatusOK {
			return el, false, fmt.Errorf("status %d", resp.StatusCode)
		}
		return el, resp.Header.Get("X-Cache") == "HIT", nil
	}

	// Warm-up primes connections (and the cache when --unique=false).
	var wg sync.WaitGroup
	warm := make(chan struct{}, o.warmup)
	for i := 0; i < o.warmup; i++ {
		warm <- struct{}{}
	}
	close(warm)
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range warm {
				one(false)
			}
		}()
	}
	wg.Wait()
	if !o.unique {
		seq.Store(0)
	}

	var mu sync.Mutex
	work := make(chan struct{}, o.requests)
	for i := 0; i < o.requests; i++ {
		work <- struct{}{}
	}
	close(work)
	start := time.Now()
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				el, hit, err := one(true)
				mu.Lock()
				if err != nil {
					r.errors++
				} else {
					r.latencies = append(r.latencies, el)
					if hit {
						r.cacheHits++
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	r.elapsed = time.Since(start)
	return r
}
