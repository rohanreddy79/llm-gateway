# llm-gateway

**One OpenAI-compatible endpoint in front of OpenAI, Anthropic and mock upstreams, with the operational layers a platform team actually needs: virtual keys, rate limits, retries, fallback, circuit breakers, a response cache, streaming translation, cost accounting and Prometheus metrics.**

[![ci](https://github.com/rohanreddy79/llm-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/rohanreddy79/llm-gateway/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Why

Every team that ships LLM features ends up writing the same thing: a proxy that hands out per-app keys, caps spend, survives a provider outage and speaks one wire format while the model behind it changes. The interesting engineering is not any single layer but how they compose, and that is where most home-grown gateways go wrong: retries that hammer a dying provider, caches that return a sampled answer as if it were deterministic, streams that drop token counts so nobody can bill. This repo is a small, fully tested implementation of that stack in Go with no framework, two dependencies (Prometheus client, YAML) and every layer measured against an in-process mock provider.

## Features

- `POST /v1/chat/completions` in the OpenAI chat format, non-streaming and streaming (`data: {json}` SSE chunks ending in `data: [DONE]`); `GET /v1/models`, `GET /healthz`, `GET /metrics`, `GET /admin/usage`.
- Providers: `openai` (passthrough to any OpenAI-compatible base URL; unknown request fields and upstream response bytes are forwarded verbatim), `anthropic` (OpenAI chat → Messages API translation for requests, responses and the `message_start` / `content_block_delta` / `message_delta` / `message_stop` event stream, with usage carried into the final chunk), `mock` (deterministic text, configurable latency and failure injection; used by the tests, the demo and the benchmark).
- Model aliases map to an ordered fallback chain of `provider:model` targets.
- Virtual API keys with per-key requests-per-minute and tokens-per-minute token buckets, model allowlists and OpenAI-style `X-RateLimit-*` / `Retry-After` headers.
- Retry with capped exponential backoff and jitter on 429/5xx/timeouts, honouring upstream `Retry-After`; per-provider circuit breaker (closed → open → half-open) and concurrency cap; per-attempt timeouts via `context`.
- LRU + TTL response cache keyed on a hash of the normalised request, used only for deterministic requests; `X-Cache: HIT|MISS`.
- Per-key usage accounting (requests, input/output tokens, estimated USD from a dated price table) and Prometheus metrics: request counts and latency histograms, tokens, cost, breaker state, cache events, rate-limit rejections, upstream attempts by outcome.
- Structured JSON logs with request ids; graceful shutdown; strict config validation that reports every error at once.

## Quickstart

```bash
git clone https://github.com/rohanreddy79/llm-gateway && cd llm-gateway
CGO_ENABLED=0 go build -o bin/llm-gateway ./cmd/llm-gateway && CGO_ENABLED=0 go build -o bin/loadgen ./cmd/loadgen
bin/llm-gateway serve --config gateway.yaml &      # mock providers only, no API keys needed
curl -s http://127.0.0.1:19080/v1/chat/completions -H "Authorization: Bearer gw-demo-key" \
  -H "Content-Type: application/json" -d '{"model":"fast","messages":[{"role":"user","content":"Hello"}]}'
curl -s http://127.0.0.1:19080/admin/usage -H "Authorization: Bearer gw-demo-key"
```

Trimmed output of the two `curl` calls:

```json
{"id":"chatcmpl-mock","object":"chat.completion","created":1792611590,"model":"mock-fast",
 "choices":[{"index":0,"message":{"role":"assistant","content":"This is a deterministic mock completion from llm-gateway. ..."},"finish_reason":"stop"}],
 "usage":{"prompt_tokens":6,"completion_tokens":24,"total_tokens":30}}
{"keys":[{"key":"demo","requests":1,"input_tokens":6,"output_tokens":24,"cost_usd":0.0000102,
 "models":{"mock:mock-fast":{"requests":1,"input_tokens":6,"output_tokens":24,"cost_usd":0.0000102}}}]}
```

Repeat the first `curl` and the response comes back with `X-Cache: HIT`, `X-RateLimit-Remaining-Requests: 598` and no upstream call. Add `"stream": true` and you get SSE chunks. The `resilient` alias in `gateway.yaml` routes to a mock that fails every third call: six requests in a row return `200 200 200 200 200 200` because the retry layer absorbs the injected 503s. To use real providers, uncomment the `anthropic` / `openai` blocks and aliases in `gateway.yaml` and export the keys named in `.env.example`.

### Point your OpenAI SDK at it

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:19080/v1", api_key="gw-demo-key")
resp = client.chat.completions.create(model="fast", messages=[{"role": "user", "content": "Hello"}])
print(resp.choices[0].message.content)
for chunk in client.chat.completions.create(model="fast", messages=[{"role": "user", "content": "Hello"}], stream=True):
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```

The full script is `examples/openai_sdk.py` (`uv run --with openai examples/openai_sdk.py`). With curl, streaming is `-N` plus `"stream": true`; the gateway also accepts the key as `x-api-key`.

## How it works

```
client ──► request id + slog ──► auth (virtual key) ──► rate limit (RPM bucket, TPM debt)
       ──► decode + validate ──► model allowlist ──► cache lookup (deterministic only)
       ──► router: for target in alias chain:
               breaker.Allow? ──► concurrency slot ──► per-attempt timeout ──► provider
               ▲ retry (backoff + jitter) on 429/5xx/timeout; breaker fed per attempt
               └─ open circuit / retries exhausted ──► next target
       ──► cache store ──► usage + TPM debit + metrics ──► JSON or SSE to client
```

| Package | Responsibility |
|---|---|
| `internal/chat` | OpenAI wire types; unknown fields preserved for passthrough, string-or-parts content |
| `internal/provider` | `Provider` interface (`Complete`, `Stream`), typed upstream errors, retryability |
| `internal/provider/{openai,anthropic,mock}` | The three upstreams; Anthropic translation lives in `translate.go` + `stream.go` with golden fixtures under `testdata/` |
| `internal/config` | YAML loading, defaults, strict cross-reference validation, dated price table |
| `internal/ratelimit`, `cache`, `breaker`, `retry` | The resilience primitives, each with an injectable clock |
| `internal/router` | Fallback chain + breaker + concurrency cap + timeout + retry composition |
| `internal/gateway` | HTTP edge: middleware, handlers, SSE relay, error mapping, usage, metrics |
| `cmd/llm-gateway`, `cmd/loadgen` | `serve` / `check` CLI; concurrent load generator that prints the benchmark table |

**Token bucket per key, debited after the fact for tokens.** Requests per minute are a classic bucket consumed before the call. Tokens per minute cannot be: the cost is only known when the completion finishes, so the TPM bucket is debited afterwards and may go negative. A key in debt is rejected with `429` and a `Retry-After` computed from the refill rate, and the rejection does not burn an RPM token. Fixed windows were rejected because they allow 2× bursts at the window edge; a global limiter was rejected because the point of virtual keys is isolation between apps.

**Cache only what is deterministic.** A cached completion is only correct if the same request would have produced the same answer, so only non-streaming requests with `temperature` 0 or unset and `n` 1 are cached, keyed on a SHA-256 of the normalised request (model, messages, max tokens, temperature, top_p, stop and any passthrough fields; `user` and stream flags are excluded). Treating an unset temperature as cacheable is a deliberate policy: OpenAI's default is 1, but a client that omits temperature and hits a gateway cache is asking for cheap answers, and the TTL bounds the staleness. Cached responses are not billed against the key's tokens, which is the whole incentive.

**Breaker inside retry, not around it.** Each attempt asks the breaker for permission and reports its outcome, so a provider that fails three attempts in one request moves the breaker three steps towards open, and once it opens the remaining retries are skipped and the router falls straight through to the next target. Putting the breaker outside the retry loop would let one request hammer a dying provider `max_attempts` times before the breaker learned anything. A healthy provider rejecting a bad request (400) counts as a success for the breaker and is returned immediately without fallback, because the next provider would reject it too and the caller needs the real message.

**Streaming translation carries usage.** Anthropic sends `input_tokens` in `message_start` and cumulative `output_tokens` in `message_delta`; OpenAI clients expect usage on a chunk. The translator accumulates both and emits them on the final chunk together with `finish_reason`, so streamed requests are billed and TPM-limited exactly like non-streamed ones. For the OpenAI passthrough the gateway forces `stream_options.include_usage` for the same reason. An error after the first byte cannot change the HTTP status, so it is sent as a final `data: {"error": ...}` event before `[DONE]`, which is what the SDKs parse.

**Honest about what is missing for production.** Keys live in the YAML file and usage lives in memory: a real deployment needs a key store with rotation, persistent and multi-tenant quotas, and an upstream-key vault. Fallback only happens before the first streamed byte; retries are per target, not budgeted across the chain; and the cache is per process. See Roadmap.

## Configuration

```yaml
listen: "127.0.0.1:19080"
providers:
  anthropic: {type: anthropic, api_key_env: ANTHROPIC_API_KEY, timeout: 120s, max_concurrency: 32, default_max_tokens: 1024}
  openai:    {type: openai, base_url: https://api.openai.com/v1, api_key_env: OPENAI_API_KEY}
  mock:      {type: mock, latency: 2ms, fail_every: 0, fail_status: 503}
models:                       # alias -> ordered fallback chain
  claude: [anthropic:claude-sonnet-5-5, anthropic:claude-haiku-5-5]
keys:
  gw-demo-key: {name: demo, rpm: 600, tpm: 200000, models: [claude], admin: true}
cache:   {ttl: 5m, max_entries: 1000}        # max_entries 0 disables
retry:   {max_attempts: 3, base_backoff: 100ms, max_backoff: 2s}
breaker: {failures: 5, open_for: 30s}
prices:  {mock-fast: {input: 0.10, output: 0.40}}   # USD per million tokens, merged over built-ins
```

`rpm`/`tpm` of 0 mean unlimited and an empty `models` list allows every alias. Upstream keys are read from the named environment variables and never from the file. Built-in prices cover the Claude models (Anthropic list prices as of 2026-10-06, recorded in `internal/config/prices.go`); other models cost 0 until you add them, and `/admin/usage` reports `unpriced_requests` so the gap is visible. `llm-gateway check --config gateway.yaml` validates a file without starting the server.

## Benchmarks

Machine: Apple M3, 8 GB RAM, macOS 27.0, CPU only, `go1.25.6 darwin/arm64`, `CGO_ENABLED=0`. The load generator and the gateway share the machine, info-level JSON logging to a file was on, and every row is 3000 requests after 20 warm-up requests. Peak gateway RSS during the run: 42 MB. Reproduce with:

```bash
bin/llm-gateway serve --config examples/bench.yaml > bench.log 2>&1 &
bin/loadgen --url http://127.0.0.1:19081 --key gw-bench-key --model fast --requests 3000 --concurrency 1,8,32,64
bin/loadgen --url http://127.0.0.1:19081 --key gw-bench-key --model fast --requests 3000 --concurrency 1,8,32,64 --stream
bin/loadgen --url http://127.0.0.1:19081 --key gw-bench-key --model fast --requests 3000 --concurrency 1,8,32,64 --unique=false
bin/loadgen --url http://127.0.0.1:19081 --key gw-bench-key --model slow --requests 3000 --concurrency 1,8,32,64
bin/loadgen --url http://127.0.0.1:19081 --key gw-bench-key --model flaky --requests 3000 --concurrency 1,32
```

Gateway overhead: zero-latency mock upstream, unique prompts (every request goes through auth, rate limiting, cache miss, router, breaker, provider, accounting, logging).

| mode | concurrency | RPS | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| complete | 1 | 14450 | 0.06 | 0.12 | 0.17 |
| complete | 8 | 53663 | 0.13 | 0.27 | 0.38 |
| complete | 32 | 65895 | 0.35 | 1.20 | 1.73 |
| complete | 64 | 64934 | 0.67 | 2.64 | 3.60 |
| stream (26 chunks each) | 1 | 3962 | 0.24 | 0.28 | 0.43 |
| stream (26 chunks each) | 8 | 8998 | 0.75 | 1.52 | 3.91 |
| stream (26 chunks each) | 32 | 8636 | 2.91 | 9.40 | 16.07 |
| stream (26 chunks each) | 64 | 9262 | 4.77 | 19.54 | 31.78 |
| cache hit (identical prompt) | 1 | 17334 | 0.05 | 0.07 | 0.12 |
| cache hit (identical prompt) | 64 | 64372 | 0.77 | 2.44 | 3.28 |

Streaming plateaus around 9k requests/s because each request is 26 chunk writes with a flush each, i.e. roughly 240k SSE events per second; the per-request cost is the chunk count, not the gateway's pipeline.

Concurrency scaling with a 2 ms upstream (`slow` alias) and resilience with an upstream that fails every tenth call with 503 (`flaky` alias; `max_attempts: 3`, 1–10 ms backoff):

| mode | concurrency | RPS | p50 ms | p95 ms | p99 ms | client errors |
|---|---|---|---|---|---|---|
| 2 ms upstream | 1 | 336 | 2.83 | 3.45 | 4.41 | 0 |
| 2 ms upstream | 8 | 2711 | 2.81 | 3.91 | 5.12 | 0 |
| 2 ms upstream | 32 | 12437 | 2.50 | 3.18 | 3.72 | 0 |
| 2 ms upstream | 64 | 26442 | 2.25 | 3.00 | 3.87 | 0 |
| flaky upstream, 10% 503 | 1 | 4272 | 0.11 | 1.16 | 1.37 | 0 |
| flaky upstream, 10% 503 | 32 | 26768 | 0.94 | 3.18 | 7.61 | 0 |

During the flaky rows `/metrics` recorded 670 retryable upstream failures absorbed by retries, 5 requests that exhausted their attempts and fell back to the second provider, and 0 client-visible errors. Throughput varies about ±10% between runs; the shape does not.

## Testing

```bash
CGO_ENABLED=0 go test -race ./...
```

87 tests (about 2.4k lines) run offline in about 4 seconds and cover: OpenAI→Anthropic request translation against golden JSON (system extraction, turn merging, max_tokens default, temperature clamp, stop sequences, user metadata) and the error cases (tool calls, image parts, unknown roles); Anthropic→OpenAI response translation including cache-read usage and stop-reason mapping; streaming translation against SSE fixtures (role chunk first, text deltas, usage and `finish_reason` on the final chunk, thinking blocks dropped, `error` events and truncated streams); OpenAI passthrough of unknown fields and raw bytes; `httptest`-backed provider tests for headers, status mapping, `Retry-After`, timeouts and mid-stream cancellation; auth (401), allowlist (403), unknown model (404), validation (400); RPM and TPM limits with headers; retry on 503 then success; fallback to the second provider and no fallback on 400; breaker open → fast reject → half-open probe → close, with the Prometheus gauge checked; cache hit without an upstream call, TTL expiry and bypass of sampled requests; usage totals and cost; SSE output ending in `[DONE]`; stream errors before the first byte as JSON; 504 on timeout; the concurrency cap; slot release after client disconnect; an end-to-end run through a config-built Anthropic provider against an `httptest` upstream; config validation; and both CLIs. Every primitive takes an injected clock, so nothing sleeps for real except the deliberately short latency tests.

## Roadmap / Limitations

- Keys and usage are in-memory and file-based; a production gateway needs a key store with rotation, persistent per-tenant quotas and an upstream-credential vault.
- Fallback and retry only happen before the first streamed byte; a stream that dies halfway is reported to the client as an error event, not replayed elsewhere.
- The Anthropic translation covers text conversations only: tool calls, images and structured outputs are rejected with a 400 rather than translated.
- Cost is estimated at the full input price; prompt-cache reads and writes are not discounted, and non-Claude prices must be supplied in config.
- One `timeout` bounds the whole upstream call including a long stream; a separate time-to-first-byte timeout would be more precise.
- The cache and the limiter are per process; a multi-replica deployment needs a shared store (Redis) for both.

## License

MIT. Copyright (c) 2026 Rohan Reddy Jakkam.
