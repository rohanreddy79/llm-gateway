package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

var update = flag.Bool("update", false, "rewrite golden files")

// fixedNow keeps created timestamps stable across runs.
var fixedNow = time.Unix(1_760_000_000, 0)

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// assertGolden compares got (as canonical JSON) with the file, or rewrites it
// under -update.
func assertGolden(t *testing.T, path string, got any) {
	t.Helper()
	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	gotJSON = append(gotJSON, '\n')
	if *update {
		if err := os.WriteFile(path, gotJSON, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	wantJSON, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create)", err)
	}
	var g, w any
	if err := json.Unmarshal(gotJSON, &g); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wantJSON, &w); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", path, gotJSON, wantJSON)
	}
}

func TestRequestTranslationGolden(t *testing.T) {
	inputs, _ := filepath.Glob("testdata/request/*.openai.json")
	if len(inputs) == 0 {
		t.Fatal("no request fixtures")
	}
	for _, in := range inputs {
		name := strings.TrimSuffix(filepath.Base(in), ".openai.json")
		t.Run(name, func(t *testing.T) {
			var req chat.Request
			readJSON(t, in, &req)
			got, err := toMessagesRequest("anthropic", &req, 1024)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			assertGolden(t, filepath.Join("testdata/request", name+".anthropic.json"), got)
		})
	}
}

func TestRequestTranslationErrors(t *testing.T) {
	cases := map[string]string{
		"tool role":        `{"model":"m","messages":[{"role":"user","content":"x"},{"role":"tool","content":"r","tool_call_id":"c1"}]}`,
		"tool calls":       `{"model":"m","messages":[{"role":"assistant","content":"","tool_calls":[{"id":"c1"}]}]}`,
		"image part":       `{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:x"}}]}]}`,
		"unknown role":     `{"model":"m","messages":[{"role":"function","content":"x"}]}`,
		"only system":      `{"model":"m","messages":[{"role":"system","content":"x"}]}`,
		"no messages":      `{"model":"m","messages":[]}`,
		"nil content part": `{"model":"m","messages":[{"role":"user","content":[{"type":"input_audio"}]}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var req chat.Request
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatal(err)
			}
			_, err := toMessagesRequest("anthropic", &req, 1024)
			if provider.StatusOf(err) != 400 || provider.IsRetryable(err) {
				t.Fatalf("expected non-retryable 400, got %v", err)
			}
		})
	}
}

func TestResponseTranslationGolden(t *testing.T) {
	inputs, _ := filepath.Glob("testdata/response/*.anthropic.json")
	if len(inputs) == 0 {
		t.Fatal("no response fixtures")
	}
	for _, in := range inputs {
		name := strings.TrimSuffix(filepath.Base(in), ".anthropic.json")
		t.Run(name, func(t *testing.T) {
			var resp messagesResponse
			readJSON(t, in, &resp)
			got := fromMessagesResponse(&resp, fixedNow)
			assertGolden(t, filepath.Join("testdata/response", name+".openai.json"), got)
		})
	}
}

// runStream feeds an SSE fixture through the translator.
func runStream(t *testing.T, path string) (chunks []*chat.Chunk, err error) {
	t.Helper()
	b, rerr := os.ReadFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}
	out := make(chan provider.Chunk)
	go translateStream(context.Background(), "anthropic", io.NopCloser(bytes.NewReader(b)), fixedNow, out)
	for c := range out {
		if c.Err != nil {
			err = c.Err
			continue
		}
		chunks = append(chunks, c.Data)
	}
	return chunks, err
}

func TestStreamTranslationGolden(t *testing.T) {
	for _, name := range []string{"basic", "length_cached"} {
		t.Run(name, func(t *testing.T) {
			chunks, err := runStream(t, "testdata/stream/"+name+".sse")
			if err != nil {
				t.Fatalf("stream error: %v", err)
			}
			assertGolden(t, "testdata/stream/"+name+".chunks.json", chunks)
			// Structural invariants the OpenAI SDK relies on.
			if chunks[0].Choices[0].Delta.Role != "assistant" {
				t.Error("first chunk must carry the assistant role")
			}
			last := chunks[len(chunks)-1]
			if last.Choices[0].FinishReason == nil || last.Usage == nil {
				t.Error("final chunk must carry finish_reason and usage")
			}
			for _, c := range chunks[:len(chunks)-1] {
				if c.Choices[0].FinishReason != nil {
					t.Error("only the final chunk may carry finish_reason")
				}
			}
		})
	}
}

func TestStreamTranslationErrors(t *testing.T) {
	chunks, err := runStream(t, "testdata/stream/error.sse")
	if provider.StatusOf(err) != 529 || !provider.IsRetryable(err) {
		t.Fatalf("error event: want retryable 529, got %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks before the error should be delivered, got %d", len(chunks))
	}
	chunks, err = runStream(t, "testdata/stream/truncated.sse")
	if provider.StatusOf(err) != 502 || len(chunks) != 2 {
		t.Fatalf("truncated stream: err=%v chunks=%d", err, len(chunks))
	}
	_, err = runStream(t, "testdata/request/basic.openai.json") // not SSE at all
	if err == nil {
		t.Fatal("garbage input must produce an error")
	}
}

func TestFinishReason(t *testing.T) {
	cases := map[string]string{"end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length", "tool_use": "tool_calls", "refusal": "content_filter", "": "stop"}
	for in, want := range cases {
		if got := finishReason(in); got != want {
			t.Errorf("finishReason(%q) = %q, want %q", in, got, want)
		}
	}
}
