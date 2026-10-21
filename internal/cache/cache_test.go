package cache

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
)

func TestGetSetTTL(t *testing.T) {
	now := time.Unix(0, 0)
	c := New(time.Minute, 10, func() time.Time { return now })
	if _, ok := c.Get("a"); ok {
		t.Fatal("empty cache hit")
	}
	c.Set("a", []byte("x"))
	if v, ok := c.Get("a"); !ok || string(v) != "x" {
		t.Fatalf("Get = %q, %v", v, ok)
	}
	now = now.Add(time.Minute)
	if _, ok := c.Get("a"); ok {
		t.Fatal("entry must expire at ttl")
	}
	if c.Len() != 0 {
		t.Fatalf("expired entry not removed: len=%d", c.Len())
	}
}

func TestLRUEviction(t *testing.T) {
	c := New(time.Hour, 2, nil)
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Get("a") // a is now most recent
	c.Set("c", []byte("3"))
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should survive")
	}
	if c.Len() != 2 {
		t.Fatalf("len = %d", c.Len())
	}
	c.Set("a", []byte("9")) // overwrite keeps size
	if v, _ := c.Get("a"); string(v) != "9" || c.Len() != 2 {
		t.Fatalf("overwrite: v=%s len=%d", v, c.Len())
	}
}

func TestDisabled(t *testing.T) {
	c := New(time.Hour, 0, nil)
	c.Set("a", []byte("1"))
	if _, ok := c.Get("a"); ok || c.Enabled() {
		t.Fatal("max_entries 0 must disable the cache")
	}
	var nilCache *Cache
	if nilCache.Enabled() {
		t.Fatal("nil cache must report disabled")
	}
}

func parse(t *testing.T, s string) *chat.Request {
	t.Helper()
	var r chat.Request
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

func TestCacheable(t *testing.T) {
	cases := map[string]bool{
		`{"model":"m","messages":[]}`:                       true,
		`{"model":"m","messages":[],"temperature":0}`:       true,
		`{"model":"m","messages":[],"temperature":0.7}`:     false,
		`{"model":"m","messages":[],"stream":true}`:         false,
		`{"model":"m","messages":[],"n":2}`:                 false,
		`{"model":"m","messages":[],"n":1,"temperature":0}`: true,
	}
	for in, want := range cases {
		if got := Cacheable(parse(t, in)); got != want {
			t.Errorf("Cacheable(%s) = %v, want %v", in, got, want)
		}
	}
}

func TestKey(t *testing.T) {
	base := `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0}`
	k := Key(parse(t, base))
	same := []string{
		`{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
		`{"temperature":0,"messages":[{"role":"user","content":"hi"}],"model":"m","user":"u1","stream_options":{"include_usage":true}}`,
	}
	for _, s := range same {
		if Key(parse(t, s)) != k {
			t.Errorf("key for %s should equal base", s)
		}
	}
	different := []string{
		`{"model":"m2","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"m","messages":[{"role":"user","content":"hi!"}]}`,
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":5}`,
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"stop":["x"]}`,
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_object"}}`,
		`{"model":"m","messages":[{"role":"system","content":"hi"}]}`,
	}
	for _, s := range different {
		if Key(parse(t, s)) == k {
			t.Errorf("key for %s should differ from base", s)
		}
	}
}
