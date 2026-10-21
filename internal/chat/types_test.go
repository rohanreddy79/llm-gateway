package chat

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRequestRoundTripKeepsExtra(t *testing.T) {
	in := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stop":"x","tools":[{"type":"function"}],"logit_bias":{"1":2},"max_tokens":5}`
	var r Request
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Extra) != 2 || r.Extra["tools"] == nil || r.Extra["logit_bias"] == nil {
		t.Fatalf("Extra = %v", r.Extra)
	}
	if r.MaxOutputTokens() != 5 || len(r.Stop) != 1 || r.Stop[0] != "x" {
		t.Fatalf("typed fields: %+v", r)
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]json.RawMessage
	json.Unmarshal(out, &back)
	for _, k := range []string{"model", "messages", "stop", "tools", "logit_bias", "max_tokens"} {
		if _, ok := back[k]; !ok {
			t.Errorf("round trip lost %q: %s", k, out)
		}
	}
	if _, ok := back["stream"]; ok {
		t.Errorf("zero-valued optional fields must be omitted: %s", out)
	}
	if string(back["stop"]) != `["x"]` {
		t.Errorf("stop must marshal as an array: %s", back["stop"])
	}
}

func TestMaxOutputTokensPrefersNewField(t *testing.T) {
	var r Request
	json.Unmarshal([]byte(`{"max_tokens":5,"max_completion_tokens":9}`), &r)
	if r.MaxOutputTokens() != 9 {
		t.Fatal("max_completion_tokens should win")
	}
	if (&Request{}).MaxOutputTokens() != 0 {
		t.Fatal("unset should be 0")
	}
}

func TestStopVariants(t *testing.T) {
	var s Stop
	if err := json.Unmarshal([]byte(`["a","b"]`), &s); err != nil || len(s) != 2 {
		t.Fatalf("array: %v %v", s, err)
	}
	if err := json.Unmarshal([]byte(`null`), &s); err != nil || s != nil {
		t.Fatalf("null: %v %v", s, err)
	}
	if err := json.Unmarshal([]byte(`5`), &s); err == nil {
		t.Fatal("number must be rejected")
	}
}

func TestContentForms(t *testing.T) {
	var m Message
	json.Unmarshal([]byte(`{"role":"user","content":[{"type":"text","text":"a"},{"type":"image_url","image_url":{"url":"u"}},{"type":"text","text":"b"}]}`), &m)
	text, ok := m.Content.PlainText()
	if ok || text != "a\nb" {
		t.Fatalf("PlainText = %q, %v", text, ok)
	}
	out, _ := json.Marshal(m)
	if !strings.Contains(string(out), `"image_url":{"url":"u"}`) {
		t.Fatalf("non-text parts must survive a round trip: %s", out)
	}
	var s Message
	json.Unmarshal([]byte(`{"role":"user","content":"plain"}`), &s)
	if text, ok := s.Content.PlainText(); !ok || text != "plain" {
		t.Fatal("string content")
	}
	out, _ = json.Marshal(s)
	if !strings.Contains(string(out), `"content":"plain"`) {
		t.Fatalf("string content must marshal as a string: %s", out)
	}
	var n Message
	json.Unmarshal([]byte(`{"role":"assistant","content":null}`), &n)
	if text, ok := n.Content.PlainText(); !ok || text != "" {
		t.Fatal("null content")
	}
	var bad Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":7}`), &bad); err == nil {
		t.Fatal("numeric content must be rejected")
	}
}
