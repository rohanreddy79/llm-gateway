package sse

import (
	"io"
	"strings"
	"testing"
)

func TestReader(t *testing.T) {
	in := ": comment\r\n\r\nevent: ping\ndata: {\"a\":1}\n\ndata: line1\ndata:line2\n\nid: 3\ndata: tail\n\n"
	rd := NewReader(strings.NewReader(in))
	want := []Event{{"ping", `{"a":1}`}, {"", "line1\nline2"}, {"", "tail"}}
	for i, w := range want {
		ev, err := rd.Next()
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if ev != w {
			t.Fatalf("event %d = %+v, want %+v", i, ev, w)
		}
	}
	if _, err := rd.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReaderPartialEvent(t *testing.T) {
	rd := NewReader(strings.NewReader("data: half"))
	if _, err := rd.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestReaderLongLine(t *testing.T) {
	big := strings.Repeat("x", 200<<10)
	rd := NewReader(strings.NewReader("data: " + big + "\n\n"))
	ev, err := rd.Next()
	if err != nil || len(ev.Data) != len(big) {
		t.Fatalf("long line: err=%v len=%d", err, len(ev.Data))
	}
}

func TestFormat(t *testing.T) {
	if got := string(Format("", "[DONE]")); got != "data: [DONE]\n\n" {
		t.Fatalf("Format = %q", got)
	}
	if got := string(Format("error", "{}")); got != "event: error\ndata: {}\n\n" {
		t.Fatalf("Format = %q", got)
	}
}
