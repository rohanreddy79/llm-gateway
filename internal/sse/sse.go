// Package sse parses Server-Sent Events as emitted by the OpenAI and
// Anthropic streaming endpoints. It handles multi-line data, CRLF line
// endings, comments and oversized lines without the size limit of
// bufio.Scanner.
package sse

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// Event is one SSE message. Data has the per-line "data:" prefixes stripped
// and lines joined by "\n", as the spec requires.
type Event struct {
	Name string
	Data string
}

// Reader yields events from a stream.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64<<10)}
}

// Next returns the next event, or io.EOF when the stream ends cleanly. A
// partial event at EOF is returned with io.ErrUnexpectedEOF.
func (rd *Reader) Next() (Event, error) {
	var ev Event
	var data []string
	seen := false
	for {
		line, err := rd.r.ReadString('\n')
		if err != nil && err != io.EOF {
			return Event{}, err
		}
		if err == io.EOF {
			// A final line without a newline, or a dangling event without
			// its terminating blank line, is a truncated stream.
			if line == "" && !seen {
				return Event{}, io.EOF
			}
			return Event{}, io.ErrUnexpectedEOF
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if !seen {
				continue // leading blank lines
			}
			ev.Data = strings.Join(data, "\n")
			return ev, nil
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		seen = true
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.Name = value
		case "data":
			data = append(data, value)
		}
	}
}

// Format renders one event the way the gateway writes it.
func Format(name, data string) []byte {
	var b bytes.Buffer
	if name != "" {
		b.WriteString("event: ")
		b.WriteString(name)
		b.WriteByte('\n')
	}
	b.WriteString("data: ")
	b.WriteString(data)
	b.WriteString("\n\n")
	return b.Bytes()
}
