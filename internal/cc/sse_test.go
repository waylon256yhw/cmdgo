package cc

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestScannerSimpleEvents(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"type":"start"}`, ``,
		`data: {"type":"text-delta","text":"hello"}`, ``,
		`data: {"type":"text-delta","text":" world"}`, ``,
		`data: {"type":"finish","finishReason":"stop"}`, ``,
		"",
	}, "\n")

	got := drainScanner(t, NewScanner(strings.NewReader(stream)))
	wantTypes := []string{"start", "text-delta", "text-delta", "finish"}
	if len(got) != len(wantTypes) {
		t.Fatalf("got %d events (%v), want %d", len(got), eventTypes(got), len(wantTypes))
	}
	for i, ev := range got {
		if ev.Type != wantTypes[i] {
			t.Errorf("event %d: type=%q, want %q", i, ev.Type, wantTypes[i])
		}
	}
}

func TestScannerIgnoresCommentsAndUnknownPrefixes(t *testing.T) {
	stream := ": keepalive\n" +
		"event: custom\n" +
		"id: 42\n" +
		"retry: 5000\n" +
		"data: {\"type\":\"start\"}\n\n" +
		":\n" + // bare colon comment
		"data: {\"type\":\"finish\"}\n\n"

	got := drainScanner(t, NewScanner(strings.NewReader(stream)))
	if len(got) != 2 || got[0].Type != "start" || got[1].Type != "finish" {
		t.Fatalf("unexpected events: %v", eventTypes(got))
	}
}

func TestScannerHandlesCRLF(t *testing.T) {
	stream := "data: {\"type\":\"start\"}\r\n\r\n" +
		"data: {\"type\":\"finish\"}\r\n\r\n"
	got := drainScanner(t, NewScanner(strings.NewReader(stream)))
	if len(got) != 2 {
		t.Fatalf("CRLF: got %d events, want 2", len(got))
	}
}

func TestScannerFlushesUnterminatedFrameAtEOF(t *testing.T) {
	// No trailing blank line — make sure we still flush the buffered frame.
	stream := "data: {\"type\":\"finish\"}\n"
	got := drainScanner(t, NewScanner(strings.NewReader(stream)))
	if len(got) != 1 || got[0].Type != "finish" {
		t.Fatalf("unterminated frame: %v", eventTypes(got))
	}
}

func TestScannerHandlesJSONLines(t *testing.T) {
	// CC's actual wire format: one JSON event per line, no `data:` prefix,
	// no blank-line separator. We must accept it as readily as the
	// SSE-framed form.
	stream := `{"type":"start"}
{"type":"reasoning-delta","text":"thinking"}
{"type":"text-delta","text":"pong"}
{"type":"finish","finishReason":"stop","totalUsage":{}}
`
	got := drainScanner(t, NewScanner(strings.NewReader(stream)))
	wantTypes := []string{"start", "reasoning-delta", "text-delta", "finish"}
	if len(got) != len(wantTypes) {
		t.Fatalf("got %d events (%v), want %d", len(got), eventTypes(got), len(wantTypes))
	}
	for i, ev := range got {
		if ev.Type != wantTypes[i] {
			t.Errorf("event %d: type=%q, want %q", i, ev.Type, wantTypes[i])
		}
	}
}

func TestScannerMixedJSONLineAndSSE(t *testing.T) {
	// A pathological mix — should still parse both shapes.
	stream := ": keepalive\n" +
		`{"type":"start"}` + "\n" +
		"data: {\"type\":\"text-delta\",\"text\":\"hi\"}\n\n" +
		`{"type":"finish"}` + "\n"
	got := drainScanner(t, NewScanner(strings.NewReader(stream)))
	wantTypes := []string{"start", "text-delta", "finish"}
	if len(got) != len(wantTypes) {
		t.Fatalf("got %d events (%v), want %d", len(got), eventTypes(got), len(wantTypes))
	}
	for i, ev := range got {
		if ev.Type != wantTypes[i] {
			t.Errorf("event %d: type=%q, want %q", i, ev.Type, wantTypes[i])
		}
	}
}

func TestScannerEmptyStream(t *testing.T) {
	s := NewScanner(strings.NewReader(""))
	ev, err := s.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got err=%v ev=%v", err, ev)
	}
}

func drainScanner(t *testing.T, s *Scanner) []*StreamEvent {
	t.Helper()
	var out []*StreamEvent
	for {
		ev, err := s.Next()
		if ev != nil {
			out = append(out, ev)
		}
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("scanner: %v", err)
		}
	}
}

func eventTypes(events []*StreamEvent) []string {
	out := make([]string, len(events))
	for i, ev := range events {
		out[i] = ev.Type
	}
	return out
}
