package cc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// StreamEvent is one SSE frame from `/alpha/generate`. The proxy adapters
// in commit 3/4 decode Raw into typed structs (text-delta, finish, ...).
type StreamEvent struct {
	Type string
	Raw  json.RawMessage
}

// Scanner reads SSE frames written by CC's `/alpha/generate`. CC only
// emits `data: <json>\n\n` frames (no comments, no `event:`/`id:`/`retry:`
// lines), so we focus on that shape but ignore unknown prefixes
// defensively.
type Scanner struct {
	r       *bufio.Reader
	dataBuf bytes.Buffer
	err     error
}

// Default buffer sizes. SSE frames from finish events can grow well past
// the default 64 KiB bufio capacity when usage details + tool calls
// stack up, so we start with a roomy reader and cap accumulation at
// scanBufMax to avoid runaway memory if upstream misbehaves.
const (
	scanReaderSize = 1 << 16 // 64 KiB
	scanBufMax     = 1 << 22 // 4 MiB per frame
)

// NewScanner returns a Scanner over r. The caller is responsible for
// closing the underlying stream.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReaderSize(r, scanReaderSize)}
}

// Next returns the next event. io.EOF means a clean end of stream;
// any other error is fatal for the scanner.
func (s *Scanner) Next() (*StreamEvent, error) {
	if s.err != nil {
		return nil, s.err
	}
	for {
		line, err := s.r.ReadBytes('\n')
		hasLine := len(line) > 0
		if err != nil && !errors.Is(err, io.EOF) {
			s.err = err
			return nil, err
		}
		eof := errors.Is(err, io.EOF)

		// Trim CRLF before parsing prefix.
		trimmed := bytes.TrimRight(line, "\r\n")
		blank := len(trimmed) == 0

		if hasLine {
			switch {
			case blank:
				if s.dataBuf.Len() > 0 {
					return s.flush()
				}
			case bytes.HasPrefix(trimmed, []byte(":")):
				// SSE comment / keepalive — ignore.
			case bytes.HasPrefix(trimmed, []byte("data:")):
				payload := trimmed[len("data:"):]
				if len(payload) > 0 && payload[0] == ' ' {
					payload = payload[1:]
				}
				if s.dataBuf.Len()+len(payload) > scanBufMax {
					s.err = errors.New("cc: SSE frame exceeded scanBufMax")
					return nil, s.err
				}
				s.dataBuf.Write(payload)
			default:
				// Unknown field — ignore (event:, id:, retry: not used by CC).
			}
		}

		if eof {
			s.err = io.EOF
			if s.dataBuf.Len() > 0 {
				ev, derr := s.flush()
				if derr != nil {
					return nil, derr
				}
				return ev, io.EOF
			}
			return nil, io.EOF
		}
	}
}

func (s *Scanner) flush() (*StreamEvent, error) {
	// Copy out because dataBuf is reused.
	payload := make([]byte, s.dataBuf.Len())
	copy(payload, s.dataBuf.Bytes())
	s.dataBuf.Reset()

	if len(payload) == 0 {
		return nil, nil
	}
	if string(payload) == "[DONE]" {
		// OpenAI sentinel — upstream doesn't emit it, but be tolerant.
		return &StreamEvent{Type: "done", Raw: payload}, nil
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &head); err != nil {
		return nil, fmt.Errorf("cc: parse sse frame: %w (raw=%s)", err, truncate(payload, 200))
	}
	return &StreamEvent{Type: head.Type, Raw: payload}, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
