package proxy

import (
	"encoding/json"
	"errors"
	"net/http"
)

// SSEWriter wraps an http.ResponseWriter so we can stream Server-Sent
// Events to clients. The first WriteJSON/WriteRaw call flushes the
// response headers (Content-Type, no-cache, no proxy buffering) so
// long-running streams don't get buffered by upstream reverse
// proxies.
type SSEWriter struct {
	w           http.ResponseWriter
	flusher     http.Flusher
	wroteHeader bool
	err         error
}

// NewSSEWriter wraps w. It fails fast if the response writer does not
// implement http.Flusher (cmdgo's middlewares all preserve it).
func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("proxy: ResponseWriter does not support http.Flusher")
	}
	return &SSEWriter{w: w, flusher: f}, nil
}

func (s *SSEWriter) writeHeadersOnce() {
	if s.wroteHeader {
		return
	}
	h := s.w.Header()
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
	}
	if h.Get("Cache-Control") == "" {
		h.Set("Cache-Control", "no-cache")
	}
	if h.Get("Connection") == "" {
		h.Set("Connection", "keep-alive")
	}
	if h.Get("X-Accel-Buffering") == "" {
		h.Set("X-Accel-Buffering", "no")
	}
	s.w.WriteHeader(http.StatusOK)
	s.flusher.Flush()
	s.wroteHeader = true
}

// WriteJSON emits one SSE frame whose data: line is the JSON
// representation of v. Returns the first error from a previous write
// or this one.
func (s *SSEWriter) WriteJSON(v any) error {
	if s.err != nil {
		return s.err
	}
	payload, err := json.Marshal(v)
	if err != nil {
		s.err = err
		return err
	}
	return s.writeFrame(payload)
}

// WriteRaw emits one SSE frame with the given bytes as the data:
// payload (used for OpenAI's `data: [DONE]` sentinel).
func (s *SSEWriter) WriteRaw(payload []byte) error {
	if s.err != nil {
		return s.err
	}
	return s.writeFrame(payload)
}

// WriteEvent emits a named SSE event (`event: name\ndata: ...\n\n`).
// Anthropic's protocol uses this form for every chunk.
func (s *SSEWriter) WriteEvent(name string, v any) error {
	if s.err != nil {
		return s.err
	}
	payload, err := json.Marshal(v)
	if err != nil {
		s.err = err
		return err
	}
	s.writeHeadersOnce()
	if _, err := s.w.Write([]byte("event: " + name + "\ndata: ")); err != nil {
		s.err = err
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		s.err = err
		return err
	}
	if _, err := s.w.Write([]byte("\n\n")); err != nil {
		s.err = err
		return err
	}
	s.flusher.Flush()
	return nil
}

func (s *SSEWriter) writeFrame(payload []byte) error {
	s.writeHeadersOnce()
	if _, err := s.w.Write([]byte("data: ")); err != nil {
		s.err = err
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		s.err = err
		return err
	}
	if _, err := s.w.Write([]byte("\n\n")); err != nil {
		s.err = err
		return err
	}
	s.flusher.Flush()
	return nil
}

// Err returns the first error encountered by any Write call.
func (s *SSEWriter) Err() error { return s.err }
