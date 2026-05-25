// Package server holds the HTTP middlewares and handlers that wire the
// rest of cmdgo into a single net/http server. Individual feature
// handlers (oauth, dashboard partials, traffic SSE) live in their own
// files in this package as subsequent commits land.
package server

import (
	"crypto/subtle"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/store"
)

// RequireProxyToken returns a middleware that gates a handler behind the
// proxy token persisted in state.json. Expect `Authorization: Bearer
// pcc_...`; anything else is 401.
func RequireProxyToken(s *store.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(h, prefix) {
				writeAuthError(w, "missing_authorization", "missing or malformed Authorization header")
				return
			}
			got := h[len(prefix):]
			want := s.Snapshot().ProxyToken
			if want == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
				writeAuthError(w, "invalid_token", "proxy token did not match the value in state.json")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func writeAuthError(w http.ResponseWriter, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", `Bearer realm="cmdgo"`)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = io.WriteString(w, `{"error":{"type":"unauthorized","code":"`+code+`","message":"`+msg+`"}}`+"\n")
}

// WithAccessLog logs one structured line per request. Keep it lean: the
// proxy bodies will be voluminous in production, we only want method,
// path, status, duration, remote.
func WithAccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			logger.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.written,
				"dur_ms", time.Since(start).Milliseconds(),
				"remote", r.RemoteAddr,
			)
		})
	}
}

// statusRecorder lets WithAccessLog observe the status code and bytes
// written without swallowing the underlying ResponseWriter's Flush
// behaviour (needed for SSE in later commits).
type statusRecorder struct {
	http.ResponseWriter
	status      int
	written     int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.wroteHeader = true
	}
	n, err := r.ResponseWriter.Write(p)
	r.written += n
	return n, err
}

// Flush forwards to the underlying writer if it supports flushing. SSE
// handlers in later commits rely on this.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
