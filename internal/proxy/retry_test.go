package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/pool"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// twoAccountSetup spins up:
//   - a mock CC whose /alpha/generate behaviour is supplied by `handle`
//   - a store with two accounts
//   - a pool + runner using that mock
//
// `handle` is invoked once per upstream attempt and gets the request
// body so it can decide based on which account/SID/attempt this is.
func twoAccountSetup(t *testing.T, handle func(int, *http.Request, http.ResponseWriter)) (*Runner, *pool.Pool, *store.Store, func()) {
	t.Helper()
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/alpha/generate", func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		handle(n, r, w)
	})
	srv := httptest.NewServer(mux)

	st, err := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Update(func(s *store.State) error {
		s.Accounts = []store.Account{
			{ID: "alpha", APIKey: "user_alpha1111111", LastKnownCredits: 9},
			{ID: "beta", APIKey: "user_beta22222222", LastKnownCredits: 9},
		}
		return nil
	})
	p := pool.New(st)
	r := &Runner{
		Pool:   p,
		CC:     cc.NewWithBaseURL(srv.URL),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return r, p, st, srv.Close
}

func writeSSE(w http.ResponseWriter, frames ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	for _, f := range frames {
		_, _ = io.WriteString(w, "data: "+f+"\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func makeCanon() *Canonical {
	return &Canonical{
		Model:       "deepseek/deepseek-v4-pro",
		Messages:    []json.RawMessage{json.RawMessage(`{"role":"user","content":"hi"}`)},
		ClientToken: "tok",
		Protocol:    "openai",
	}
}

func TestRunnerHappyPathFirstAttemptSucceeds(t *testing.T) {
	r, _, _, cleanup := twoAccountSetup(t, func(n int, r *http.Request, w http.ResponseWriter) {
		writeSSE(w,
			`{"type":"start"}`,
			`{"type":"text-delta","text":"pong"}`,
			`{"type":"finish","finishReason":"stop","totalUsage":{}}`,
		)
	})
	defer cleanup()

	att, err := r.Execute(context.Background(), makeCanon())
	if err != nil {
		t.Fatal(err)
	}
	defer att.Response.Body.Close()
	if att.Retried {
		t.Error("Retried=true on happy path")
	}
	if att.FirstEvent == nil || att.FirstEvent.Type != "start" {
		t.Fatalf("FirstEvent=%+v", att.FirstEvent)
	}
}

func TestRunnerRetriesOn5xx(t *testing.T) {
	var firstAccountAuth, secondAccountAuth string
	r, _, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		if n == 1 {
			firstAccountAuth = req.Header.Get("Authorization")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"success":false,"error":{"code":"BAD_GATEWAY","status":502,"message":"upstream is sad"}}`)
			return
		}
		secondAccountAuth = req.Header.Get("Authorization")
		writeSSE(w, `{"type":"start"}`, `{"type":"finish","finishReason":"stop","totalUsage":{}}`)
	})
	defer cleanup()

	att, err := r.Execute(context.Background(), makeCanon())
	if err != nil {
		t.Fatal(err)
	}
	defer att.Response.Body.Close()
	if !att.Retried {
		t.Error("Retried=false; expected retry after 502")
	}
	if firstAccountAuth == "" || secondAccountAuth == "" {
		t.Fatalf("missing attempt: first=%q second=%q", firstAccountAuth, secondAccountAuth)
	}
	if firstAccountAuth == secondAccountAuth {
		t.Errorf("retry used the same account: %q", firstAccountAuth)
	}
}

func TestRunnerDoesNotRetryOn403(t *testing.T) {
	r, _, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"success":false,"error":{"code":"FORBIDDEN","status":403,"message":"MODEL_NOT_IN_PLAN"}}`)
	})
	defer cleanup()

	_, err := r.Execute(context.Background(), makeCanon())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	ae, ok := err.(*cc.APIError)
	if !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Fatalf("err=%v", err)
	}
}

func TestRunnerRetriesOnStreamErrorRetryable(t *testing.T) {
	r, _, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		if n == 1 {
			writeSSE(w, `{"type":"error","error":{"type":"server_error","message":"flaky","statusCode":503,"isRetryable":true}}`)
			return
		}
		writeSSE(w, `{"type":"start"}`, `{"type":"finish","finishReason":"stop","totalUsage":{}}`)
	})
	defer cleanup()

	att, err := r.Execute(context.Background(), makeCanon())
	if err != nil {
		t.Fatal(err)
	}
	defer att.Response.Body.Close()
	if !att.Retried {
		t.Error("Retried=false; expected retry on retryable stream error")
	}
}

func TestRunnerPropagatesStreamErrorNonRetryable(t *testing.T) {
	r, _, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		writeSSE(w, `{"type":"error","error":{"type":"invalid_request","message":"bad input","statusCode":400,"isRetryable":false}}`)
	})
	defer cleanup()
	_, err := r.Execute(context.Background(), makeCanon())
	if err == nil {
		t.Fatal("expected error")
	}
	ae, ok := err.(*cc.APIError)
	if !ok {
		t.Fatalf("err=%T %v", err, err)
	}
	if ae.HTTPStatus != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", ae.HTTPStatus)
	}
}

func TestRunnerExhaustsBudgetAndReturnsLastErr(t *testing.T) {
	r, _, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"success":false,"error":{"code":"FAULT","status":503,"message":"down"}}`)
	})
	defer cleanup()

	start := time.Now()
	_, err := r.Execute(context.Background(), makeCanon())
	if err == nil {
		t.Fatal("expected error")
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		// 0 + 250ms + 750ms ~ 1s minimum
		t.Errorf("retry backoff too short: %v", elapsed)
	}
}

func TestClassifyOpenError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want FailureClass
	}{
		{"context_canceled", context.Canceled, classClient},
		{"context_deadline", context.DeadlineExceeded, classClient},
		{"http_401", &cc.APIError{HTTPStatus: 401, Body: cc.APIErrorBody{Code: "UNAUTHORIZED"}}, classAccount},
		{"http_403", &cc.APIError{HTTPStatus: 403, Body: cc.APIErrorBody{Code: "FORBIDDEN"}}, classAccount},
		{"http_429", &cc.APIError{HTTPStatus: 429}, classTransient},
		{"http_502", &cc.APIError{HTTPStatus: 502}, classTransient},
		{"network_bare", io.ErrUnexpectedEOF, classTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyOpenError(tc.err); got != tc.want {
				t.Errorf("classifyOpenError(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyStreamError(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want FailureClass
	}{
		{
			"retryable_true",
			`{"type":"error","error":{"type":"server_error","statusCode":503,"isRetryable":true}}`,
			classTransient,
		},
		{
			"retryable_false",
			`{"type":"error","error":{"type":"invalid_request","statusCode":400,"isRetryable":false}}`,
			classAccount,
		},
		{
			"retryable_unset_5xx_defaults_retry",
			`{"type":"error","error":{"type":"server_error","statusCode":503}}`,
			classTransient,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyStreamError(json.RawMessage(tc.raw)); got != tc.want {
				t.Errorf("classifyStreamError(%s) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRunnerRetryEachAttemptUsesFreshSID(t *testing.T) {
	var sids []string
	r, _, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		sids = append(sids, req.Header.Get("X-Session-Id"))
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"success":false,"error":{"code":"BAD","status":502,"message":""}}`)
			return
		}
		writeSSE(w, `{"type":"start"}`, `{"type":"finish","finishReason":"stop","totalUsage":{}}`)
	})
	defer cleanup()

	att, err := r.Execute(context.Background(), makeCanon())
	if err != nil {
		t.Fatal(err)
	}
	defer att.Response.Body.Close()
	if len(sids) < 2 {
		t.Fatalf("got %d attempts, want >= 2", len(sids))
	}
	if sids[0] == sids[1] {
		t.Errorf("retry reused session id: %q", sids[0])
	}
}
