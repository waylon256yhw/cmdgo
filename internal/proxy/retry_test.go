package proxy

import (
	"context"
	"encoding/json"
	"errors"
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

// oneAccountSetup is twoAccountSetup's single-account sibling — useful
// for exercising the "no alternative account" path in Runner.Execute.
func oneAccountSetup(t *testing.T, handle func(int, *http.Request, http.ResponseWriter)) (*Runner, *pool.Pool, *store.Store, func()) {
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
			{ID: "solo", APIKey: "user_solo111111", LastKnownCredits: 9},
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
		// Client-side
		{"context_canceled", context.Canceled, classClient},
		{"context_deadline", context.DeadlineExceeded, classClient},

		// Account: message prefix beats status code
		{
			"402_insufficient_credits",
			&cc.APIError{HTTPStatus: 402, Body: cc.APIErrorBody{Code: "FORBIDDEN", Message: "INSUFFICIENT_CREDITS: out of credit"}},
			classAccount,
		},
		{
			"401_invalid_api_key",
			&cc.APIError{HTTPStatus: 401, Body: cc.APIErrorBody{Code: "UNAUTHORIZED", Message: "INVALID_API_KEY: bad token"}},
			classAccount,
		},
		{
			"403_account_suspended",
			&cc.APIError{HTTPStatus: 403, Body: cc.APIErrorBody{Code: "FORBIDDEN", Message: "ACCOUNT_SUSPENDED: tos violation"}},
			classAccount,
		},
		// Account: code-only without prefix still wins
		{
			"401_no_message_unauthorized_code",
			&cc.APIError{HTTPStatus: 401, Body: cc.APIErrorBody{Code: "UNAUTHORIZED"}},
			classAccount,
		},

		// Protocol: MODEL_NOT_IN_PLAN under FORBIDDEN must NOT be account
		{
			"403_model_not_in_plan",
			&cc.APIError{HTTPStatus: 403, Body: cc.APIErrorBody{Code: "FORBIDDEN", Message: "MODEL_NOT_IN_PLAN: Claude Haiku 4.5 is Pro+"}},
			classProtocol,
		},
		{
			"400_invalid_request",
			&cc.APIError{HTTPStatus: 400, Body: cc.APIErrorBody{Code: "BAD_REQUEST", Message: "INVALID_REQUEST: missing field"}},
			classProtocol,
		},
		// Protocol: bare 403 FORBIDDEN with no prefix defaults to protocol
		// (single-account safety — don't kill the only key on a stray 403).
		{
			"403_bare_forbidden_no_prefix",
			&cc.APIError{HTTPStatus: 403, Body: cc.APIErrorBody{Code: "FORBIDDEN"}},
			classProtocol,
		},

		// Transient
		{"http_429", &cc.APIError{HTTPStatus: 429}, classTransient},
		{"http_502", &cc.APIError{HTTPStatus: 502}, classTransient},
		{"network_bare_unexpected_eof", io.ErrUnexpectedEOF, classTransient},
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
		// Message-prefix priority
		{
			"model_not_in_plan_under_forbidden_type",
			`{"type":"error","error":{"type":"forbidden","message":"MODEL_NOT_IN_PLAN: ...","statusCode":403,"isRetryable":false}}`,
			classProtocol,
		},
		{
			"insufficient_credits_isretryable_false",
			`{"type":"error","error":{"type":"server_error","message":"INSUFFICIENT_CREDITS: out","statusCode":402,"isRetryable":false}}`,
			classAccount,
		},
		{
			"invalid_request_prefix",
			`{"type":"error","error":{"type":"invalid_request","message":"INVALID_REQUEST: bad schema","statusCode":400,"isRetryable":false}}`,
			classProtocol,
		},

		// Code priority when no message prefix
		{
			"unauthorized_code_no_prefix",
			`{"type":"error","error":{"code":"UNAUTHORIZED","statusCode":401,"isRetryable":false}}`,
			classAccount,
		},

		// Inner type fallback when no prefix and no code
		{
			"inner_type_invalid_request",
			`{"type":"error","error":{"type":"invalid_request","statusCode":400,"isRetryable":false}}`,
			classProtocol,
		},

		// isRetryable + status fallback
		{
			"retryable_true_server_error",
			`{"type":"error","error":{"type":"server_error","statusCode":503,"isRetryable":true}}`,
			classTransient,
		},
		{
			"retryable_unset_5xx_defaults_transient",
			`{"type":"error","error":{"type":"server_error","statusCode":503}}`,
			classTransient,
		},
		{
			"retryable_unset_no_status_defaults_transient",
			`{"type":"error","error":{"message":"network blip"}}`,
			classTransient,
		},

		// Malformed payload → conservative classAccount (matches the
		// historic always-MarkError behaviour for unparseable errors).
		{
			"malformed_json",
			`not even json`,
			classAccount,
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

func TestPreFlushClientCancelDoesNotMarkError(t *testing.T) {
	hold := make(chan struct{})
	r, p, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		// Hang until the test releases us (or the client cancels the ctx,
		// which net/http surfaces by closing the body).
		<-hold
	})
	defer cleanup()
	defer close(hold)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel almost immediately so the upstream open returns a ctx error
	// from net/http's transport layer.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := r.Execute(ctx, makeCanon())
	if err == nil {
		t.Fatal("expected ctx error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx canceled, got %v", err)
	}
	// Neither account should have been poisoned — the client hung up,
	// the apikeys are still healthy.
	for _, id := range []string{"alpha", "beta"} {
		reqs, errs := p.Stats(id)
		if errs != 0 {
			t.Errorf("account %s: errs=%d, want 0 (client cancel must not MarkError); reqs=%d", id, errs, reqs)
		}
	}
}

func TestPreFlushModelNotInPlanDoesNotMarkError(t *testing.T) {
	r, p, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"success":false,"error":{"code":"FORBIDDEN","status":403,"message":"MODEL_NOT_IN_PLAN: Claude Haiku 4.5 is Pro+"}}`)
	})
	defer cleanup()

	_, err := r.Execute(context.Background(), makeCanon())
	if err == nil {
		t.Fatal("expected protocol error, got nil")
	}
	if _, ok := err.(*cc.APIError); !ok {
		t.Fatalf("expected *cc.APIError, got %T %v", err, err)
	}
	for _, id := range []string{"alpha", "beta"} {
		_, errs := p.Stats(id)
		if errs != 0 {
			t.Errorf("account %s: errs=%d, want 0 (MODEL_NOT_IN_PLAN is protocol, not account)", id, errs)
		}
	}
}

func TestPreFlushInvalidAPIKeyMarksError(t *testing.T) {
	var hitID string
	r, p, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		// Both accounts will return 401 — we don't care which one Pick
		// lands on, just that exactly one gets marked.
		hitID = req.Header.Get("Authorization")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"success":false,"error":{"code":"UNAUTHORIZED","status":401,"message":"INVALID_API_KEY: token revoked"}}`)
	})
	defer cleanup()
	_ = hitID

	_, err := r.Execute(context.Background(), makeCanon())
	if err == nil {
		t.Fatal("expected 401 error, got nil")
	}
	// Exactly one account should have been marked. (The 401 is non-
	// retryable, so retry budget isn't exercised.)
	total := 0
	for _, id := range []string{"alpha", "beta"} {
		_, errs := p.Stats(id)
		total += errs
	}
	if total != 1 {
		t.Errorf("expected exactly 1 MarkError across both accounts, got %d", total)
	}
}

func TestSingleAccountRetryRecovers(t *testing.T) {
	var sids []string
	r, p, _, cleanup := oneAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		sids = append(sids, req.Header.Get("X-Session-Id"))
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"success":false,"error":{"code":"BAD_GATEWAY","status":502,"message":"upstream sad"}}`)
			return
		}
		writeSSE(w, `{"type":"start"}`, `{"type":"finish","finishReason":"stop","totalUsage":{}}`)
	})
	defer cleanup()

	att, err := r.Execute(context.Background(), makeCanon())
	if err != nil {
		t.Fatalf("single-account retry failed to recover: %v", err)
	}
	defer att.Response.Body.Close()
	if !att.Retried {
		t.Error("Retried=false; expected retry on a single account")
	}
	if len(sids) < 2 || sids[0] == sids[1] {
		t.Errorf("expected distinct session ids across attempts, got %v", sids)
	}
	// Transient 502 must not have marked the only account.
	if _, errs := p.Stats("solo"); errs != 0 {
		t.Errorf("solo account: errs=%d, want 0", errs)
	}
}

func TestSingleAccountRetryAllFail(t *testing.T) {
	var attempts atomic.Int32
	r, _, _, cleanup := oneAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"success":false,"error":{"code":"FAULT","status":503,"message":"down"}}`)
	})
	defer cleanup()

	_, err := r.Execute(context.Background(), makeCanon())
	if err == nil {
		t.Fatal("expected error after exhausting retry budget")
	}
	if errors.Is(err, pool.ErrNoHealthyAccount) {
		t.Fatalf("got ErrNoHealthyAccount — the single account was self-excluded, want the upstream 503 error")
	}
	if got := attempts.Load(); int(got) != len(retryBackoffs) {
		t.Errorf("attempts=%d, want %d (full retry budget should be spent on the solo account)", got, len(retryBackoffs))
	}
}

func TestPreFlush502DoesNotMarkErrorButRetries(t *testing.T) {
	r, p, _, cleanup := twoAccountSetup(t, func(n int, req *http.Request, w http.ResponseWriter) {
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"success":false,"error":{"code":"BAD_GATEWAY","status":502,"message":"upstream"}}`)
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
		t.Error("Retried=false; expected retry after transient 502")
	}
	for _, id := range []string{"alpha", "beta"} {
		_, errs := p.Stats(id)
		if errs != 0 {
			t.Errorf("account %s: errs=%d, want 0 (transient 502 must not MarkError)", id, errs)
		}
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
