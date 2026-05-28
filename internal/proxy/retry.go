package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/pool"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// retryBackoffs is the delay before attempt index N (0-based). The
// total wall-clock cost of all retries is bounded at ~1s.
var retryBackoffs = []time.Duration{0, 250 * time.Millisecond, 750 * time.Millisecond}

// FailureClass groups upstream failures by what they tell us about the
// account's health, so the retry path can route each into the right
// follow-up — only classAccount actually poisons rolling stats.
//
//   - classAccount   — the apikey itself is broken (401, INVALID_API_KEY,
//                      INSUFFICIENT_CREDITS, ACCOUNT_SUSPENDED). MarkError.
//   - classProtocol  — request is malformed or model not on plan
//                      (MODEL_NOT_IN_PLAN, INVALID_REQUEST, generic 4xx).
//                      The account is innocent; propagate to caller.
//   - classTransient — upstream link wobble (5xx, network, retryable
//                      stream error). The account is innocent; retry.
//   - classClient    — client cancelled or its connection died. Nothing
//                      to retry, nothing to mark.
//
// Commit 1 wires the enum and the markError wrapper without changing
// behaviour (the wrapper currently always calls MarkError). Commit 2
// gates MarkError on class; Commit 1.5 fills in real classifiers.
type FailureClass int

const (
	classAccount FailureClass = iota
	classProtocol
	classTransient
	classClient
)

func (c FailureClass) String() string {
	switch c {
	case classAccount:
		return "account"
	case classProtocol:
		return "protocol"
	case classTransient:
		return "transient"
	case classClient:
		return "client"
	}
	return "unknown"
}

// markError is the single entry point for poisoning an account's
// rolling stats. Only classAccount counts — protocol errors are
// request-shape problems, transient errors are upstream-link wobble
// (a future commit wires them into a separate Pool.MarkTransient
// counter for dashboard observability without affecting Pick), and
// client errors mean the user hung up before we even saw a response.
//
// Treating all three of those as "this account is unhealthy" caused
// single-account deployments to evict their only key after a single
// network blip; that's the exact regression we're fixing.
func markError(p *pool.Pool, accountID string, class FailureClass) {
	if class != classAccount {
		return
	}
	if p != nil {
		p.MarkError(accountID)
	}
}

// UpstreamAttempt is the result of one successful upstream open. The
// caller owns Response.Body and must close it. FirstEvent is the
// already-consumed first SSE frame from the stream — the adapter
// should write it to the client before scanning the rest of the
// stream.
type UpstreamAttempt struct {
	Response   *http.Response
	Scanner    *cc.Scanner
	FirstEvent *cc.StreamEvent // may be nil if upstream returned an empty stream
	Account    *store.Account
	Retried    bool
}

// AccumulatedAttempt is the result of a fully-drained non-streaming
// upstream call. ExecuteAccumulated returns this; the caller
// serialises Accumulated into the protocol-specific JSON response.
//
// Unlike UpstreamAttempt, the body is already consumed and closed —
// non-streaming callers don't need streaming access to the upstream.
type AccumulatedAttempt struct {
	Accumulated *AccumulatedResponse
	Account     *store.Account
	Retried     bool
}

// Runner ties together a Pool and a cc.Client. Adapters acquire one
// via NewRunner and call Execute with the canonical request.
type Runner struct {
	Pool   *pool.Pool
	CC     *cc.Client
	Logger *slog.Logger
}

// Execute opens an upstream stream, retrying up to 3 attempts total
// when retryable failures hit *before any byte is flushed to the
// client*. Per plan §9 the rules are:
//
//   - network / context errors          → retry
//   - HTTP 5xx                          → retry
//   - HTTP 429 (Retry-After respected)  → retry
//   - SSE error{isRetryable:true}       → retry
//   - HTTP 4xx (except 429)             → propagate
//   - SSE error{isRetryable:false}      → propagate
//
// Each retry hashes a different bucket (via PickOptions.Exclude) and
// uses a fresh x-session-id so CC routes us to a different upstream
// pod.
func (r *Runner) Execute(ctx context.Context, canon *Canonical) (*UpstreamAttempt, error) {
	tried := make(map[string]bool)
	var lastErr error
	prefix := MessagesPrefix(canon.Messages)
	hadAttempt := false

	for attempt := 0; attempt < len(retryBackoffs); attempt++ {
		if backoff := retryBackoffs[attempt]; backoff > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		pickOpts := pool.PickOptions{
			ClientToken:    canon.ClientToken,
			Model:          canon.Model,
			MessagesPrefix: prefix,
			Exclude:        tried,
		}
		acc, err := r.Pool.Pick(pickOpts)
		if errors.Is(err, pool.ErrNoHealthyAccount) && len(tried) > 0 {
			// Single-account (or fully-exhausted-pool) deployments would
			// otherwise burn the rest of the retry budget here. Clear
			// the Exclude set and try again — the fresh x-session-id
			// (already keyed off attempt index in SessionID below) makes
			// CC route us to a different upstream pod, which is what
			// retries are really for.
			tried = make(map[string]bool)
			pickOpts.Exclude = tried
			acc, err = r.Pool.Pick(pickOpts)
		}
		if err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		tried[acc.ID] = true
		hadAttempt = true

		sid := SessionID(canon.ClientToken+"#"+strconv.Itoa(attempt), canon.Model, prefix)
		resp, err := r.CC.Generate(ctx, cc.GenerateOpts{
			APIKey:    acc.APIKey,
			SessionID: sid,
			Body:      BuildCCBody(canon),
		})
		if err != nil {
			// markError vs retry are two orthogonal decisions:
			//   - markError gates on FailureClass (does this prove the
			//     apikey is broken?).
			//   - retry gates on plan §9 retryability (would a fresh
			//     account / session id plausibly succeed?).
			// They overlap a lot but not always: 429 QUOTA_EXCEEDED is
			// classAccount (mark this key) AND retryable (a different
			// account in the pool will probably still serve).
			class := classifyOpenError(err)
			markError(r.Pool, acc.ID, class)
			if class == classClient {
				return nil, err
			}
			if !shouldRetryError(err) {
				return nil, err
			}
			r.Logger.Warn("upstream open failed, will retry",
				"attempt", attempt+1, "account", acc.ID, "class", class, "err", err)
			lastErr = err
			continue
		}

		scanner := cc.NewScanner(resp.Body)
		first, sErr := scanner.Next()
		if sErr != nil && !errors.Is(sErr, io.EOF) {
			_ = resp.Body.Close()
			class := classifyOpenError(sErr)
			markError(r.Pool, acc.ID, class)
			if class == classClient {
				return nil, sErr
			}
			r.Logger.Warn("upstream first-read failed, will retry",
				"attempt", attempt+1, "account", acc.ID, "class", class, "err", sErr)
			lastErr = sErr
			continue
		}

		// Empty stream: CC accepted the request and closed without
		// emitting a single event. Most often that means an upstream
		// pod died mid-response and CC gave up. Treat as classTransient
		// and retry — propagating an empty 200 to the client would
		// surface as a stuck/blank reply in the SDK.
		if first == nil && errors.Is(sErr, io.EOF) {
			_ = resp.Body.Close()
			markError(r.Pool, acc.ID, classTransient)
			r.Logger.Warn("upstream returned empty stream, will retry",
				"attempt", attempt+1, "account", acc.ID)
			lastErr = errors.New("proxy: upstream returned empty stream")
			continue
		}

		// CC sometimes sends an `error` event as the first SSE frame
		// instead of a non-2xx HTTP status. Respect its isRetryable
		// flag.
		if first != nil && first.Type == "error" {
			class := classifyStreamError(first.Raw)
			if isStreamErrorRetryable(first.Raw) {
				_ = resp.Body.Close()
				markError(r.Pool, acc.ID, class)
				r.Logger.Warn("upstream stream error (retryable), will retry",
					"attempt", attempt+1, "account", acc.ID, "class", class,
					"raw", truncate(first.Raw, 200))
				lastErr = fmt.Errorf("upstream stream error: %s", truncate(first.Raw, 200))
				continue
			}
			// Non-retryable — propagate as a synthetic 502 with the
			// upstream message so the caller can serialise to its
			// protocol's error envelope.
			ae := streamErrorToAPIError(first.Raw)
			_ = resp.Body.Close()
			markError(r.Pool, acc.ID, class)
			return nil, ae
		}

		return &UpstreamAttempt{
			Response:   resp,
			Scanner:    scanner,
			FirstEvent: first,
			Account:    acc,
			Retried:    attempt > 0,
		}, nil
	}

	if !hadAttempt && lastErr == nil {
		return nil, errors.New("proxy: no upstream attempt was made")
	}
	if lastErr == nil {
		lastErr = errors.New("proxy: retry budget exhausted")
	}
	return nil, lastErr
}

// ExecuteAccumulated is the non-streaming sibling of Execute. Because
// nothing is flushed to the client until the full response is built,
// the retry budget covers the *entire* CC stream — including mid-
// stream retryable errors. Compare to Execute, which can only retry
// before the first frame is handed to a streaming adapter (and from
// there to the wire). The pick / backoff / classify / mark logic is
// otherwise identical.
//
// Returns AccumulatedAttempt on success; the body is already drained
// and closed. On failure, the error is the same shape Execute would
// return so the protocol adapters can reuse mapCCErrorToOpenAI /
// mapCCErrorToAnthropic without branching.
func (r *Runner) ExecuteAccumulated(ctx context.Context, canon *Canonical) (*AccumulatedAttempt, error) {
	tried := make(map[string]bool)
	var lastErr error
	prefix := MessagesPrefix(canon.Messages)
	hadAttempt := false

	for attempt := 0; attempt < len(retryBackoffs); attempt++ {
		if backoff := retryBackoffs[attempt]; backoff > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		pickOpts := pool.PickOptions{
			ClientToken:    canon.ClientToken,
			Model:          canon.Model,
			MessagesPrefix: prefix,
			Exclude:        tried,
		}
		acc, err := r.Pool.Pick(pickOpts)
		if errors.Is(err, pool.ErrNoHealthyAccount) && len(tried) > 0 {
			tried = make(map[string]bool)
			pickOpts.Exclude = tried
			acc, err = r.Pool.Pick(pickOpts)
		}
		if err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		tried[acc.ID] = true
		hadAttempt = true

		sid := SessionID(canon.ClientToken+"#"+strconv.Itoa(attempt), canon.Model, prefix)
		resp, err := r.CC.Generate(ctx, cc.GenerateOpts{
			APIKey:    acc.APIKey,
			SessionID: sid,
			Body:      BuildCCBody(canon),
		})
		if err != nil {
			class := classifyOpenError(err)
			markError(r.Pool, acc.ID, class)
			if class == classClient {
				return nil, err
			}
			if !shouldRetryError(err) {
				return nil, err
			}
			r.Logger.Warn("accumulated upstream open failed, will retry",
				"attempt", attempt+1, "account", acc.ID, "class", class, "err", err)
			lastErr = err
			continue
		}

		// Drain the whole stream — non-streaming clients don't see
		// bytes until we've serialised JSON, so the retry budget
		// applies to anything that happens before this returns.
		scanner := cc.NewScanner(resp.Body)
		accum, accErr := AccumulateStream(scanner)
		_ = resp.Body.Close()

		if accErr != nil {
			// CC `error` frame mid-stream — same retryability rules as
			// Execute's first-frame error case, just at a later point
			// in the stream.
			var se *UpstreamStreamError
			if errors.As(accErr, &se) {
				class := classifyStreamError(se.Raw)
				if isStreamErrorRetryable(se.Raw) {
					markError(r.Pool, acc.ID, class)
					r.Logger.Warn("accumulated stream error (retryable), will retry",
						"attempt", attempt+1, "account", acc.ID, "class", class,
						"raw", truncate(se.Raw, 200))
					lastErr = se
					continue
				}
				ae := streamErrorToAPIError(se.Raw)
				markError(r.Pool, acc.ID, class)
				return nil, ae
			}
			// Bare scanner / I/O error mid-stream. classifyOpenError's
			// rules apply — client cancel propagates without retry;
			// everything else is transient.
			class := classifyOpenError(accErr)
			markError(r.Pool, acc.ID, class)
			if class == classClient {
				return nil, accErr
			}
			r.Logger.Warn("accumulated upstream read failed, will retry",
				"attempt", attempt+1, "account", acc.ID, "class", class, "err", accErr)
			lastErr = accErr
			continue
		}

		// EOF before the documented terminal `finish` event — empty
		// or truncated. For non-streaming this is always recoverable
		// because no bytes have been sent to the client, so we don't
		// have to decide whether the partial text we already
		// collected is "good enough". Retry as classTransient
		// (mirrors Execute's empty-stream handling). If the budget
		// is spent on truncations, the explicit error below
		// translates to a 502 with an empty_stream / truncated code
		// in the handler — much better than a 200 with a synthetic
		// finish_reason=length and zero usage.
		if !accum.FinishReceived {
			markError(r.Pool, acc.ID, classTransient)
			r.Logger.Warn("accumulated upstream truncated before finish, will retry",
				"attempt", attempt+1, "account", acc.ID, "blocks", len(accum.Blocks))
			lastErr = errors.New("proxy: upstream stream truncated before finish")
			continue
		}

		return &AccumulatedAttempt{
			Accumulated: accum,
			Account:     acc,
			Retried:     attempt > 0,
		}, nil
	}

	if !hadAttempt && lastErr == nil {
		return nil, errors.New("proxy: no upstream attempt was made")
	}
	if lastErr == nil {
		lastErr = errors.New("proxy: retry budget exhausted")
	}
	return nil, lastErr
}

// shouldRetryError encodes plan §9's retry policy for *open* errors
// (the HTTP-level handshake / non-2xx response from /alpha/generate).
// It deliberately does not look at FailureClass: a 429 with
// QUOTA_EXCEEDED is still retryable (the next pool attempt may land
// on an account that still has quota) even though the *current*
// account gets marked. classifyOpenError handles the mark decision;
// this handles the retry decision.
func shouldRetryError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ae *cc.APIError
	if errors.As(err, &ae) {
		if ae.HTTPStatus == http.StatusTooManyRequests {
			return true
		}
		if ae.HTTPStatus >= 500 && ae.HTTPStatus < 600 {
			return true
		}
		return false
	}
	// Bare errors from net/http (dial / read header / connection reset
	// before reply) are typically transient.
	return true
}

// accountMessagePrefixes are the CC-side message prefixes that mean
// "this apikey itself is broken". These dominate over HTTP status —
// CC sometimes returns 402/403 for INSUFFICIENT_CREDITS.
var accountMessagePrefixes = []string{
	"INVALID_API_KEY",
	"INSUFFICIENT_CREDITS",
	"QUOTA_EXCEEDED",
	"ACCOUNT_SUSPENDED",
}

// protocolMessagePrefixes are messages that describe a request-shape
// or plan-tier problem — the account is innocent. Notably
// MODEL_NOT_IN_PLAN arrives under HTTP 403 / code FORBIDDEN, which
// without this list would look identical to a real auth failure.
var protocolMessagePrefixes = []string{
	"MODEL_NOT_IN_PLAN",
	"INVALID_MODEL",
	"INVALID_REQUEST",
}

// classifyOpenError maps an error from the upstream open (HTTP dial,
// header read, /alpha/generate non-2xx) into a FailureClass. Decision
// order, per docs/cc-api.md §4:
//
//  1. context errors → classClient (caller's fault, not the apikey's).
//  2. *cc.APIError with a known account/protocol message prefix wins
//     outright (CC packs the real reason into the message head; the
//     code is too coarse — FORBIDDEN covers both MODEL_NOT_IN_PLAN and
//     real bans).
//  3. *cc.APIError code: UNAUTHORIZED / INVALID_API_KEY → classAccount;
//     FORBIDDEN with no prefix match defaults to classProtocol so a
//     single 403 doesn't poison the account.
//  4. HTTP status fallback: 401 = classAccount, 429/5xx = classTransient,
//     other 4xx = classProtocol.
//  5. Bare error (dial timeout, EOF, reset) = classTransient.
func classifyOpenError(err error) FailureClass {
	if err == nil {
		return classAccount
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return classClient
	}
	var ae *cc.APIError
	if errors.As(err, &ae) {
		if class, ok := classifyByMessage(ae.Body.Message); ok {
			return class
		}
		switch strings.ToUpper(ae.Body.Code) {
		case "UNAUTHORIZED", "INVALID_API_KEY":
			return classAccount
		case "FORBIDDEN":
			// Already passed the message-prefix gate above. Without a
			// prefix CC's 403 is ambiguous; default to protocol so a
			// stray 403 doesn't kill the only healthy account.
			return classProtocol
		}
		switch {
		case ae.HTTPStatus == http.StatusUnauthorized:
			return classAccount
		case ae.HTTPStatus == http.StatusTooManyRequests:
			return classTransient
		case ae.HTTPStatus >= 500 && ae.HTTPStatus < 600:
			return classTransient
		case ae.HTTPStatus >= 400 && ae.HTTPStatus < 500:
			return classProtocol
		}
	}
	return classTransient
}

// classifyStreamError maps a CC-emitted `error` SSE frame to a
// FailureClass. Same priority as classifyOpenError: message prefix →
// inner type/code → isRetryable + statusCode fallback.
func classifyStreamError(raw json.RawMessage) FailureClass {
	var pl struct {
		Error struct {
			Type        string `json:"type"`
			Message     string `json:"message"`
			Code        string `json:"code"`
			StatusCode  int    `json:"statusCode"`
			IsRetryable *bool  `json:"isRetryable"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &pl); err != nil {
		// Malformed payload — historic behaviour was "treat as account
		// error". Keep that, since we can't reason about it.
		return classAccount
	}
	if class, ok := classifyByMessage(pl.Error.Message); ok {
		return class
	}
	switch strings.ToUpper(pl.Error.Code) {
	case "UNAUTHORIZED", "INVALID_API_KEY":
		return classAccount
	case "FORBIDDEN":
		return classProtocol
	}
	switch strings.ToLower(pl.Error.Type) {
	case "invalid_request", "invalid_request_error":
		return classProtocol
	case "unauthorized", "authentication_error":
		return classAccount
	}
	if pl.Error.IsRetryable != nil && *pl.Error.IsRetryable {
		return classTransient
	}
	switch {
	case pl.Error.StatusCode >= 500 && pl.Error.StatusCode < 600:
		return classTransient
	case pl.Error.StatusCode == http.StatusUnauthorized:
		return classAccount
	case pl.Error.StatusCode >= 400 && pl.Error.StatusCode < 500:
		return classProtocol
	}
	return classTransient
}

// classifyByMessage inspects a CC error message for the well-known
// SCREAMING_SNAKE prefixes. Returns (_, false) when no prefix matches.
func classifyByMessage(msg string) (FailureClass, bool) {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return 0, false
	}
	upper := strings.ToUpper(msg)
	for _, p := range accountMessagePrefixes {
		if strings.HasPrefix(upper, p) {
			return classAccount, true
		}
	}
	for _, p := range protocolMessagePrefixes {
		if strings.HasPrefix(upper, p) {
			return classProtocol, true
		}
	}
	return 0, false
}

func isStreamErrorRetryable(raw json.RawMessage) bool {
	var pl struct {
		Error struct {
			IsRetryable *bool  `json:"isRetryable"`
			StatusCode  int    `json:"statusCode"`
			Type        string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &pl); err != nil {
		return false
	}
	if pl.Error.IsRetryable != nil {
		return *pl.Error.IsRetryable
	}
	// No explicit flag — default to retry on 5xx-shaped errors.
	if pl.Error.StatusCode >= 500 && pl.Error.StatusCode < 600 {
		return true
	}
	return false
}

func streamErrorToAPIError(raw json.RawMessage) *cc.APIError {
	var pl struct {
		Error struct {
			Type       string `json:"type"`
			Message    string `json:"message"`
			StatusCode int    `json:"statusCode"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &pl)
	status := pl.Error.StatusCode
	if status == 0 {
		status = http.StatusBadGateway
	}
	return &cc.APIError{
		HTTPStatus: status,
		Body: cc.APIErrorBody{
			Code:    pl.Error.Type,
			Status:  status,
			Message: pl.Error.Message,
		},
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// clientGone reports whether streamErr looks like a client-side
// disconnect rather than an upstream / account problem. Used to avoid
// poisoning a healthy account's rolling stats when the user just
// closed their browser tab mid-stream.
func clientGone(r *http.Request, streamErr error) bool {
	if r != nil && r.Context().Err() != nil {
		return true
	}
	if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
		return true
	}
	// net/http surfaces client-side resets as "use of closed network
	// connection" or "broken pipe" — these are string-typed; recognise
	// the most common ones defensively.
	msg := streamErr.Error()
	switch {
	case strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection reset by peer"),
		strings.Contains(msg, "use of closed network connection"):
		return true
	}
	return false
}

// openAttempt runs Runner.Execute when the handler has one configured;
// otherwise it falls back to the no-retry placeholder path so unit
// tests that don't supply a pool keep working. The second return is
// the account ID used (for response headers / stats).
func openAttempt(
	ctx context.Context,
	runner *Runner,
	st *store.Store,
	client *cc.Client,
	logger *slog.Logger,
	canon *Canonical,
) (*UpstreamAttempt, string, error) {
	if runner != nil {
		att, err := runner.Execute(ctx, canon)
		if err != nil {
			return nil, "", err
		}
		return att, att.Account.ID, nil
	}
	acc, err := pickAccount(st)
	if err != nil {
		return nil, "", err
	}
	body := BuildCCBody(canon)
	sid := SessionID(canon.ClientToken, canon.Model, MessagesPrefix(canon.Messages))
	resp, err := client.Generate(ctx, cc.GenerateOpts{
		APIKey:    acc.APIKey,
		SessionID: sid,
		Body:      body,
	})
	if err != nil {
		return nil, acc.ID, err
	}
	scanner := cc.NewScanner(resp.Body)
	return &UpstreamAttempt{
		Response: resp,
		Scanner:  scanner,
		Account:  acc,
	}, acc.ID, nil
}

// openAccumulated is the non-streaming analogue of openAttempt:
// Runner.ExecuteAccumulated when a runner is wired, otherwise a
// no-retry fallback that opens, drains, and closes the upstream so
// tests without a pool keep working.
func openAccumulated(
	ctx context.Context,
	runner *Runner,
	st *store.Store,
	client *cc.Client,
	logger *slog.Logger,
	canon *Canonical,
) (*AccumulatedAttempt, string, error) {
	if runner != nil {
		att, err := runner.ExecuteAccumulated(ctx, canon)
		if err != nil {
			return nil, "", err
		}
		return att, att.Account.ID, nil
	}
	acc, err := pickAccount(st)
	if err != nil {
		return nil, "", err
	}
	sid := SessionID(canon.ClientToken, canon.Model, MessagesPrefix(canon.Messages))
	resp, err := client.Generate(ctx, cc.GenerateOpts{
		APIKey:    acc.APIKey,
		SessionID: sid,
		Body:      BuildCCBody(canon),
	})
	if err != nil {
		return nil, acc.ID, err
	}
	accum, accErr := AccumulateStream(cc.NewScanner(resp.Body))
	_ = resp.Body.Close()
	if accErr != nil {
		return nil, acc.ID, accErr
	}
	return &AccumulatedAttempt{
		Accumulated: accum,
		Account:     acc,
	}, acc.ID, nil
}
