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
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/pool"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// retryBackoffs is the delay before attempt index N (0-based). The
// total wall-clock cost of all retries is bounded at ~1s.
var retryBackoffs = []time.Duration{0, 250 * time.Millisecond, 750 * time.Millisecond}

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

		acc, err := r.Pool.Pick(pool.PickOptions{
			ClientToken:    canon.ClientToken,
			Model:          canon.Model,
			MessagesPrefix: prefix,
			Exclude:        tried,
		})
		if err != nil {
			// Out of healthy accounts mid-retry — surface the failure
			// from the first failed attempt if we had one, since it's
			// more diagnostic than "no healthy account".
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
			r.Pool.MarkError(acc.ID)
			if !shouldRetryError(err) {
				return nil, err
			}
			r.Logger.Warn("upstream open failed, will retry",
				"attempt", attempt+1, "account", acc.ID, "err", err)
			lastErr = err
			continue
		}

		scanner := cc.NewScanner(resp.Body)
		first, sErr := scanner.Next()
		if sErr != nil && !errors.Is(sErr, io.EOF) {
			_ = resp.Body.Close()
			r.Pool.MarkError(acc.ID)
			r.Logger.Warn("upstream first-read failed, will retry",
				"attempt", attempt+1, "account", acc.ID, "err", sErr)
			lastErr = sErr
			continue
		}

		// CC sometimes sends an `error` event as the first SSE frame
		// instead of a non-2xx HTTP status. Respect its isRetryable
		// flag.
		if first != nil && first.Type == "error" {
			if isStreamErrorRetryable(first.Raw) {
				_ = resp.Body.Close()
				r.Pool.MarkError(acc.ID)
				r.Logger.Warn("upstream stream error (retryable), will retry",
					"attempt", attempt+1, "account", acc.ID, "raw", truncate(first.Raw, 200))
				lastErr = fmt.Errorf("upstream stream error: %s", truncate(first.Raw, 200))
				continue
			}
			// Non-retryable — propagate as a synthetic 502 with the
			// upstream message so the caller can serialise to its
			// protocol's error envelope.
			ae := streamErrorToAPIError(first.Raw)
			_ = resp.Body.Close()
			r.Pool.MarkError(acc.ID)
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

func shouldRetryError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ae *cc.APIError
	if errors.As(err, &ae) {
		// 429 → retry. 5xx → retry. Everything else → no.
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
