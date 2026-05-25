package pool

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// CreditSyncer runs in the background and refreshes every healthy
// account's LastKnownCredits at a configurable cadence. The interval
// comes from store.Settings.CreditPollSec (default 60s).
type CreditSyncer struct {
	Store    *store.Store
	CC       *cc.Client
	Logger   *slog.Logger
	OnUpdate func(accountID string) // optional, called after each refresh
}

// Run blocks until ctx is cancelled. Per-cycle errors are logged and
// swallowed — one paused account or temporary API hiccup shouldn't
// stop the whole loop.
func (s *CreditSyncer) Run(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	for {
		settings := s.Store.Snapshot().Settings
		interval := time.Duration(settings.CreditPollSec) * time.Second
		if interval <= 0 {
			interval = 60 * time.Second
		}
		s.cycle(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (s *CreditSyncer) cycle(ctx context.Context) {
	snap := s.Store.Snapshot()
	for _, acc := range snap.Accounts {
		if ctx.Err() != nil {
			return
		}
		if acc.Paused || acc.APIKey == "" {
			continue
		}
		reqCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		credits, err := s.CC.BillingCredits(reqCtx, acc.APIKey)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.Logger.Warn("credit sync failed",
				"account", acc.ID, "err", err,
			)
			continue
		}
		if err := s.Store.SetAccountCredits(acc.ID, credits.Total()); err != nil {
			s.Logger.Warn("persist credits failed", "account", acc.ID, "err", err)
			continue
		}
		if s.OnUpdate != nil {
			s.OnUpdate(acc.ID)
		}
	}
}
