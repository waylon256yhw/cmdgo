package proxy

import (
	"errors"

	"github.com/waylon256yhw/cmdgo/internal/pool"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// ErrNoHealthyAccount is returned by the pool stand-in when no account
// is currently eligible to serve a request. The real pool surfaces
// pool.ErrNoHealthyAccount with the same semantics — handlers compare
// against both.
var ErrNoHealthyAccount = errors.New("proxy: no healthy account available")

// poolErrNoHealthyAccount is a local alias so error mapping can match
// either sentinel without importing pool at every handler site.
var poolErrNoHealthyAccount = pool.ErrNoHealthyAccount

// pickAccount is a deliberately dumb stand-in for the real Pool that
// lands in commit 5 (internal/pool). It returns the first non-paused
// account with a non-empty apikey. Commit 5 replaces the call sites
// with affinity-aware routing + rolling-stats health filtering.
func pickAccount(st *store.Store) (*store.Account, error) {
	snap := st.Snapshot()
	for i := range snap.Accounts {
		acc := &snap.Accounts[i]
		if !acc.Paused && acc.APIKey != "" {
			return acc, nil
		}
	}
	return nil, ErrNoHealthyAccount
}
