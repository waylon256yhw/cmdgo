package pool

import (
	"errors"
	"hash/maphash"
	"sort"
	"sync"

	"github.com/waylon256yhw/cmdgo/internal/store"
)

// ErrNoHealthyAccount is returned by Pick when no eligible account
// remains after applying the health filter.
var ErrNoHealthyAccount = errors.New("pool: no healthy account available")

// Pool implements plan §10: hash-based affinity routing over the
// subset of accounts that pass a credit + error-rate health filter.
// All methods are safe for concurrent use.
type Pool struct {
	store *store.Store

	mu    sync.Mutex
	stats map[string]*RollingStats
	seed  maphash.Seed
}

// New constructs a Pool backed by st. The hash seed is generated once
// here so successive Picks land on the same shard for the same input,
// but two cmdgo processes will hash to different shards (which is
// fine — affinity is intra-process).
func New(st *store.Store) *Pool {
	return &Pool{
		store: st,
		stats: make(map[string]*RollingStats),
		seed:  maphash.MakeSeed(),
	}
}

// PickOptions controls how Pick filters and chooses an account.
type PickOptions struct {
	// ClientToken / Model / MessagesPrefix are the affinity inputs.
	// Identical inputs land on the same healthy account every time.
	ClientToken    string
	Model          string
	MessagesPrefix []byte

	// Exclude lists account IDs to skip — used by the retry loop so
	// the second attempt picks a different shard.
	Exclude map[string]bool
}

// Pick returns the next account to serve a request, applying the
// health filter (credits ≥ Settings.MinCreditsUSD, error rate <
// Settings.MaxErrorRate5min, !Paused) and then routing inside the
// healthy set by hash(clientToken|model|prefix).
func (p *Pool) Pick(opts PickOptions) (*store.Account, error) {
	snap := p.store.Snapshot()
	settings := snap.Settings

	// Sort by ID for deterministic shard ordering — without this,
	// hash%len would map to different accounts if state.json is
	// reordered.
	candidates := make([]store.Account, 0, len(snap.Accounts))
	for _, acc := range snap.Accounts {
		if acc.Paused || acc.APIKey == "" {
			continue
		}
		if settings.MinCreditsUSD > 0 && acc.LastKnownCredits > 0 && acc.LastKnownCredits < settings.MinCreditsUSD {
			continue
		}
		if opts.Exclude[acc.ID] {
			continue
		}
		if p.errorRate(acc.ID) > settings.MaxErrorRate5min && settings.MaxErrorRate5min > 0 {
			continue
		}
		candidates = append(candidates, acc)
	}
	if len(candidates) == 0 {
		return nil, ErrNoHealthyAccount
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })

	var h maphash.Hash
	h.SetSeed(p.seed)
	_, _ = h.WriteString(opts.ClientToken)
	_ = h.WriteByte('|')
	_, _ = h.WriteString(opts.Model)
	_ = h.WriteByte('|')
	_, _ = h.Write(opts.MessagesPrefix)
	idx := int(h.Sum64() % uint64(len(candidates)))
	chosen := candidates[idx]
	return &chosen, nil
}

// MarkSuccess records a successful upstream interaction for stats.
func (p *Pool) MarkSuccess(accountID string) {
	p.statsFor(accountID).Record(true)
}

// MarkError records a failed upstream interaction. Once the rolling
// error rate climbs above Settings.MaxErrorRate5min the account drops
// out of the healthy set on subsequent Pick calls.
func (p *Pool) MarkError(accountID string) {
	p.statsFor(accountID).Record(false)
}

// Stats returns the current rolling counters for an account. Returns
// (0, 0) for accounts with no recorded history.
func (p *Pool) Stats(accountID string) (reqs, errs int) {
	p.mu.Lock()
	rs, ok := p.stats[accountID]
	p.mu.Unlock()
	if !ok {
		return 0, 0
	}
	return rs.Snapshot()
}

func (p *Pool) errorRate(id string) float64 {
	p.mu.Lock()
	rs, ok := p.stats[id]
	p.mu.Unlock()
	if !ok {
		return 0
	}
	return rs.ErrorRate()
}

func (p *Pool) statsFor(id string) *RollingStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	rs, ok := p.stats[id]
	if !ok {
		rs = NewRollingStats(nil)
		p.stats[id] = rs
	}
	return rs
}
