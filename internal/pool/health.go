// Package pool owns affinity-aware routing across the configured
// Command Code accounts and the 5-minute rolling stats used to weed
// out unhealthy ones. The proxy adapters call Pool.Pick on every
// request; the retry loop calls Pool.MarkSuccess / Pool.MarkError as
// it sees the outcome.
package pool

import (
	"sync"
	"time"
)

// rollingBucketCount is the resolution of the rolling-stats window.
// Six 50-second buckets give us a 5-minute window with manageable
// granularity.
const (
	rollingBucketCount    = 6
	rollingBucketDuration = 50 * time.Second
	rollingWindow         = rollingBucketCount * rollingBucketDuration // 5m
)

// RollingStats counts requests and errors observed in the last
// rollingWindow seconds. buckets[0] is always the bucket currently
// being written to; older buckets shift toward higher indices and
// drop off the end of the array as time advances.
type RollingStats struct {
	mu      sync.Mutex
	clock   func() time.Time
	buckets [rollingBucketCount]bucket
	head    time.Time // start of buckets[0]
}

type bucket struct {
	reqs int
	errs int
}

// NewRollingStats returns a stats tracker. clock is overridable for
// tests; pass nil to use time.Now.
func NewRollingStats(clock func() time.Time) *RollingStats {
	if clock == nil {
		clock = time.Now
	}
	return &RollingStats{clock: clock}
}

// Record increments req count and, when success is false, the err
// count of the current bucket.
func (r *RollingStats) Record(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rollLocked(r.clock())
	r.buckets[0].reqs++
	if !success {
		r.buckets[0].errs++
	}
}

// Snapshot returns the totals across all live buckets.
func (r *RollingStats) Snapshot() (reqs, errs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rollLocked(r.clock())
	for _, b := range r.buckets {
		reqs += b.reqs
		errs += b.errs
	}
	return reqs, errs
}

// ErrorRate returns errs/reqs in the rolling window, or 0 if no
// requests have been recorded — an empty history means "no evidence
// of trouble", not "definitely broken".
func (r *RollingStats) ErrorRate() float64 {
	reqs, errs := r.Snapshot()
	if reqs == 0 {
		return 0
	}
	return float64(errs) / float64(reqs)
}

// rollLocked advances the buckets so buckets[0] corresponds to the
// bucket containing `now`. Older entries move to higher indices and
// are zeroed once they exit the window.
func (r *RollingStats) rollLocked(now time.Time) {
	cur := now.Truncate(rollingBucketDuration)
	if r.head.IsZero() {
		r.head = cur
		return
	}
	if !cur.After(r.head) {
		return
	}
	shifts := int(cur.Sub(r.head) / rollingBucketDuration)
	if shifts >= rollingBucketCount {
		for i := range r.buckets {
			r.buckets[i] = bucket{}
		}
		r.head = cur
		return
	}
	for i := rollingBucketCount - 1; i >= shifts; i-- {
		r.buckets[i] = r.buckets[i-shifts]
	}
	for i := 0; i < shifts; i++ {
		r.buckets[i] = bucket{}
	}
	r.head = cur
}
