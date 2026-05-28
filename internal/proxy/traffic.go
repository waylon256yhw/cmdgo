package proxy

import (
	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// TrafficRecorder is the surface the proxy adapters use to log one
// completed request. Implementations also forward to subscribers
// (the dashboard's SSE broadcaster). DashboardService implements
// this in internal/server; tests can supply a no-op.
type TrafficRecorder interface {
	RecordTraffic(entry store.TrafficEntry)
}

// streamSummary captures the per-stream token counts from CC's
// finish event so the adapter can build a TrafficEntry once the
// stream drains.
type streamSummary struct {
	InputTokens      int
	OutputTokens     int
	TotalTokens      int // as reported by CC; usually InputTokens+OutputTokens
	CacheReadTokens  int
	CacheWriteTokens int
	ReasoningTokens  int
	FinishReason     string
	Recorded         bool
}

// computeCostUSD applies the per-1M pricing from cc.GoModels.
// inputTokens is the *total* (CC's `inputTokens` already counts both
// cached and uncached), so we deduct cacheReadTokens before applying
// the non-cached input rate. cacheWriteTokens is billed separately
// when the model supports cache writes.
func computeCostUSD(model string, s streamSummary) float64 {
	m := cc.LookupGoModel(model)
	if m == nil {
		return 0
	}
	nonCached := s.InputTokens - s.CacheReadTokens
	if nonCached < 0 {
		nonCached = 0
	}
	cost := float64(nonCached) * m.Pricing.InputPerM / 1_000_000
	cost += float64(s.CacheReadTokens) * m.Pricing.CacheReadPerM / 1_000_000
	cost += float64(s.OutputTokens) * m.Pricing.OutputPerM / 1_000_000
	if s.CacheWriteTokens > 0 && m.Pricing.CacheWritePerM > 0 {
		cost += float64(s.CacheWriteTokens) * m.Pricing.CacheWritePerM / 1_000_000
	}
	return cost
}

// nopRecorder discards every entry — used by adapters when Recorder
// is nil so tests don't need to wire up a dashboard.
type nopRecorder struct{}

func (nopRecorder) RecordTraffic(store.TrafficEntry) {}
