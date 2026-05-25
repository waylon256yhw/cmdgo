// Package store owns the on-disk JSON state file (`~/.cmdgo/state.json`).
// All persistent runtime data lives here: proxy token, account list,
// per-account credit snapshots, and the rolling traffic log.
//
// Writes go through a tmp-file + rename to keep the file atomic across
// crashes. A single RWMutex serialises in-process access.
package store

import "time"

// StateVersion is the schema version written into state.json. Bumped when
// fields change shape, never for additive changes.
const StateVersion = 1

// State is the root document persisted to disk.
type State struct {
	Version    int            `json:"version"`
	ProxyToken string         `json:"proxyToken"`
	Settings   Settings       `json:"settings"`
	Accounts   []Account      `json:"accounts"`
	TrafficLog []TrafficEntry `json:"trafficLog"`
}

// Settings holds runtime knobs the user can tweak through the dashboard.
// All durations are in seconds, all credits in USD.
type Settings struct {
	Routing                   string  `json:"routing"`
	MinCreditsUSD             float64 `json:"minCreditsUsd"`
	MaxErrorRate5min          float64 `json:"maxErrorRate5min"`
	CreditPollSec             int     `json:"creditPollSec"`
	TrafficLogMax             int     `json:"trafficLogMax"`
	MergeReasoningIntoContent bool    `json:"mergeReasoningIntoContent"`
}

// DefaultSettings matches plan.md §4 defaults. Used when a state file is
// freshly created or a previously-persisted Settings has zero values for
// every field (likely an older schema).
func DefaultSettings() Settings {
	return Settings{
		Routing:          "affinity",
		MinCreditsUSD:    0.5,
		MaxErrorRate5min: 0.20,
		CreditPollSec:    60,
		TrafficLogMax:    500,
	}
}

// Account is one Command Code Go-tier user whose apikey the proxy is
// allowed to route requests through.
type Account struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	Email              string    `json:"email"`
	UserName           string    `json:"userName"`
	APIKey             string    `json:"apiKey"`
	AddedAt            time.Time `json:"addedAt"`
	LastUsedAt         time.Time `json:"lastUsedAt,omitempty"`
	Paused             bool      `json:"paused"`
	LastKnownCredits   float64   `json:"lastKnownCredits"`
	LastKnownCreditsAt time.Time `json:"lastKnownCreditsAt,omitempty"`
}

// TrafficEntry is one row in the rolling traffic log. Trimmed to
// Settings.TrafficLogMax by Store.AppendTrafficLog.
type TrafficEntry struct {
	TS               time.Time `json:"ts"`
	AccountID        string    `json:"accountId"`
	Protocol         string    `json:"protocol"` // "openai" | "anthropic"
	Model            string    `json:"model"`
	Status           int       `json:"status"`
	InputTokens      int       `json:"inputTokens"`
	CacheReadTokens  int       `json:"cacheReadTokens"`
	CacheWriteTokens int       `json:"cacheWriteTokens"`
	OutputTokens     int       `json:"outputTokens"`
	CostUSD          float64   `json:"costUsd"`
	DurationMS       int       `json:"durationMs"`
	Retried          bool      `json:"retried"`
	ErrorCode        string    `json:"errorCode,omitempty"`
}

// Clone returns a deep copy safe to read outside the store lock.
func (st State) Clone() State {
	out := st
	if st.Accounts != nil {
		out.Accounts = append([]Account(nil), st.Accounts...)
	}
	if st.TrafficLog != nil {
		out.TrafficLog = append([]TrafficEntry(nil), st.TrafficLog...)
	}
	return out
}
