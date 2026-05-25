// Package cc holds Command Code client-side concerns: API types, upstream
// client, and the Go-tier model whitelist.
package cc

// Model holds metadata for a Command Code model usable on the Go tier.
type Model struct {
	ID            string
	Name          string
	Reasoning     bool
	ContextWindow int
	MaxTokens     int
	Pricing       Pricing
	PingCostUSD   float64
	Notes         string
}

// Pricing is per 1M tokens, in USD. These are the discounted prices CC
// actually bills on the Go tier (e.g. DeepSeek V4 Pro is 75% off list).
// CacheWritePerM is zero when the upstream does not support cache writes.
type Pricing struct {
	InputPerM      float64
	OutputPerM     float64
	CacheReadPerM  float64
	CacheWritePerM float64
}

// GoModels is the canonical whitelist of CC models that work on the Go tier
// ($1/mo, $10 credits). Premium models (Anthropic/OpenAI/Gemini) are
// server-side plan-locked and return 403 MODEL_NOT_IN_PLAN.
//
// Sorted by observed PingCostUSD (cheapest first), which roughly corresponds
// to "quality per credit" once cache hits kick in.
//
// Pricing sourced from https://commandcode.ai/docs/resources/pricing-limits
// and confirmed empirically against /alpha/usage/summary on 2026-05-25.
var GoModels = []Model{
	{
		ID:            "stepfun/Step-3.5-Flash",
		Name:          "Step 3.5 Flash",
		Reasoning:     true,
		ContextWindow: 200_000,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 0.10, OutputPerM: 0.30, CacheReadPerM: 0.02},
		PingCostUSD:   0.0007,
		Notes:         "Cheapest. Sparse-MoE agentic reasoning.",
	},
	{
		ID:            "deepseek/deepseek-v4-pro",
		Name:          "DeepSeek V4 Pro",
		Reasoning:     true,
		ContextWindow: 1_000_000,
		MaxTokens:     384_000,
		Pricing:       Pricing{InputPerM: 0.435, OutputPerM: 0.87, CacheReadPerM: 0.003625},
		PingCostUSD:   0.0001,
		Notes:         "75% off permanent => 4x credit value on Go. Best price after cache hit.",
	},
	{
		ID:            "deepseek/deepseek-v4-flash",
		Name:          "DeepSeek V4 Flash",
		Reasoning:     true,
		ContextWindow: 1_000_000,
		MaxTokens:     384_000,
		Pricing:       Pricing{InputPerM: 0.14, OutputPerM: 0.28, CacheReadPerM: 0.01},
		PingCostUSD:   0.0001,
		Notes:         "Intermittent 503 (isRetryable=true). Retry once on stream error.",
	},
	{
		ID:            "MiniMaxAI/MiniMax-M2.5",
		Name:          "MiniMax M2.5",
		Reasoning:     true,
		ContextWindow: 1_048_576,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 0.27, OutputPerM: 0.95, CacheReadPerM: 0.03},
		PingCostUSD:   0.0022,
		Notes:         "No prompt cache (upstream limitation).",
	},
	{
		ID:            "MiniMaxAI/MiniMax-M2.7",
		Name:          "MiniMax M2.7",
		Reasoning:     true,
		ContextWindow: 1_048_576,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 0.30, OutputPerM: 1.20, CacheReadPerM: 0.06},
		PingCostUSD:   0.0022,
		Notes:         "No prompt cache (upstream limitation).",
	},
	{
		ID:            "Qwen/Qwen3.6-Plus",
		Name:          "Qwen 3.6 Plus",
		Reasoning:     true,
		ContextWindow: 1_000_000,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 0.50, OutputPerM: 3.00, CacheReadPerM: 0.10},
		PingCostUSD:   0.0039,
	},
	{
		ID:            "moonshotai/Kimi-K2.5",
		Name:          "Kimi K2.5",
		Reasoning:     true,
		ContextWindow: 262_144,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 0.60, OutputPerM: 3.00, CacheReadPerM: 0.10},
		PingCostUSD:   0.0045,
	},
	{
		ID:            "moonshotai/Kimi-K2.6",
		Name:          "Kimi K2.6",
		Reasoning:     true,
		ContextWindow: 262_144,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 0.95, OutputPerM: 4.00, CacheReadPerM: 0.16},
		PingCostUSD:   0.0071,
	},
	{
		ID:            "zai-org/GLM-5",
		Name:          "GLM-5",
		Reasoning:     true,
		ContextWindow: 200_000,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 1.00, OutputPerM: 3.20, CacheReadPerM: 0.20},
		PingCostUSD:   0.0074,
	},
	{
		ID:            "zai-org/GLM-5.1",
		Name:          "GLM-5.1",
		Reasoning:     true,
		ContextWindow: 200_000,
		MaxTokens:     131_072,
		Pricing:       Pricing{InputPerM: 1.40, OutputPerM: 4.40, CacheReadPerM: 0.26},
		PingCostUSD:   0.0103,
	},
	{
		ID:            "Qwen/Qwen3.7-Max",
		Name:          "Qwen 3.7 Max",
		Reasoning:     true,
		ContextWindow: 1_000_000,
		MaxTokens:     131_072,
		Pricing: Pricing{
			InputPerM:      1.25,
			OutputPerM:     3.75,
			CacheReadPerM:  0.25,
			CacheWritePerM: 1.56,
		},
		PingCostUSD: 0.0121,
		Notes:       "50% off through 2026-06-22; list price doubles after deal ends.",
	},
	{
		ID:            "Qwen/Qwen3.6-Max-Preview",
		Name:          "Qwen 3.6 Max Preview",
		Reasoning:     true,
		ContextWindow: 1_000_000,
		MaxTokens:     131_072,
		Pricing: Pricing{
			InputPerM:      1.30,
			OutputPerM:     7.80,
			CacheReadPerM:  0.26,
			CacheWritePerM: 1.63,
		},
		PingCostUSD: 0.0126,
	},
}

var goModelByID = func() map[string]*Model {
	m := make(map[string]*Model, len(GoModels))
	for i := range GoModels {
		m[GoModels[i].ID] = &GoModels[i]
	}
	return m
}()

// IsGoModel reports whether the given model ID is in the Go-tier whitelist.
func IsGoModel(id string) bool {
	_, ok := goModelByID[id]
	return ok
}

// LookupGoModel returns model metadata for the given ID, or nil if not whitelisted.
func LookupGoModel(id string) *Model {
	return goModelByID[id]
}

// DefaultGoModel is the model the proxy routes to when a client does not
// specify one. DeepSeek V4 Pro is chosen for its 4x usage multiplier on Go
// and aggressive auto-caching.
const DefaultGoModel = "deepseek/deepseek-v4-pro"
