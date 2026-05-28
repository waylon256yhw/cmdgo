package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/waylon256yhw/cmdgo/internal/cc"
)

// eventStream is the minimal surface the OpenAI / Anthropic stream
// translators need. cc.Scanner satisfies it directly; prefixedStream
// wraps a pre-consumed first event ahead of a real scanner so the
// retry loop can peek the first frame for retry decisions without
// the adapter ever knowing.
type eventStream interface {
	Next() (*cc.StreamEvent, error)
}

// prefixedStream yields `first` once (if non-nil) and then delegates
// to `inner`.
type prefixedStream struct {
	first     *cc.StreamEvent
	delivered bool
	inner     *cc.Scanner
}

func newPrefixedStream(first *cc.StreamEvent, inner *cc.Scanner) *prefixedStream {
	return &prefixedStream{first: first, inner: inner}
}

func (p *prefixedStream) Next() (*cc.StreamEvent, error) {
	if !p.delivered {
		p.delivered = true
		if p.first != nil {
			return p.first, nil
		}
	}
	return p.inner.Next()
}

// ContentBlock is one piece of an assembled assistant response. Both
// the OpenAI and Anthropic non-stream serialisers consume slices of
// these. Text and thinking deltas of the same kind are merged into a
// single block by AccumulateStream; tool calls always stand alone.
type ContentBlock struct {
	Type          string          // "text" | "thinking" | "tool_use"
	Text          string          // populated for text and thinking
	ToolID        string          // populated for tool_use
	ToolName      string          // populated for tool_use
	ToolInputJSON json.RawMessage // populated for tool_use; "{}" when empty
}

// AccumulatedResponse is the protocol-neutral snapshot of a fully
// drained CC stream. It is the single intermediate representation
// used by the non-streaming OpenAI and Anthropic handlers.
type AccumulatedResponse struct {
	Blocks         []ContentBlock
	Summary        streamSummary
	FinishReceived bool // true iff CC emitted a finish event before EOF
}

// AccumulateStream drains sc into an AccumulatedResponse. Returns
// (acc, nil) on a clean stream (with or without a finish frame —
// FinishReceived signals which). Returns an error for scanner
// failures and for mid-stream upstream error frames; in those cases
// the partial accumulator is still returned so callers can decide
// whether to surface what was collected.
func AccumulateStream(sc eventStream) (*AccumulatedResponse, error) {
	acc := &AccumulatedResponse{}
	// openMergeable is the type ("text"/"thinking") of the last
	// appended block iff it is still eligible to absorb subsequent
	// deltas of the same kind. Cleared by reasoning-end, by tool-call,
	// and by any type switch.
	openMergeable := ""

	for {
		ev, err := sc.Next()
		if ev != nil {
			switch ev.Type {
			case "start":
				// nothing to accumulate
			case "text-delta":
				var pl struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				if openMergeable == "text" {
					acc.Blocks[len(acc.Blocks)-1].Text += pl.Text
				} else {
					acc.Blocks = append(acc.Blocks, ContentBlock{Type: "text", Text: pl.Text})
					openMergeable = "text"
				}
			case "reasoning-delta":
				var pl struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				if openMergeable == "thinking" {
					acc.Blocks[len(acc.Blocks)-1].Text += pl.Text
				} else {
					acc.Blocks = append(acc.Blocks, ContentBlock{Type: "thinking", Text: pl.Text})
					openMergeable = "thinking"
				}
			case "reasoning-end":
				// Force a fresh block on the next thinking-delta.
				openMergeable = ""
			case "tool-call":
				var pl struct {
					ToolCallID string          `json:"toolCallId"`
					ToolName   string          `json:"toolName"`
					Input      json.RawMessage `json:"input"`
					Args       json.RawMessage `json:"args"`
					Arguments  json.RawMessage `json:"arguments"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				inputRaw := firstNonEmptyJSON(pl.Input, pl.Args, pl.Arguments)
				if len(inputRaw) == 0 {
					inputRaw = json.RawMessage("{}")
				}
				acc.Blocks = append(acc.Blocks, ContentBlock{
					Type:          "tool_use",
					ToolID:        pl.ToolCallID,
					ToolName:      pl.ToolName,
					ToolInputJSON: inputRaw,
				})
				openMergeable = ""
			case "finish":
				acc.Summary = parseCCFinish(ev.Raw)
				acc.FinishReceived = true
			case "error":
				return acc, fmt.Errorf("upstream emitted error event mid-stream: %s", truncate(ev.Raw, 200))
			}
		}
		if errors.Is(err, io.EOF) {
			return acc, nil
		}
		if err != nil {
			return acc, fmt.Errorf("scan upstream sse: %w", err)
		}
	}
}

// parseCCFinish decodes a CC "finish" frame into a streamSummary. It
// is the single source of truth for CC's finish schema — both the
// streaming and non-streaming paths route through it so the per-
// stream token / cost accounting stays consistent.
func parseCCFinish(raw json.RawMessage) streamSummary {
	var pl struct {
		FinishReason string `json:"finishReason"`
		TotalUsage   struct {
			InputTokens       int `json:"inputTokens"`
			OutputTokens      int `json:"outputTokens"`
			TotalTokens       int `json:"totalTokens"`
			InputTokenDetails struct {
				CacheReadTokens  int `json:"cacheReadTokens"`
				CacheWriteTokens int `json:"cacheWriteTokens"`
				NoCacheTokens    int `json:"noCacheTokens"`
			} `json:"inputTokenDetails"`
			OutputTokenDetails struct {
				TextTokens      int `json:"textTokens"`
				ReasoningTokens int `json:"reasoningTokens"`
			} `json:"outputTokenDetails"`
		} `json:"totalUsage"`
	}
	_ = json.Unmarshal(raw, &pl)
	return streamSummary{
		InputTokens:      pl.TotalUsage.InputTokens,
		OutputTokens:     pl.TotalUsage.OutputTokens,
		TotalTokens:      pl.TotalUsage.TotalTokens,
		CacheReadTokens:  pl.TotalUsage.InputTokenDetails.CacheReadTokens,
		CacheWriteTokens: pl.TotalUsage.InputTokenDetails.CacheWriteTokens,
		ReasoningTokens:  pl.TotalUsage.OutputTokenDetails.ReasoningTokens,
		FinishReason:     pl.FinishReason,
		Recorded:         true,
	}
}
