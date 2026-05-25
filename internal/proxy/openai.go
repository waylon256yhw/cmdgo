package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// OpenAIHandler serves POST /v1/chat/completions. It is wired in
// main.go with a bearer-protected route. Runner is the shared
// pool+retry executor; if nil, ServeHTTP falls back to the
// placeholder pick so the handler stays usable in unit tests that do
// not exercise the retry path.
type OpenAIHandler struct {
	Store    *store.Store
	CC       *cc.Client
	Logger   *slog.Logger
	Runner   *Runner
	Recorder TrafficRecorder
}

// openaiRequest is what we actually look at from incoming JSON. Fields
// like temperature / top_p / tool_choice / response_format are
// intentionally ignored — we accept them so SDKs don't break, but they
// have no upstream equivalent on the Go tier.
type openaiRequest struct {
	Model       string          `json:"model"`
	Messages    []openaiMessage `json:"messages"`
	Tools       []openaiTool    `json:"tools,omitempty"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	MaxComplTok int             `json:"max_completion_tokens,omitempty"`
	Stream      *bool           `json:"stream,omitempty"`
	StreamOpts  *streamOptions  `json:"stream_options,omitempty"`
	User        string          `json:"user,omitempty"`
	// Accepted but ignored (no Go-tier equivalent). Captured so we
	// can advertise them in x-cmdgo-ignored.
	Temperature    json.RawMessage `json:"temperature,omitempty"`
	TopP           json.RawMessage `json:"top_p,omitempty"`
	ToolChoice     json.RawMessage `json:"tool_choice,omitempty"`
	ResponseFormat json.RawMessage `json:"response_format,omitempty"`
	N              json.RawMessage `json:"n,omitempty"`
	Stop           json.RawMessage `json:"stop,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type openaiMessage struct {
	Role       string           `json:"role"`
	Content    json.RawMessage  `json:"content"`
	Name       string           `json:"name,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openaiToolCall `json:"tool_calls,omitempty"`
}

type openaiToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openaiTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
	} `json:"function"`
}

// outbound OpenAI SSE chunk shape.
type openaiChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []openaiChoice `json:"choices"`
	Usage   *openaiUsage   `json:"usage,omitempty"`
}

type openaiChoice struct {
	Index        int         `json:"index"`
	Delta        openaiDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type openaiDelta struct {
	Role             string                `json:"role,omitempty"`
	Content          string                `json:"content,omitempty"`
	ReasoningContent string                `json:"reasoning_content,omitempty"`
	ToolCalls        []openaiToolCallDelta `json:"tool_calls,omitempty"`
}

type openaiToolCallDelta struct {
	Index    int                    `json:"index"`
	ID       string                 `json:"id,omitempty"`
	Type     string                 `json:"type,omitempty"`
	Function *openaiToolCallFnDelta `json:"function,omitempty"`
}

type openaiToolCallFnDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

type openaiUsage struct {
	PromptTokens            int                  `json:"prompt_tokens"`
	CompletionTokens        int                  `json:"completion_tokens"`
	TotalTokens             int                  `json:"total_tokens"`
	PromptTokensDetails     *openaiTokensDetails `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *openaiTokensDetails `json:"completion_tokens_details,omitempty"`
}

type openaiTokensDetails struct {
	CachedTokens    int `json:"cached_tokens,omitempty"`
	ReasoningTokens int `json:"reasoning_tokens,omitempty"`
}

func (h *OpenAIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req openaiRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 16<<20)) // 16 MiB
	if err := dec.Decode(&req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "failed to parse request body: "+err.Error())
		return
	}

	if req.Model == "" {
		req.Model = cc.DefaultGoModel
	}
	if !cc.IsGoModel(req.Model) {
		writeOpenAIError(w, http.StatusBadRequest, "model_not_in_plan_go",
			"model not in Go-tier whitelist: "+req.Model)
		return
	}

	maxTok := req.MaxTokens
	if maxTok == 0 {
		maxTok = req.MaxComplTok
	}

	system, messages, err := convertOpenAIMessages(req.Messages)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	tools, err := convertOpenAITools(req.Tools)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	clientToken := extractBearer(r)
	canon := &Canonical{
		Model:       req.Model,
		Messages:    messages,
		Tools:       tools,
		System:      system,
		MaxTokens:   maxTok,
		ClientToken: clientToken,
		Protocol:    "openai",
	}

	ignored := collectIgnoredOpenAIFields(&req)
	if len(ignored) > 0 {
		w.Header().Set("x-cmdgo-ignored", strings.Join(ignored, ","))
	}
	w.Header().Set("x-cmdgo-protocol", "openai")
	w.Header().Set("x-cmdgo-model", req.Model)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	started := time.Now()
	rec := h.recorder()

	attempt, accID, err := openAttempt(ctx, h.Runner, h.Store, h.CC, h.Logger, canon)
	if err != nil {
		mapCCErrorToOpenAI(w, err)
		rec.RecordTraffic(store.TrafficEntry{
			AccountID:  accID,
			Protocol:   "openai",
			Model:      req.Model,
			Status:     httpStatusFromErr(err, http.StatusBadGateway),
			DurationMS: int(time.Since(started).Milliseconds()),
			ErrorCode:  errorCodeFromErr(err),
		})
		return
	}
	defer attempt.Response.Body.Close()
	w.Header().Set("x-cmdgo-account-id", accID)
	if attempt.Retried {
		w.Header().Set("x-cmdgo-retried", "true")
	}

	sse, err := NewSSEWriter(w)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Mark success the moment we have a healthy upstream attempt with
	// at least one event ready. The apikey is proven valid; the
	// account is proven reachable. Anything that goes wrong from
	// here on out — CC emitting a mid-stream `error` frame with
	// isRetryable:true, the upstream TCP dying, the client closing
	// its end — is NOT a signal that this account is unhealthy, and
	// must not feed Pool.MarkError. Single-account deployments
	// otherwise lose the only account to the rolling-stats threshold
	// the first time CC has a hiccup.
	//
	// Pre-flush failures (bad apikey, plan-locked model, retry budget
	// exhausted) are handled at the retry.Runner level — those DO
	// MarkError because they're the legitimate "this apikey is
	// broken" signal.
	if h.Runner != nil {
		h.Runner.Pool.MarkSuccess(accID)
	}

	id := newCompletionID()
	created := time.Now().Unix()
	stream := newPrefixedStream(attempt.FirstEvent, attempt.Scanner)
	var summary streamSummary
	streamErr := streamCCToOpenAI(h.Logger, stream, sse, id, req.Model, created, &summary)
	status := http.StatusOK
	errCode := ""
	switch {
	case streamErr == nil:
		// nothing more to do
	case clientGone(r, streamErr):
		h.Logger.Info("openai client disconnected mid-stream", "account", accID)
	default:
		h.Logger.Warn("openai stream error", "err", streamErr, "account", accID)
		status = http.StatusBadGateway
		errCode = "stream_error"
	}
	_ = h.Store.TouchAccountLastUsed(accID)
	rec.RecordTraffic(store.TrafficEntry{
		AccountID:        accID,
		Protocol:         "openai",
		Model:            req.Model,
		Status:           status,
		InputTokens:      summary.InputTokens,
		CacheReadTokens:  summary.CacheReadTokens,
		CacheWriteTokens: summary.CacheWriteTokens,
		OutputTokens:     summary.OutputTokens,
		CostUSD:          computeCostUSD(req.Model, summary),
		DurationMS:       int(time.Since(started).Milliseconds()),
		Retried:          attempt.Retried,
		ErrorCode:        errCode,
	})
}

// recorder returns the configured TrafficRecorder or a no-op when
// unset (tests).
func (h *OpenAIHandler) recorder() TrafficRecorder {
	if h.Recorder == nil {
		return nopRecorder{}
	}
	return h.Recorder
}

func streamCCToOpenAI(logger *slog.Logger, sc eventStream, sse *SSEWriter, id, model string, created int64, summary *streamSummary) error {
	sentRole := false
	var toolIndex int

	mkChunk := func(delta openaiDelta, finish *string, usage *openaiUsage) openaiChunk {
		return openaiChunk{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   model,
			Choices: []openaiChoice{{Index: 0, Delta: delta, FinishReason: finish}},
			Usage:   usage,
		}
	}

	for {
		ev, err := sc.Next()
		if ev != nil {
			switch ev.Type {
			case "start":
				if !sentRole {
					if werr := sse.WriteJSON(mkChunk(openaiDelta{Role: "assistant"}, nil, nil)); werr != nil {
						return werr
					}
					sentRole = true
				}
			case "text-delta":
				if !sentRole {
					if werr := sse.WriteJSON(mkChunk(openaiDelta{Role: "assistant"}, nil, nil)); werr != nil {
						return werr
					}
					sentRole = true
				}
				var pl struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				if werr := sse.WriteJSON(mkChunk(openaiDelta{Content: pl.Text}, nil, nil)); werr != nil {
					return werr
				}
			case "reasoning-delta":
				if !sentRole {
					if werr := sse.WriteJSON(mkChunk(openaiDelta{Role: "assistant"}, nil, nil)); werr != nil {
						return werr
					}
					sentRole = true
				}
				var pl struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				if werr := sse.WriteJSON(mkChunk(openaiDelta{ReasoningContent: pl.Text}, nil, nil)); werr != nil {
					return werr
				}
			case "reasoning-end":
				// OpenAI has no distinct reasoning-end frame; clients tell
				// from the next event type.
			case "tool-call":
				if !sentRole {
					if werr := sse.WriteJSON(mkChunk(openaiDelta{Role: "assistant"}, nil, nil)); werr != nil {
						return werr
					}
					sentRole = true
				}
				var pl struct {
					ToolCallID string          `json:"toolCallId"`
					ToolName   string          `json:"toolName"`
					Input      json.RawMessage `json:"input"`
					Args       json.RawMessage `json:"args"`
					Arguments  json.RawMessage `json:"arguments"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				rawArgs := firstNonEmptyJSON(pl.Input, pl.Args, pl.Arguments)
				argsStr := string(rawArgs)
				if argsStr == "" || argsStr == "null" {
					argsStr = "{}"
				}
				delta := openaiDelta{
					ToolCalls: []openaiToolCallDelta{{
						Index:    toolIndex,
						ID:       pl.ToolCallID,
						Type:     "function",
						Function: &openaiToolCallFnDelta{Name: pl.ToolName, Arguments: argsStr},
					}},
				}
				if werr := sse.WriteJSON(mkChunk(delta, nil, nil)); werr != nil {
					return werr
				}
				toolIndex++
			case "finish":
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
				_ = json.Unmarshal(ev.Raw, &pl)
				fr := normalizeFinishReason(pl.FinishReason)
				if summary != nil {
					summary.InputTokens = pl.TotalUsage.InputTokens
					summary.OutputTokens = pl.TotalUsage.OutputTokens
					summary.CacheReadTokens = pl.TotalUsage.InputTokenDetails.CacheReadTokens
					summary.CacheWriteTokens = pl.TotalUsage.InputTokenDetails.CacheWriteTokens
					summary.ReasoningTokens = pl.TotalUsage.OutputTokenDetails.ReasoningTokens
					summary.FinishReason = pl.FinishReason
					summary.Recorded = true
				}
				usage := &openaiUsage{
					PromptTokens:     pl.TotalUsage.InputTokens,
					CompletionTokens: pl.TotalUsage.OutputTokens,
					TotalTokens:      pl.TotalUsage.TotalTokens,
				}
				if pl.TotalUsage.InputTokenDetails.CacheReadTokens > 0 {
					usage.PromptTokensDetails = &openaiTokensDetails{
						CachedTokens: pl.TotalUsage.InputTokenDetails.CacheReadTokens,
					}
				}
				if pl.TotalUsage.OutputTokenDetails.ReasoningTokens > 0 {
					usage.CompletionTokensDetails = &openaiTokensDetails{
						ReasoningTokens: pl.TotalUsage.OutputTokenDetails.ReasoningTokens,
					}
				}
				if werr := sse.WriteJSON(mkChunk(openaiDelta{}, &fr, usage)); werr != nil {
					return werr
				}
			case "error":
				logger.Warn("upstream error event", "raw", string(ev.Raw))
				return errors.New("upstream emitted error event mid-stream")
			}
		}
		if errors.Is(err, io.EOF) {
			return sse.WriteRaw([]byte("[DONE]"))
		}
		if err != nil {
			return fmt.Errorf("scan upstream sse: %w", err)
		}
	}
}

// convertOpenAIMessages turns the inbound OpenAI message list into the
// content-block shape `params.messages` expects. System messages get
// hoisted into the params.system field.
func convertOpenAIMessages(in []openaiMessage) (system json.RawMessage, out []json.RawMessage, err error) {
	var sysParts []string
	for i, m := range in {
		switch m.Role {
		case "system":
			s, ok := jsonStringOrConcat(m.Content)
			if !ok {
				return nil, nil, fmt.Errorf("messages[%d]: system content must be string", i)
			}
			sysParts = append(sysParts, s)
		case "user":
			// Pass content through: CC accepts both string and content-block array.
			obj := map[string]any{"role": "user", "content": jsonOrNull(m.Content)}
			raw, _ := json.Marshal(obj)
			out = append(out, raw)
		case "assistant":
			blocks := make([]any, 0, 1+len(m.ToolCalls))
			if len(m.Content) > 0 && string(m.Content) != "null" {
				if s, ok := jsonStringOrConcat(m.Content); ok && s != "" {
					blocks = append(blocks, map[string]any{"type": "text", "text": s})
				}
			}
			for _, tc := range m.ToolCalls {
				var input any
				if tc.Function.Arguments != "" {
					if jerr := json.Unmarshal([]byte(tc.Function.Arguments), &input); jerr != nil {
						input = tc.Function.Arguments
					}
				}
				blocks = append(blocks, map[string]any{
					"type":       "tool-call",
					"toolCallId": tc.ID,
					"toolName":   tc.Function.Name,
					"input":      input,
				})
			}
			obj := map[string]any{"role": "assistant", "content": blocks}
			raw, _ := json.Marshal(obj)
			out = append(out, raw)
		case "tool":
			s, _ := jsonStringOrConcat(m.Content)
			block := map[string]any{
				"type":       "tool-result",
				"toolCallId": m.ToolCallID,
				"toolName":   m.Name,
				"output":     map[string]any{"type": "text", "value": s},
			}
			obj := map[string]any{"role": "tool", "content": []any{block}}
			raw, _ := json.Marshal(obj)
			out = append(out, raw)
		case "developer":
			// OpenAI's reasoning models accept role=developer like
			// system. Treat them the same way.
			s, _ := jsonStringOrConcat(m.Content)
			sysParts = append(sysParts, s)
		default:
			return nil, nil, fmt.Errorf("messages[%d]: unsupported role %q", i, m.Role)
		}
	}
	if len(sysParts) > 0 {
		raw, _ := json.Marshal(strings.Join(sysParts, "\n\n"))
		system = raw
	}
	return system, out, nil
}

func convertOpenAITools(in []openaiTool) ([]json.RawMessage, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]json.RawMessage, 0, len(in))
	for i, t := range in {
		if t.Type != "" && t.Type != "function" {
			return nil, fmt.Errorf("tools[%d]: unsupported type %q (only function tools are supported)", i, t.Type)
		}
		ccTool := map[string]any{
			"type":         "function",
			"name":         t.Function.Name,
			"description":  t.Function.Description,
			"input_schema": json.RawMessage(t.Function.Parameters),
		}
		if len(t.Function.Parameters) == 0 {
			ccTool["input_schema"] = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		raw, _ := json.Marshal(ccTool)
		out = append(out, raw)
	}
	return out, nil
}

// jsonStringOrConcat unwraps a raw OpenAI content field. Returns the
// concatenated text if content is a string or an array of {type:text}
// blocks; image_url / audio parts are silently dropped per plan §11.
func jsonStringOrConcat(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, true
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err == nil {
		var b strings.Builder
		for _, p := range parts {
			if p.Type == "text" || p.Type == "" {
				b.WriteString(p.Text)
			}
		}
		return b.String(), true
	}
	return "", false
}

// jsonOrNull returns a RawMessage of "null" for empty input so JSON
// marshaling never produces nil values that look unset.
func jsonOrNull(raw json.RawMessage) any {
	if len(raw) == 0 {
		return ""
	}
	return raw
}

func firstNonEmptyJSON(candidates ...json.RawMessage) json.RawMessage {
	for _, c := range candidates {
		if len(c) > 0 && string(c) != "null" {
			return c
		}
	}
	return nil
}

func normalizeFinishReason(s string) string {
	switch s {
	case "tool-calls", "tool_calls":
		return "tool_calls"
	case "length", "max_tokens", "max-tokens", "max_output_tokens":
		return "length"
	case "stop", "":
		return "stop"
	default:
		return "stop"
	}
}

func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    code,
			"code":    code,
		},
	})
}

func mapCCErrorToOpenAI(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNoHealthyAccount) || errors.Is(err, poolErrNoHealthyAccount) {
		writeOpenAIError(w, http.StatusServiceUnavailable, "no_account", err.Error())
		return
	}
	var ae *cc.APIError
	if errors.As(err, &ae) {
		status := ae.HTTPStatus
		if status == 0 {
			status = http.StatusBadGateway
		}
		writeOpenAIError(w, status, strings.ToLower(ae.Body.Code), ae.Body.Message)
		return
	}
	writeOpenAIError(w, http.StatusBadGateway, "upstream_error", err.Error())
}

func extractBearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return h[len("Bearer "):]
	}
	return ""
}

func collectIgnoredOpenAIFields(req *openaiRequest) []string {
	var out []string
	add := func(name string, raw json.RawMessage) {
		if len(raw) > 0 && string(raw) != "null" {
			out = append(out, name)
		}
	}
	add("temperature", req.Temperature)
	add("top_p", req.TopP)
	add("tool_choice", req.ToolChoice)
	add("response_format", req.ResponseFormat)
	add("n", req.N)
	add("stop", req.Stop)
	return out
}

func newCompletionID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "chatcmpl-" + hex.EncodeToString(b[:])
}

// httpStatusFromErr extracts the HTTP status carried by *cc.APIError;
// falls back to fallback for anything else (network failures, retry
// budget exhausted, etc.).
func httpStatusFromErr(err error, fallback int) int {
	var ae *cc.APIError
	if errors.As(err, &ae) && ae.HTTPStatus > 0 {
		return ae.HTTPStatus
	}
	if errors.Is(err, ErrNoHealthyAccount) || errors.Is(err, poolErrNoHealthyAccount) {
		return http.StatusServiceUnavailable
	}
	return fallback
}

// errorCodeFromErr returns a short machine-friendly code suitable for
// the TrafficEntry.ErrorCode column.
func errorCodeFromErr(err error) string {
	if err == nil {
		return ""
	}
	var ae *cc.APIError
	if errors.As(err, &ae) {
		if ae.Body.Code != "" {
			return strings.ToLower(ae.Body.Code)
		}
		return "upstream_error"
	}
	if errors.Is(err, ErrNoHealthyAccount) || errors.Is(err, poolErrNoHealthyAccount) {
		return "no_account"
	}
	return "internal_error"
}
