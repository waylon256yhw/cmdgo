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

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// AnthropicHandler serves POST /v1/messages with the Anthropic SSE
// protocol (message_start / content_block_* / message_delta /
// message_stop). It shares the canonical runner with the OpenAI
// adapter — only the wire format differs.
type AnthropicHandler struct {
	Store  *store.Store
	CC     *cc.Client
	Logger *slog.Logger
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	Messages  []anthropicMessage `json:"messages"`
	System    json.RawMessage    `json:"system,omitempty"`
	Tools     []anthropicTool    `json:"tools,omitempty"`
	MaxTokens int                `json:"max_tokens"`
	Stream    *bool              `json:"stream,omitempty"`
	Metadata  *anthropicMetadata `json:"metadata,omitempty"`
	// Accepted but ignored.
	Temperature   json.RawMessage `json:"temperature,omitempty"`
	TopP          json.RawMessage `json:"top_p,omitempty"`
	TopK          json.RawMessage `json:"top_k,omitempty"`
	StopSequences json.RawMessage `json:"stop_sequences,omitempty"`
	ToolChoice    json.RawMessage `json:"tool_choice,omitempty"`
	Thinking      json.RawMessage `json:"thinking,omitempty"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

type anthropicMetadata struct {
	UserID string `json:"user_id,omitempty"`
}

func (h *AnthropicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req anthropicRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&req); err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "failed to parse request body: "+err.Error())
		return
	}

	if req.Model == "" {
		req.Model = cc.DefaultGoModel
	}
	if !cc.IsGoModel(req.Model) {
		writeAnthropicError(w, http.StatusBadRequest, "model_not_in_plan_go",
			"model not in Go-tier whitelist: "+req.Model)
		return
	}
	if req.MaxTokens <= 0 {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "max_tokens must be > 0")
		return
	}

	system := normalizeAnthropicSystem(req.System)
	messages, err := convertAnthropicMessages(req.Messages)
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	tools := convertAnthropicTools(req.Tools)

	clientToken := extractBearer(r)
	// Per plan §11, metadata.user_id wins as the affinity key over the
	// bearer when present — it lets multiple end users of one shared
	// dashboard get sticky routing without sharing an account.
	affinity := clientToken
	if req.Metadata != nil && req.Metadata.UserID != "" {
		affinity = req.Metadata.UserID
	}

	canon := &Canonical{
		Model:       req.Model,
		Messages:    messages,
		Tools:       tools,
		System:      system,
		MaxTokens:   req.MaxTokens,
		ClientToken: affinity,
		Protocol:    "anthropic",
	}

	acc, err := pickAccount(h.Store)
	if err != nil {
		writeAnthropicError(w, http.StatusServiceUnavailable, "no_account", err.Error())
		return
	}

	body := BuildCCBody(canon)
	sid := SessionID(affinity, canon.Model, MessagesPrefix(canon.Messages))

	ignored := collectIgnoredAnthropicFields(&req)
	if len(ignored) > 0 {
		w.Header().Set("x-cmdgo-ignored", strings.Join(ignored, ","))
	}
	w.Header().Set("x-cmdgo-account-id", acc.ID)
	w.Header().Set("x-cmdgo-protocol", "anthropic")
	w.Header().Set("x-cmdgo-model", req.Model)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	resp, err := h.CC.Generate(ctx, cc.GenerateOpts{
		APIKey:    acc.APIKey,
		SessionID: sid,
		Body:      body,
	})
	if err != nil {
		mapCCErrorToAnthropic(w, err)
		return
	}
	defer resp.Body.Close()

	sse, err := NewSSEWriter(w)
	if err != nil {
		writeAnthropicError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	if err := streamCCToAnthropic(h.Logger, cc.NewScanner(resp.Body), sse, newAnthropicMessageID(), req.Model); err != nil {
		h.Logger.Warn("anthropic stream error", "err", err, "account", acc.ID)
	}
}

// anthState tracks which content block is currently open so we can emit
// content_block_start/stop on the boundaries between text, thinking,
// and tool_use spans.
type anthState struct {
	sentMessageStart bool
	currentType      string // "" | "text" | "thinking" | "tool_use"
	index            int
}

func (s *anthState) closeCurrent(sse *SSEWriter) error {
	if s.currentType == "" {
		return nil
	}
	if err := sse.WriteEvent("content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": s.index,
	}); err != nil {
		return err
	}
	s.index++
	s.currentType = ""
	return nil
}

func (s *anthState) openBlock(sse *SSEWriter, typ string, init map[string]any) error {
	if s.currentType != "" {
		if err := s.closeCurrent(sse); err != nil {
			return err
		}
	}
	s.currentType = typ
	return sse.WriteEvent("content_block_start", map[string]any{
		"type":          "content_block_start",
		"index":         s.index,
		"content_block": init,
	})
}

func streamCCToAnthropic(logger *slog.Logger, sc *cc.Scanner, sse *SSEWriter, msgID, model string) error {
	st := &anthState{}

	emitMessageStart := func() error {
		if st.sentMessageStart {
			return nil
		}
		st.sentMessageStart = true
		return sse.WriteEvent("message_start", map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"id":            msgID,
				"type":          "message",
				"role":          "assistant",
				"content":       []any{},
				"model":         model,
				"stop_reason":   nil,
				"stop_sequence": nil,
				"usage": map[string]any{
					"input_tokens":                0,
					"output_tokens":               0,
					"cache_creation_input_tokens": 0,
					"cache_read_input_tokens":     0,
				},
			},
		})
	}

	for {
		ev, err := sc.Next()
		if ev != nil {
			switch ev.Type {
			case "start":
				if werr := emitMessageStart(); werr != nil {
					return werr
				}
			case "text-delta":
				if werr := emitMessageStart(); werr != nil {
					return werr
				}
				var pl struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				if st.currentType != "text" {
					if werr := st.openBlock(sse, "text", map[string]any{
						"type": "text",
						"text": "",
					}); werr != nil {
						return werr
					}
				}
				if werr := sse.WriteEvent("content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": st.index,
					"delta": map[string]any{"type": "text_delta", "text": pl.Text},
				}); werr != nil {
					return werr
				}
			case "reasoning-delta":
				if werr := emitMessageStart(); werr != nil {
					return werr
				}
				var pl struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(ev.Raw, &pl)
				if st.currentType != "thinking" {
					if werr := st.openBlock(sse, "thinking", map[string]any{
						"type":     "thinking",
						"thinking": "",
					}); werr != nil {
						return werr
					}
				}
				if werr := sse.WriteEvent("content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": st.index,
					"delta": map[string]any{"type": "thinking_delta", "thinking": pl.Text},
				}); werr != nil {
					return werr
				}
			case "reasoning-end":
				if st.currentType == "thinking" {
					if werr := st.closeCurrent(sse); werr != nil {
						return werr
					}
				}
			case "tool-call":
				if werr := emitMessageStart(); werr != nil {
					return werr
				}
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
				if werr := st.openBlock(sse, "tool_use", map[string]any{
					"type":  "tool_use",
					"id":    pl.ToolCallID,
					"name":  pl.ToolName,
					"input": map[string]any{},
				}); werr != nil {
					return werr
				}
				if werr := sse.WriteEvent("content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": st.index,
					"delta": map[string]any{"type": "input_json_delta", "partial_json": string(inputRaw)},
				}); werr != nil {
					return werr
				}
				if werr := st.closeCurrent(sse); werr != nil {
					return werr
				}
			case "finish":
				if werr := st.closeCurrent(sse); werr != nil {
					return werr
				}
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
				stop := anthropicStopReason(pl.FinishReason)
				if werr := sse.WriteEvent("message_delta", map[string]any{
					"type": "message_delta",
					"delta": map[string]any{
						"stop_reason":   stop,
						"stop_sequence": nil,
					},
					"usage": map[string]any{
						"input_tokens":                pl.TotalUsage.InputTokens,
						"output_tokens":               pl.TotalUsage.OutputTokens,
						"cache_read_input_tokens":     pl.TotalUsage.InputTokenDetails.CacheReadTokens,
						"cache_creation_input_tokens": pl.TotalUsage.InputTokenDetails.CacheWriteTokens,
					},
				}); werr != nil {
					return werr
				}
				if werr := sse.WriteEvent("message_stop", map[string]any{
					"type": "message_stop",
				}); werr != nil {
					return werr
				}
			case "error":
				logger.Warn("upstream error event", "raw", string(ev.Raw))
				// Anthropic's error frame, surfaced via SSE so already-flushed
				// clients see a structured failure.
				_ = sse.WriteEvent("error", map[string]any{
					"type":  "error",
					"error": map[string]any{"type": "upstream_error", "message": "upstream emitted error mid-stream"},
				})
				return errors.New("upstream emitted error event mid-stream")
			}
		}
		if errors.Is(err, io.EOF) {
			// If we never saw a finish frame (e.g. upstream truncated),
			// still close any open block so the protocol stays balanced.
			if st.currentType != "" {
				_ = st.closeCurrent(sse)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("scan upstream sse: %w", err)
		}
	}
}

// convertAnthropicMessages produces CC content-block messages. The
// Anthropic schema is closer to CC's than OpenAI's, so most blocks
// pass through with minor renaming (thinking → reasoning, tool_use →
// tool-call, tool_result → CC's role=tool message).
func convertAnthropicMessages(in []anthropicMessage) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(in))
	for i, m := range in {
		switch m.Role {
		case "user":
			converted, isToolResult, err := convertAnthropicUserContent(m.Content)
			if err != nil {
				return nil, fmt.Errorf("messages[%d]: %w", i, err)
			}
			if isToolResult {
				// Tool results inside a user-role message map to a CC
				// role=tool message.
				out = append(out, mustMarshal(map[string]any{
					"role":    "tool",
					"content": converted,
				}))
				continue
			}
			out = append(out, mustMarshal(map[string]any{
				"role":    "user",
				"content": converted,
			}))
		case "assistant":
			blocks, err := convertAnthropicAssistantContent(m.Content)
			if err != nil {
				return nil, fmt.Errorf("messages[%d]: %w", i, err)
			}
			out = append(out, mustMarshal(map[string]any{
				"role":    "assistant",
				"content": blocks,
			}))
		default:
			return nil, fmt.Errorf("messages[%d]: unsupported role %q", i, m.Role)
		}
	}
	return out, nil
}

// convertAnthropicUserContent returns the CC blocks for one user-role
// message. If the message is entirely tool_result blocks the second
// return is true — the caller should emit a role=tool message instead
// of role=user.
func convertAnthropicUserContent(raw json.RawMessage) (any, bool, error) {
	// String shorthand.
	if s, ok := jsonStringOrConcat(raw); ok && len(raw) > 0 && string(raw) != "null" && raw[0] == '"' {
		return s, false, nil
	}
	var blocks []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, false, fmt.Errorf("content must be string or block array: %w", err)
	}

	allToolResults := len(blocks) > 0
	out := make([]any, 0, len(blocks))
	for j, b := range blocks {
		typRaw, ok := b["type"]
		if !ok {
			return nil, false, fmt.Errorf("blocks[%d]: missing type", j)
		}
		var typ string
		_ = json.Unmarshal(typRaw, &typ)
		switch typ {
		case "text":
			var text string
			_ = json.Unmarshal(b["text"], &text)
			out = append(out, map[string]any{"type": "text", "text": text})
			allToolResults = false
		case "tool_result":
			var toolUseID string
			_ = json.Unmarshal(b["tool_use_id"], &toolUseID)
			result := normalizeToolResultValue(b["content"])
			out = append(out, map[string]any{
				"type":       "tool-result",
				"toolCallId": toolUseID,
				"toolName":   "",
				"output":     map[string]any{"type": "text", "value": result},
			})
		case "image", "document":
			// Skipped: Go-tier text-only proxying.
			allToolResults = false
		default:
			// Pass through unknown block types defensively so future
			// content types don't 400 the request.
			anyBlock := make(map[string]any, len(b))
			for k, v := range b {
				var dst any
				_ = json.Unmarshal(v, &dst)
				anyBlock[k] = dst
			}
			out = append(out, anyBlock)
			allToolResults = false
		}
	}
	return out, allToolResults, nil
}

func convertAnthropicAssistantContent(raw json.RawMessage) ([]any, error) {
	// Anthropic allows string shorthand for assistant content too.
	if s, ok := jsonStringOrConcat(raw); ok && len(raw) > 0 && raw[0] == '"' {
		return []any{map[string]any{"type": "text", "text": s}}, nil
	}
	var blocks []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("assistant content must be string or block array: %w", err)
	}
	out := make([]any, 0, len(blocks))
	for j, b := range blocks {
		typRaw, ok := b["type"]
		if !ok {
			return nil, fmt.Errorf("blocks[%d]: missing type", j)
		}
		var typ string
		_ = json.Unmarshal(typRaw, &typ)
		switch typ {
		case "text":
			var text string
			_ = json.Unmarshal(b["text"], &text)
			out = append(out, map[string]any{"type": "text", "text": text})
		case "thinking":
			var text string
			if err := json.Unmarshal(b["thinking"], &text); err != nil {
				_ = json.Unmarshal(b["text"], &text)
			}
			out = append(out, map[string]any{"type": "reasoning", "text": text})
		case "tool_use":
			var id, name string
			_ = json.Unmarshal(b["id"], &id)
			_ = json.Unmarshal(b["name"], &name)
			var input any
			if v, ok := b["input"]; ok && len(v) > 0 {
				_ = json.Unmarshal(v, &input)
			}
			out = append(out, map[string]any{
				"type":       "tool-call",
				"toolCallId": id,
				"toolName":   name,
				"input":      input,
			})
		default:
			anyBlock := make(map[string]any, len(b))
			for k, v := range b {
				var dst any
				_ = json.Unmarshal(v, &dst)
				anyBlock[k] = dst
			}
			out = append(out, anyBlock)
		}
	}
	return out, nil
}

// normalizeToolResultValue flattens a tool_result.content (which may
// itself be a string or a content-block array) into a single string
// suitable for CC's text-only tool-result output.
func normalizeToolResultValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var b strings.Builder
		for _, blk := range blocks {
			if blk.Type == "text" || blk.Type == "" {
				b.WriteString(blk.Text)
			}
		}
		return b.String()
	}
	return string(raw)
}

func convertAnthropicTools(in []anthropicTool) []json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make([]json.RawMessage, 0, len(in))
	for _, t := range in {
		schema := t.InputSchema
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		ccTool := map[string]any{
			"type":         "function",
			"name":         t.Name,
			"description":  t.Description,
			"input_schema": json.RawMessage(schema),
		}
		out = append(out, mustMarshal(ccTool))
	}
	return out
}

// normalizeAnthropicSystem accepts the system field in either of its
// two Anthropic shapes (string or content-block array) and returns the
// shape CC expects (a JSON string in params.system, joined when
// multiple text blocks).
func normalizeAnthropicSystem(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if s == "" {
			return nil
		}
		return raw
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var b strings.Builder
		for _, blk := range blocks {
			if blk.Type == "text" || blk.Type == "" {
				if b.Len() > 0 {
					b.WriteString("\n\n")
				}
				b.WriteString(blk.Text)
			}
		}
		if b.Len() == 0 {
			return nil
		}
		out, _ := json.Marshal(b.String())
		return out
	}
	// Unknown shape — pass through and hope CC accepts it.
	return raw
}

func anthropicStopReason(cc string) string {
	switch cc {
	case "tool-calls", "tool_calls":
		return "tool_use"
	case "length", "max_tokens", "max-tokens", "max_output_tokens":
		return "max_tokens"
	case "stop", "":
		return "end_turn"
	default:
		return "end_turn"
	}
}

func writeAnthropicError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    code,
			"message": msg,
		},
	})
}

func mapCCErrorToAnthropic(w http.ResponseWriter, err error) {
	var ae *cc.APIError
	if errors.As(err, &ae) {
		status := ae.HTTPStatus
		if status == 0 {
			status = http.StatusBadGateway
		}
		writeAnthropicError(w, status, strings.ToLower(ae.Body.Code), ae.Body.Message)
		return
	}
	writeAnthropicError(w, http.StatusBadGateway, "upstream_error", err.Error())
}

func collectIgnoredAnthropicFields(req *anthropicRequest) []string {
	var out []string
	add := func(name string, raw json.RawMessage) {
		if len(raw) > 0 && string(raw) != "null" {
			out = append(out, name)
		}
	}
	add("temperature", req.Temperature)
	add("top_p", req.TopP)
	add("top_k", req.TopK)
	add("stop_sequences", req.StopSequences)
	add("tool_choice", req.ToolChoice)
	add("thinking", req.Thinking)
	return out
}

func newAnthropicMessageID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "msg_" + hex.EncodeToString(b[:])
}

func mustMarshal(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		// Should not happen with our internal types — panic to fail
		// loud in tests rather than silently emitting `null`.
		panic(fmt.Sprintf("proxy: json marshal: %v", err))
	}
	return raw
}
