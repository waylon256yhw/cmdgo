package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/pool"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// parseAnthropicSSE parses `event: name\ndata: json\n\n` frames into
// (eventName, decoded payload) pairs.
type anthFrame struct {
	Event string
	Data  map[string]any
}

func parseAnthropicSSE(t *testing.T, body []byte) []anthFrame {
	t.Helper()
	var out []anthFrame
	for _, raw := range bytes.Split(body, []byte("\n\n")) {
		raw = bytes.TrimRight(raw, "\r\n")
		if len(raw) == 0 {
			continue
		}
		var ev, data string
		for _, line := range bytes.Split(raw, []byte("\n")) {
			switch {
			case bytes.HasPrefix(line, []byte("event: ")):
				ev = string(line[len("event: "):])
			case bytes.HasPrefix(line, []byte("data: ")):
				data = string(line[len("data: "):])
			}
		}
		var decoded map[string]any
		if data != "" {
			if err := json.Unmarshal([]byte(data), &decoded); err != nil {
				t.Fatalf("decode frame data=%q: %v", data, err)
			}
		}
		out = append(out, anthFrame{Event: ev, Data: decoded})
	}
	return out
}

func newAnthropicHandler(t *testing.T, baseURL string) (*AnthropicHandler, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Update(func(s *store.State) error {
		s.Accounts = append(s.Accounts, store.Account{
			ID:               "test-account",
			Name:             "Tester",
			UserName:         "tester",
			APIKey:           "user_testkey1234567890",
			AddedAt:          time.Now(),
			LastKnownCredits: 9.99,
		})
		return nil
	})
	return &AnthropicHandler{
		Store:  st,
		CC:     cc.NewWithBaseURL(baseURL),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, st
}

func TestAnthropicHappyPathStream(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"text-delta","text":"Hello"}`,
		`{"type":"text-delta","text":" world"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":12,"outputTokens":2,"totalTokens":14,"inputTokenDetails":{"cacheReadTokens":7,"noCacheTokens":5,"cacheWriteTokens":0},"outputTokenDetails":{"textTokens":2,"reasoningTokens":0}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newAnthropicHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"temperature":0.5}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer pcc_test")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("X-Cmdgo-Ignored"); !strings.Contains(got, "temperature") {
		t.Errorf("ignored header=%q", got)
	}

	frames := parseAnthropicSSE(t, rr.Body.Bytes())
	wantSeq := []string{
		"message_start",
		"content_block_start",
		"content_block_delta",
		"content_block_delta",
		"content_block_stop",
		"message_delta",
		"message_stop",
	}
	if len(frames) != len(wantSeq) {
		var got []string
		for _, f := range frames {
			got = append(got, f.Event)
		}
		t.Fatalf("frame sequence: got %v, want %v", got, wantSeq)
	}
	for i := range frames {
		if frames[i].Event != wantSeq[i] {
			t.Errorf("frames[%d]=%s, want %s", i, frames[i].Event, wantSeq[i])
		}
	}

	// content_block_start should declare type=text, index=0
	cbs := frames[1].Data
	cb, _ := cbs["content_block"].(map[string]any)
	if cb["type"] != "text" {
		t.Errorf("first content_block.type=%v, want text", cb["type"])
	}

	// content_block_delta payloads should be text_delta
	for _, idx := range []int{2, 3} {
		delta, _ := frames[idx].Data["delta"].(map[string]any)
		if delta["type"] != "text_delta" {
			t.Errorf("frames[%d].delta.type=%v, want text_delta", idx, delta["type"])
		}
	}

	// message_delta should have stop_reason=end_turn and usage with cache_read
	md := frames[5].Data
	delta, _ := md["delta"].(map[string]any)
	if delta["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason=%v", delta["stop_reason"])
	}
	usage, _ := md["usage"].(map[string]any)
	if usage["cache_read_input_tokens"].(float64) != 7 {
		t.Errorf("cache_read_input_tokens=%v", usage["cache_read_input_tokens"])
	}
	if usage["output_tokens"].(float64) != 2 {
		t.Errorf("output_tokens=%v", usage["output_tokens"])
	}
}

func TestAnthropicThinkingBlock(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"reasoning-delta","text":"Let me think..."}`,
		`{"type":"reasoning-end"}`,
		`{"type":"text-delta","text":"Answer"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":3,"outputTokens":4,"totalTokens":7,"inputTokenDetails":{},"outputTokenDetails":{"reasoningTokens":3,"textTokens":1}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newAnthropicHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	frames := parseAnthropicSSE(t, rr.Body.Bytes())
	// Expect: message_start, content_block_start(thinking), content_block_delta(thinking_delta),
	//         content_block_stop, content_block_start(text), content_block_delta(text_delta),
	//         content_block_stop, message_delta, message_stop
	if got := len(frames); got != 9 {
		var ev []string
		for _, f := range frames {
			ev = append(ev, f.Event)
		}
		t.Fatalf("frame count=%d, sequence=%v", got, ev)
	}

	cbThink, _ := frames[1].Data["content_block"].(map[string]any)
	if cbThink["type"] != "thinking" {
		t.Errorf("first block.type=%v, want thinking", cbThink["type"])
	}
	thinkDelta, _ := frames[2].Data["delta"].(map[string]any)
	if thinkDelta["type"] != "thinking_delta" {
		t.Errorf("delta.type=%v, want thinking_delta", thinkDelta["type"])
	}

	cbText, _ := frames[4].Data["content_block"].(map[string]any)
	if cbText["type"] != "text" {
		t.Errorf("second block.type=%v, want text", cbText["type"])
	}
	// Indices should be 0 and 1
	if int(frames[1].Data["index"].(float64)) != 0 {
		t.Error("thinking index != 0")
	}
	if int(frames[4].Data["index"].(float64)) != 1 {
		t.Error("text index != 1")
	}
}

func TestAnthropicToolUseBlock(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"tool-call","toolCallId":"call_xyz","toolName":"get_weather","input":{"city":"Osaka"}}`,
		`{"type":"finish","finishReason":"tool-calls","totalUsage":{"inputTokens":5,"outputTokens":3,"totalTokens":8,"inputTokenDetails":{},"outputTokenDetails":{}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newAnthropicHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"messages":[{"role":"user","content":"weather"}],"tools":[{"name":"get_weather","description":"...","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	frames := parseAnthropicSSE(t, rr.Body.Bytes())
	// Expect: message_start, content_block_start(tool_use), content_block_delta(input_json_delta),
	//         content_block_stop, message_delta (stop_reason=tool_use), message_stop
	if len(frames) != 6 {
		var ev []string
		for _, f := range frames {
			ev = append(ev, f.Event)
		}
		t.Fatalf("frame count=%d, sequence=%v", len(frames), ev)
	}
	cb, _ := frames[1].Data["content_block"].(map[string]any)
	if cb["type"] != "tool_use" || cb["name"] != "get_weather" || cb["id"] != "call_xyz" {
		t.Errorf("tool_use block: %+v", cb)
	}
	delta, _ := frames[2].Data["delta"].(map[string]any)
	if delta["type"] != "input_json_delta" {
		t.Errorf("delta.type=%v", delta["type"])
	}
	if !strings.Contains(delta["partial_json"].(string), "Osaka") {
		t.Errorf("partial_json missing Osaka: %v", delta["partial_json"])
	}

	md, _ := frames[4].Data["delta"].(map[string]any)
	if md["stop_reason"] != "tool_use" {
		t.Errorf("stop_reason=%v, want tool_use", md["stop_reason"])
	}

	// Verify the request to CC included the tool with input_schema preserved.
	var sentBody struct {
		Params struct {
			Tools []json.RawMessage `json:"tools"`
		} `json:"params"`
	}
	if err := json.Unmarshal(mock.lastBody, &sentBody); err != nil {
		t.Fatal(err)
	}
	if len(sentBody.Params.Tools) != 1 {
		t.Fatalf("expected 1 tool in upstream body, got %d", len(sentBody.Params.Tools))
	}
}

func TestAnthropicSystemAsStringAndBlocks(t *testing.T) {
	type tc struct {
		name        string
		systemField string
		wantSystem  string
	}
	cases := []tc{
		{
			name:        "string",
			systemField: `"You are a tester."`,
			wantSystem:  "You are a tester.",
		},
		{
			name:        "blocks",
			systemField: `[{"type":"text","text":"Part A."},{"type":"text","text":"Part B."}]`,
			wantSystem:  "Part A.\n\nPart B.",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mock := &mockCCGenerate{t: t, events: []string{
				`{"type":"start"}`,
				`{"type":"finish","finishReason":"stop","totalUsage":{}}`,
			}}
			srv := mock.server()
			defer srv.Close()
			h, _ := newAnthropicHandler(t, srv.URL)

			reqBody := `{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"system":` + c.systemField + `,"messages":[{"role":"user","content":"hi"}]}`
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
			var sent struct {
				Params struct {
					System json.RawMessage `json:"system"`
				} `json:"params"`
			}
			if err := json.Unmarshal(mock.lastBody, &sent); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := json.Unmarshal(sent.Params.System, &got); err != nil {
				t.Fatalf("system not a JSON string: %s", sent.Params.System)
			}
			if got != c.wantSystem {
				t.Errorf("system=%q, want %q", got, c.wantSystem)
			}
		})
	}
}

func TestAnthropicToolResultMapsToToolRole(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newAnthropicHandler(t, srv.URL)

	// Anthropic places tool_result blocks inside a user-role message.
	// CC expects them in a separate role=tool message.
	reqBody := `{
		"model":"deepseek/deepseek-v4-pro",
		"max_tokens":64,
		"messages":[
			{"role":"user","content":"What's the weather?"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_a","name":"get_weather","input":{"city":"Kyoto"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_a","content":"Sunny, 22C"}]}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var sent struct {
		Params struct {
			Messages []map[string]any `json:"messages"`
		} `json:"params"`
	}
	if err := json.Unmarshal(mock.lastBody, &sent); err != nil {
		t.Fatal(err)
	}
	if len(sent.Params.Messages) != 3 {
		t.Fatalf("upstream message count=%d, want 3", len(sent.Params.Messages))
	}
	roles := []string{
		sent.Params.Messages[0]["role"].(string),
		sent.Params.Messages[1]["role"].(string),
		sent.Params.Messages[2]["role"].(string),
	}
	wantRoles := []string{"user", "assistant", "tool"}
	for i := range roles {
		if roles[i] != wantRoles[i] {
			t.Errorf("messages[%d].role=%q, want %q", i, roles[i], wantRoles[i])
		}
	}

	// Inspect the tool message content.
	toolMsg := sent.Params.Messages[2]
	toolBlocks, ok := toolMsg["content"].([]any)
	if !ok || len(toolBlocks) != 1 {
		t.Fatalf("tool message content: %+v", toolMsg["content"])
	}
	block := toolBlocks[0].(map[string]any)
	if block["toolCallId"] != "call_a" {
		t.Errorf("toolCallId=%v", block["toolCallId"])
	}
	output, _ := block["output"].(map[string]any)
	if output["value"] != "Sunny, 22C" {
		t.Errorf("output.value=%v", output["value"])
	}

	// And the assistant message in CC format must carry tool-call shape.
	asstMsg := sent.Params.Messages[1]
	asstBlocks, _ := asstMsg["content"].([]any)
	asstBlock0 := asstBlocks[0].(map[string]any)
	if asstBlock0["type"] != "tool-call" {
		t.Errorf("assistant block type=%v", asstBlock0["type"])
	}
	if asstBlock0["toolName"] != "get_weather" {
		t.Errorf("toolName=%v", asstBlock0["toolName"])
	}
}

func TestAnthropicRejectsNonGoModel(t *testing.T) {
	h, _ := newAnthropicHandler(t, "http://unreachable.local")
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-opus-4","max_tokens":64,"messages":[{"role":"user","content":"x"}]}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "model_not_in_plan_go") {
		t.Errorf("body=%s", rr.Body.String())
	}
}

func TestAnthropicRequiresMaxTokens(t *testing.T) {
	h, _ := newAnthropicHandler(t, "http://unreachable.local")
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"x"}]}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAnthropicPropagatesCCError(t *testing.T) {
	mock := &mockCCGenerate{
		t:          t,
		failBefore: true,
		failStatus: http.StatusForbidden,
		failBody:   `{"success":false,"error":{"code":"FORBIDDEN","status":403,"message":"MODEL_NOT_IN_PLAN"}}`,
	}
	srv := mock.server()
	defer srv.Close()
	h, _ := newAnthropicHandler(t, srv.URL)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAnthropicMetadataUserIDDrivesAffinity(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newAnthropicHandler(t, srv.URL)

	send := func(userID string) string {
		reqBody := `{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"metadata":{"user_id":"` + userID + `"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
		req.Header.Set("Authorization", "Bearer pcc_same_token")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return mock.lastHeaders.Get("X-Session-Id")
	}
	sidA := send("alice")
	sidB := send("bob")
	if sidA == "" || sidB == "" {
		t.Fatalf("sids empty: %q %q", sidA, sidB)
	}
	if sidA == sidB {
		t.Errorf("expected metadata.user_id to differentiate session ids; both = %q", sidA)
	}
}

// TestAnthropicMidStreamErrorDoesNotPoisonAccount: same regression
// as TestOpenAIMidStreamErrorDoesNotPoisonAccount, but for the
// /v1/messages adapter. See openai_test.go for context.
func TestAnthropicMidStreamErrorDoesNotPoisonAccount(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"text-delta","text":"hi"}`,
		`{"type":"error","error":{"type":"server_error","message":"Service temporarily unavailable. Please try again shortly.","statusCode":503,"isRetryable":true}}`,
	}}
	srv := mock.server()
	defer srv.Close()

	st, err := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Update(func(s *store.State) error {
		s.Accounts = append(s.Accounts, store.Account{
			ID:               "only-account",
			APIKey:           "user_only1234567890",
			AddedAt:          time.Now(),
			LastKnownCredits: 9.99,
		})
		return nil
	})

	p := pool.New(st)
	ccClient := cc.NewWithBaseURL(srv.URL)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &AnthropicHandler{
		Store:  st,
		CC:     ccClient,
		Logger: logger,
		Runner: &Runner{Pool: p, CC: ccClient, Logger: logger},
	}

	reqBody := `{"model":"deepseek/deepseek-v4-pro","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	req.Header.Set("x-api-key", "pcc_test")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	reqs, errs := p.Stats("only-account")
	if reqs == 0 {
		t.Errorf("MarkSuccess never ran: reqs=%d", reqs)
	}
	if errs != 0 {
		t.Errorf("mid-stream error poisoned account stats: reqs=%d errs=%d", reqs, errs)
	}
	if _, err := p.Pick(pool.PickOptions{ClientToken: "tok", Model: "deepseek/deepseek-v4-pro"}); err != nil {
		t.Errorf("Pool.Pick rejected the only account after a mid-stream error: %v", err)
	}
}
