package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// mockCCGenerate streams a fixed sequence of SSE events plus the given
// finish payload. It also captures the raw request body so tests can
// assert on cache_control / session id / message shape.
type mockCCGenerate struct {
	t           *testing.T
	events      []string
	failBefore  bool
	failStatus  int
	failBody    string
	lastBody    []byte
	lastHeaders http.Header
}

func (m *mockCCGenerate) server() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/alpha/generate", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.lastBody = body
		m.lastHeaders = r.Header.Clone()
		if m.failBefore {
			w.WriteHeader(m.failStatus)
			_, _ = io.WriteString(w, m.failBody)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, ev := range m.events {
			_, _ = io.WriteString(w, "data: "+ev+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		}
	})
	return httptest.NewServer(mux)
}

func newOpenAIHandler(t *testing.T, baseURL string) (*OpenAIHandler, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Seed one healthy account.
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
	return &OpenAIHandler{
		Store:  st,
		CC:     cc.NewWithBaseURL(baseURL),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, st
}

func TestOpenAIHappyPathStream(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"text-delta","text":"pong"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":10,"outputTokens":1,"totalTokens":11,"inputTokenDetails":{"cacheReadTokens":7,"noCacheTokens":3,"cacheWriteTokens":0},"outputTokenDetails":{"textTokens":1,"reasoningTokens":0}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"Reply with exactly one word: pong"}],"stream":true,"temperature":0.5}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer pcc_test_proxy_token_abc")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type=%q", got)
	}
	if got := rr.Header().Get("X-Cmdgo-Account-Id"); got != "test-account" {
		t.Errorf("account header=%q", got)
	}
	if got := rr.Header().Get("X-Cmdgo-Ignored"); !strings.Contains(got, "temperature") {
		t.Errorf("ignored header=%q, want to contain temperature", got)
	}

	chunks := parseSSE(t, rr.Body.Bytes())
	// Expect: role chunk, content chunk, finish chunk, [DONE]
	if len(chunks) < 4 {
		t.Fatalf("expected >=4 frames, got %d: %v", len(chunks), chunks)
	}
	if chunks[len(chunks)-1] != "[DONE]" {
		t.Fatalf("final frame=%q, want [DONE]", chunks[len(chunks)-1])
	}

	first := decodeChunk(t, chunks[0])
	if first.Choices[0].Delta.Role != "assistant" {
		t.Errorf("first chunk role=%q", first.Choices[0].Delta.Role)
	}

	// Find content chunk
	var sawContent bool
	for _, c := range chunks[:len(chunks)-1] {
		ch := decodeChunk(t, c)
		if ch.Choices[0].Delta.Content == "pong" {
			sawContent = true
		}
	}
	if !sawContent {
		t.Fatal("no chunk emitted text 'pong'")
	}

	// Find finish chunk with usage
	last := decodeChunk(t, chunks[len(chunks)-2])
	if last.Choices[0].FinishReason == nil || *last.Choices[0].FinishReason != "stop" {
		t.Fatalf("finish_reason=%v", last.Choices[0].FinishReason)
	}
	if last.Usage == nil {
		t.Fatal("missing usage in final chunk")
	}
	if last.Usage.PromptTokens != 10 || last.Usage.CompletionTokens != 1 || last.Usage.TotalTokens != 11 {
		t.Errorf("usage mismatch: %+v", last.Usage)
	}
	if last.Usage.PromptTokensDetails == nil || last.Usage.PromptTokensDetails.CachedTokens != 7 {
		t.Errorf("cached_tokens mismatch: %+v", last.Usage.PromptTokensDetails)
	}

	// Inspect what we sent upstream.
	var sentBody struct {
		Params struct {
			Model        string            `json:"model"`
			Messages     []json.RawMessage `json:"messages"`
			CacheControl *cc.CacheControl  `json:"cache_control"`
			Stream       bool              `json:"stream"`
		} `json:"params"`
	}
	if err := json.Unmarshal(mock.lastBody, &sentBody); err != nil {
		t.Fatal(err)
	}
	if sentBody.Params.Model != "deepseek/deepseek-v4-pro" {
		t.Errorf("upstream model=%q", sentBody.Params.Model)
	}
	if sentBody.Params.CacheControl == nil || sentBody.Params.CacheControl.Type != "ephemeral" {
		t.Errorf("cache_control mismatch: %+v", sentBody.Params.CacheControl)
	}
	if !sentBody.Params.Stream {
		t.Error("upstream stream not forced to true")
	}
	if sid := mock.lastHeaders.Get("X-Session-Id"); sid == "" || len(sid) != 32 {
		t.Errorf("session-id header invalid: %q", sid)
	}
	if got := mock.lastHeaders.Get("X-Project-Slug"); got != "cmdgo" {
		t.Errorf("project-slug=%q", got)
	}
	if got := mock.lastHeaders.Get("X-Command-Code-Version"); got != "0.24.1" {
		t.Errorf("cli-version=%q", got)
	}
}

func TestOpenAIRejectsNonGoModel(t *testing.T) {
	h, _ := newOpenAIHandler(t, "http://unreachable.local")
	reqBody := `{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"x"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "model_not_in_plan_go") {
		t.Errorf("body=%s", rr.Body.String())
	}
}

func TestOpenAIPropagatesCCError(t *testing.T) {
	mock := &mockCCGenerate{
		t:          t,
		failBefore: true,
		failStatus: http.StatusForbidden,
		failBody:   `{"success":false,"error":{"code":"FORBIDDEN","status":403,"message":"MODEL_NOT_IN_PLAN: ..."}}`,
	}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestOpenAIToolCallStream(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"tool-call","toolCallId":"call_abc","toolName":"get_weather","input":{"city":"Tokyo"}}`,
		`{"type":"finish","finishReason":"tool-calls","totalUsage":{"inputTokens":12,"outputTokens":3,"totalTokens":15,"inputTokenDetails":{"cacheReadTokens":0,"noCacheTokens":12,"cacheWriteTokens":0},"outputTokenDetails":{"textTokens":0,"reasoningTokens":0}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"weather"}],"stream":true,"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	chunks := parseSSE(t, rr.Body.Bytes())
	var sawTool bool
	var finishReason string
	for _, c := range chunks[:len(chunks)-1] {
		ch := decodeChunk(t, c)
		for _, tc := range ch.Choices[0].Delta.ToolCalls {
			if tc.Function != nil && tc.Function.Name == "get_weather" {
				sawTool = true
				if !strings.Contains(tc.Function.Arguments, "Tokyo") {
					t.Errorf("tool arguments missing Tokyo: %q", tc.Function.Arguments)
				}
			}
		}
		if ch.Choices[0].FinishReason != nil {
			finishReason = *ch.Choices[0].FinishReason
		}
	}
	if !sawTool {
		t.Fatal("did not see tool-call chunk")
	}
	if finishReason != "tool_calls" {
		t.Errorf("finish_reason=%q, want tool_calls", finishReason)
	}
}

func TestOpenAINonStreamReturnsCompleteResponse(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"text-delta","text":"po"}`,
		`{"type":"text-delta","text":"ng"}`,
		`{"type":"text-delta","text":"!"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":7,"outputTokens":3,"totalTokens":10,"inputTokenDetails":{"cacheReadTokens":2,"cacheWriteTokens":0,"noCacheTokens":5},"outputTokenDetails":{"textTokens":3,"reasoningTokens":0}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	// No `stream` field — should default to non-streaming.
	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"say pong"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type=%q, want application/json (not SSE)", ct)
	}

	var out openaiCompletion
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rr.Body.String())
	}
	if out.Object != "chat.completion" {
		t.Errorf("object=%q, want chat.completion", out.Object)
	}
	if len(out.Choices) != 1 {
		t.Fatalf("choices=%d, want 1", len(out.Choices))
	}
	msg := out.Choices[0].Message
	if msg.Role != "assistant" || msg.Content != "pong!" {
		t.Errorf("message=%+v, want role=assistant content=\"pong!\"", msg)
	}
	if out.Choices[0].FinishReason == nil || *out.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason=%v", out.Choices[0].FinishReason)
	}
	if out.Usage == nil || out.Usage.PromptTokens != 7 || out.Usage.CompletionTokens != 3 || out.Usage.TotalTokens != 10 {
		t.Errorf("usage=%+v", out.Usage)
	}
	if out.Usage.PromptTokensDetails == nil || out.Usage.PromptTokensDetails.CachedTokens != 2 {
		t.Errorf("cached_tokens=%+v", out.Usage.PromptTokensDetails)
	}
}

func TestOpenAINonStreamToolCallsContentEmptyString(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"tool-call","toolCallId":"call_abc","toolName":"get_weather","input":{"city":"Tokyo"}}`,
		`{"type":"finish","finishReason":"tool-calls","totalUsage":{"inputTokens":12,"outputTokens":3,"totalTokens":15,"inputTokenDetails":{},"outputTokenDetails":{}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"weather"}],"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var out openaiCompletion
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	msg := out.Choices[0].Message
	// OpenAI clients trip on `null` content; non-null empty string is
	// the expected shape when only tool_calls are present.
	if msg.Content != "" {
		t.Errorf("content=%q, want empty string", msg.Content)
	}
	// Verify the JSON wire form keeps `"content":""` (not null).
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"content":""`)) {
		t.Errorf("body missing \"content\":\"\" literal: %s", rr.Body.String())
	}
	if len(msg.ToolCalls) != 1 {
		t.Fatalf("tool_calls count=%d", len(msg.ToolCalls))
	}
	tc := msg.ToolCalls[0]
	if tc.ID != "call_abc" || tc.Type != "function" || tc.Function.Name != "get_weather" {
		t.Errorf("tool_call=%+v", tc)
	}
	if !strings.Contains(tc.Function.Arguments, "Tokyo") {
		t.Errorf("arguments=%q, want to contain Tokyo", tc.Function.Arguments)
	}
	if out.Choices[0].FinishReason == nil || *out.Choices[0].FinishReason != "tool_calls" {
		t.Errorf("finish_reason=%v", out.Choices[0].FinishReason)
	}
}

func TestOpenAINonStreamReasoningContent(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"reasoning-delta","text":"hmm "}`,
		`{"type":"reasoning-delta","text":"let me think"}`,
		`{"type":"reasoning-end"}`,
		`{"type":"text-delta","text":"42"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":5,"outputTokens":2,"totalTokens":7,"inputTokenDetails":{},"outputTokenDetails":{"reasoningTokens":4}}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"q"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out openaiCompletion
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	msg := out.Choices[0].Message
	if msg.Content != "42" {
		t.Errorf("content=%q, want \"42\"", msg.Content)
	}
	if msg.ReasoningContent != "hmm let me think" {
		t.Errorf("reasoning_content=%q", msg.ReasoningContent)
	}
	if out.Usage == nil || out.Usage.CompletionTokensDetails == nil || out.Usage.CompletionTokensDetails.ReasoningTokens != 4 {
		t.Errorf("reasoning_tokens=%+v", out.Usage)
	}
}

func TestOpenAINonStreamUsageAlwaysPresent(t *testing.T) {
	// CC returns a finish frame with no usage details (zero values).
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"text-delta","text":"ok"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"x"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out openaiCompletion
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Usage == nil {
		t.Fatal("usage is nil; non-stream responses must always include usage")
	}
}

func TestExtractClientTokenFallbacks(t *testing.T) {
	const want = "pcc_aff1n1ty_test_token"

	cases := []struct {
		name  string
		setup func(r *http.Request)
	}{
		{"authorization_bearer", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+want)
		}},
		{"x_api_key_header", func(r *http.Request) {
			r.Header.Set("x-api-key", want)
		}},
		{"query_token", func(r *http.Request) {
			q := r.URL.Query()
			q.Set("token", want)
			r.URL.RawQuery = q.Encode()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
			tc.setup(req)
			if got := extractClientToken(req); got != want {
				t.Errorf("extractClientToken=%q, want %q (so affinity hashes consistently across auth modes)", got, want)
			}
		})
	}

	// Authorization Bearer beats x-api-key when both are set — matches
	// server.extractProxyToken's order so the affinity key matches the
	// auth token.
	t.Run("authorization_wins_over_xapikey", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
		req.Header.Set("Authorization", "Bearer first_token")
		req.Header.Set("x-api-key", "second_token")
		if got := extractClientToken(req); got != "first_token" {
			t.Errorf("extractClientToken=%q, want first_token", got)
		}
	})
}

// TestOpenAIAffinityRoutesXAPIKeyAndBearerToSameAccount runs two
// requests against the same handler — one carrying the affinity
// token via Authorization, one via x-api-key — and asserts they
// land on the same upstream account. With the previous extractBearer
// behaviour the x-api-key request would hash with ClientToken=""
// and could land on a different shard.
func TestOpenAIAffinityRoutesXAPIKeyAndBearerToSameAccount(t *testing.T) {
	var seenAuthHeaders []string
	mux := http.NewServeMux()
	mux.HandleFunc("/alpha/generate", func(w http.ResponseWriter, r *http.Request) {
		seenAuthHeaders = append(seenAuthHeaders, r.Header.Get("Authorization"))
		writeSSE(w, `{"type":"start"}`, `{"type":"finish","finishReason":"stop","totalUsage":{}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	st, err := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Update(func(s *store.State) error {
		s.Accounts = []store.Account{
			{ID: "alpha", APIKey: "user_alpha", LastKnownCredits: 9},
			{ID: "beta", APIKey: "user_beta", LastKnownCredits: 9},
			{ID: "gamma", APIKey: "user_gamma", LastKnownCredits: 9},
		}
		return nil
	})
	p := pool.New(st)
	ccClient := cc.NewWithBaseURL(srv.URL)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &OpenAIHandler{
		Store:  st,
		CC:     ccClient,
		Logger: logger,
		Runner: &Runner{Pool: p, CC: ccClient, Logger: logger},
	}

	send := func(setAuth func(r *http.Request)) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`))
		setAuth(req)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
	}

	const aff = "pcc_same_token_for_both"
	send(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+aff) })
	send(func(r *http.Request) { r.Header.Set("x-api-key", aff) })

	if len(seenAuthHeaders) != 2 {
		t.Fatalf("got %d upstream calls, want 2", len(seenAuthHeaders))
	}
	if seenAuthHeaders[0] != seenAuthHeaders[1] {
		t.Errorf("affinity routing failed: bearer landed on %q, x-api-key landed on %q",
			seenAuthHeaders[0], seenAuthHeaders[1])
	}
}

func TestOpenAINoHealthyAccount(t *testing.T) {
	h, st := newOpenAIHandler(t, "http://unreachable.local")
	_ = st.Update(func(s *store.State) error {
		s.Accounts[0].Paused = true
		return nil
	})
	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestOpenAISystemMessageHoisted(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"finish","finishReason":"stop","totalUsage":{}}`,
	}}
	srv := mock.server()
	defer srv.Close()
	h, _ := newOpenAIHandler(t, srv.URL)

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"system","content":"You are terse."},{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	var sent struct {
		Params struct {
			System   json.RawMessage   `json:"system"`
			Messages []json.RawMessage `json:"messages"`
		} `json:"params"`
	}
	if err := json.Unmarshal(mock.lastBody, &sent); err != nil {
		t.Fatal(err)
	}
	if string(sent.Params.System) != `"You are terse."` {
		t.Errorf("system field=%s", sent.Params.System)
	}
	if len(sent.Params.Messages) != 1 {
		t.Fatalf("messages count=%d, want 1 (system hoisted out)", len(sent.Params.Messages))
	}
}

// --- helpers ---

func parseSSE(t *testing.T, body []byte) []string {
	t.Helper()
	var out []string
	for _, frame := range bytes.Split(body, []byte("\n\n")) {
		frame = bytes.TrimRight(frame, "\r\n")
		if len(frame) == 0 {
			continue
		}
		if !bytes.HasPrefix(frame, []byte("data: ")) {
			t.Fatalf("frame missing data: prefix: %q", frame)
		}
		out = append(out, string(frame[len("data: "):]))
	}
	return out
}

func decodeChunk(t *testing.T, raw string) openaiChunk {
	t.Helper()
	var c openaiChunk
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatalf("decode chunk %q: %v", raw, err)
	}
	if len(c.Choices) == 0 {
		t.Fatalf("chunk has no choices: %q", raw)
	}
	return c
}

// statusString is a small helper to format finish_reason pointers nicely
// in test failures.
func statusString(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q", *p)
}

var _ = statusString // keep available for ad-hoc test debugging

// TestOpenAIMidStreamErrorDoesNotPoisonAccount reproduces the
// production bug seen against real CC: upstream sometimes emits a
// `{"type":"error","error":{...,"isRetryable":true}}` event after
// we've already started flushing to the client. Old behaviour was
// to count this as a stream error and MarkError on the account,
// which — with a single-account deployment — instantly tripped the
// 20% rolling error-rate threshold and rejected the next request
// with 503 no_account.
//
// Fix: post-flush errors no longer touch the account's rolling
// stats. retry.Runner alone is responsible for MarkError on
// pre-flush failures.
func TestOpenAIMidStreamErrorDoesNotPoisonAccount(t *testing.T) {
	mock := &mockCCGenerate{t: t, events: []string{
		`{"type":"start"}`,
		`{"type":"text-delta","text":"hi"}`,
		// Mid-stream upstream error matching what production CC sent.
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
	h := &OpenAIHandler{
		Store:  st,
		CC:     ccClient,
		Logger: logger,
		Runner: &Runner{Pool: p, CC: ccClient, Logger: logger},
	}

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer pcc_test")
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

	// Account should still be pickable for a follow-up request — the
	// real symptom of the bug was that the very next call returned
	// 503 no_account.
	if _, err := p.Pick(pool.PickOptions{ClientToken: "tok", Model: "deepseek/deepseek-v4-pro"}); err != nil {
		t.Errorf("Pool.Pick rejected the only account after a mid-stream error: %v", err)
	}
}
