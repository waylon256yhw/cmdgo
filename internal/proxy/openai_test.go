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

	reqBody := `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"weather"}],"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]}`
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
