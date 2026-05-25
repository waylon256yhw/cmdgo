package server

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

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// mockCC stands in for api.commandcode.ai during tests. Responds to
// /alpha/whoami and /alpha/billing/credits; everything else returns 404.
func mockCC(t *testing.T, apikey string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/alpha/whoami", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+apikey {
			http.Error(w, `{"success":false,"error":{"code":"UNAUTHORIZED","status":401,"message":"bad bearer"}}`, http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"user": map[string]any{
				"id":       "00000000-0000-0000-0000-deadbeef0001",
				"name":     "Test User",
				"email":    "test@example.com",
				"userName": "testuser",
			},
		})
	})
	mux.HandleFunc("/alpha/billing/credits", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"credits": map[string]any{
				"belowThreshold":   false,
				"creditThreshold":  0,
				"monthlyCredits":   9.42,
				"purchasedCredits": 0,
				"freeCredits":      0,
			},
		})
	})
	return httptest.NewServer(mux)
}

func newOAuthService(t *testing.T, ccURL string) (*OAuthService, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	return &OAuthService{
		Store:     st,
		CC:        cc.NewWithBaseURL(ccURL),
		States:    cc.NewStateStore(0),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Listen:    "127.0.0.1:8080",
		PublicURL: "",
	}, st
}

func TestPasteKeyAddsAccount(t *testing.T) {
	const apikey = "user_pastekeyhappy123"
	srv := mockCC(t, apikey)
	defer srv.Close()
	oa, st := newOAuthService(t, srv.URL)

	body, _ := json.Marshal(pasteKeyRequest{APIKey: apikey})
	req := httptest.NewRequest(http.MethodPost, "/api/oauth/paste-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	oa.HandlePasteKey(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Success bool            `json:"success"`
		Account accountResponse `json:"account"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rr.Body.String())
	}
	if !resp.Success {
		t.Fatal("expected success=true")
	}
	if !resp.Account.Created {
		t.Fatal("first paste-key should be created=true")
	}
	if resp.Account.ID != "00000000-0000-0000-0000-deadbeef0001" {
		t.Fatalf("ID mismatch: %q", resp.Account.ID)
	}
	if resp.Account.LastKnownCredits != 9.42 {
		t.Fatalf("credits mismatch: %v", resp.Account.LastKnownCredits)
	}

	snap := st.Snapshot()
	if len(snap.Accounts) != 1 {
		t.Fatalf("accounts persisted=%d, want 1", len(snap.Accounts))
	}
	if snap.Accounts[0].APIKey != apikey {
		t.Fatal("apikey not persisted")
	}
}

func TestPasteKeyDedupRefreshes(t *testing.T) {
	const apikey = "user_pastekeydedup123"
	srv := mockCC(t, apikey)
	defer srv.Close()
	oa, st := newOAuthService(t, srv.URL)

	postPasteKey := func() *http.Response {
		body, _ := json.Marshal(pasteKeyRequest{APIKey: apikey})
		req := httptest.NewRequest(http.MethodPost, "/api/oauth/paste-key", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		oa.HandlePasteKey(rr, req)
		return rr.Result()
	}

	first := postPasteKey()
	second := postPasteKey()

	if first.StatusCode != 200 || second.StatusCode != 200 {
		t.Fatalf("statuses: %d %d", first.StatusCode, second.StatusCode)
	}
	if got := len(st.Snapshot().Accounts); got != 1 {
		t.Fatalf("duplicate add: %d accounts", got)
	}

	var resp struct {
		Account accountResponse `json:"account"`
	}
	_ = json.NewDecoder(second.Body).Decode(&resp)
	if resp.Account.Created {
		t.Fatal("second add should be created=false (refresh)")
	}
}

func TestPasteKeyRejectsMalformedAPIKey(t *testing.T) {
	oa, _ := newOAuthService(t, "http://unreachable.local")

	for _, body := range []string{
		`{"apiKey":""}`,
		`{"apiKey":"not_user_prefix"}`,
		`{"apiKey":"user_"}`, // too short
		`{}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/oauth/paste-key", strings.NewReader(body))
		rr := httptest.NewRecorder()
		oa.HandlePasteKey(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("body=%s: status=%d, want 400", body, rr.Code)
		}
	}
}

func TestCallbackHappyPath(t *testing.T) {
	const apikey = "user_callbackhappy123"
	srv := mockCC(t, apikey)
	defer srv.Close()
	oa, st := newOAuthService(t, srv.URL)

	state, err := oa.States.Generate()
	if err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(cc.CallbackPayload{
		APIKey:   apikey,
		State:    state,
		UserID:   "00000000-0000-0000-0000-deadbeef0001",
		UserName: "testuser",
		KeyName:  "my-key",
	})
	req := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://commandcode.ai")
	rr := httptest.NewRecorder()

	oa.HandleOAuthCallback(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://commandcode.ai" {
		t.Errorf("CORS origin echoed=%q", got)
	}
	if len(st.Snapshot().Accounts) != 1 {
		t.Fatal("account not persisted")
	}
	// Re-use of state should fail.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Origin", "https://commandcode.ai")
	oa.HandleOAuthCallback(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("state replay status=%d, want 400", rr2.Code)
	}
}

func TestCallbackRejectsUnknownState(t *testing.T) {
	oa, _ := newOAuthService(t, "http://unreachable.local")
	body, _ := json.Marshal(cc.CallbackPayload{APIKey: "user_xxx12345", State: "never-generated"})
	req := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	oa.HandleOAuthCallback(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", rr.Code)
	}
}

func TestCallbackOmitsCORSForUnknownOrigin(t *testing.T) {
	oa, _ := newOAuthService(t, "http://unreachable.local")
	body, _ := json.Marshal(cc.CallbackPayload{Error: "access_denied"})
	req := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://evil.example.com")
	rr := httptest.NewRecorder()
	oa.HandleOAuthCallback(rr, req)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("untrusted origin echoed: %q", got)
	}
}

func TestPreflightCORS(t *testing.T) {
	oa, _ := newOAuthService(t, "http://unreachable.local")
	req := httptest.NewRequest(http.MethodOptions, "/callback", nil)
	req.Header.Set("Origin", "https://commandcode.ai")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rr := httptest.NewRecorder()
	oa.HandleOAuthPreflight(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d", rr.Code)
	}
	for _, want := range []struct {
		header, value string
	}{
		{"Access-Control-Allow-Origin", "https://commandcode.ai"},
		{"Access-Control-Allow-Methods", "POST, OPTIONS"},
		{"Access-Control-Allow-Headers", "Content-Type"},
		{"Access-Control-Allow-Private-Network", "true"},
	} {
		if got := rr.Header().Get(want.header); got != want.value {
			t.Errorf("%s=%q, want %q", want.header, got, want.value)
		}
	}
}

func TestOAuthStartReturnsURL(t *testing.T) {
	oa, _ := newOAuthService(t, "http://unreachable.local")
	req := httptest.NewRequest(http.MethodPost, "/api/oauth/start", nil)
	rr := httptest.NewRecorder()
	oa.HandleOAuthStart(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp startResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if !strings.HasPrefix(resp.AuthURL, "https://commandcode.ai/studio/auth/cli?") {
		t.Errorf("authURL=%q", resp.AuthURL)
	}
	if !strings.Contains(resp.AuthURL, "state="+resp.State) {
		t.Errorf("authURL missing state: %q", resp.AuthURL)
	}
	if resp.Callback != "http://localhost:8080/callback" {
		t.Errorf("callback=%q", resp.Callback)
	}
}
