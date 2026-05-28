package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

// allowedOAuthOrigins are the browser origins the `/callback` endpoint
// trusts for CORS. Matches docs/cc-api.md §5.
var allowedOAuthOrigins = map[string]bool{
	"https://commandcode.ai":         true,
	"https://staging.commandcode.ai": true,
	"http://localhost:3000":          true,
}

// AuthURLTemplate is the CC Studio entry point for the CLI login flow.
// callback + state are appended as query params.
const AuthURLTemplate = "https://commandcode.ai/studio/auth/cli"

// OAuthService bundles the dependencies the OAuth handlers need. Wired
// in main.go and shared between several HTTP routes.
type OAuthService struct {
	Store     *store.Store
	CC        *cc.Client
	States    *cc.StateStore
	Logger    *slog.Logger
	Listen    string // server bind, used to derive a fallback callback URL
	PublicURL string // optional override for the OAuth callback URL

	// OnAccountChanged is called after an account has been added or
	// refreshed (paste-key or OAuth callback). main.go wires this to
	// the dashboard so the new card lands in the DOM via SSE without
	// the browser needing to re-poll.
	OnAccountChanged func(accountID string)
}

// startResponse is the JSON body returned from POST /api/oauth/start.
type startResponse struct {
	AuthURL  string `json:"authUrl"`
	Callback string `json:"callback"`
	State    string `json:"state"`
}

// pasteKeyRequest is the JSON body accepted by POST /api/oauth/paste-key.
type pasteKeyRequest struct {
	APIKey string `json:"apiKey"`
}

// accountResponse is what we return to the dashboard after a successful
// add. APIKey is redacted; the dashboard never needs the raw key.
type accountResponse struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Email            string    `json:"email"`
	UserName         string    `json:"userName"`
	AddedAt          time.Time `json:"addedAt"`
	LastKnownCredits float64   `json:"lastKnownCredits"`
	Paused           bool      `json:"paused"`
	Created          bool      `json:"created"` // true if new, false if updated
}

// HandleOAuthStart issues a fresh state and returns the auth URL the
// dashboard should open in a new tab.
func (s *OAuthService) HandleOAuthStart(w http.ResponseWriter, r *http.Request) {
	state, err := s.States.Generate()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "state_generate_failed", err.Error())
		return
	}
	callback := s.callbackURL()
	authURL := fmt.Sprintf("%s?%s",
		AuthURLTemplate,
		url.Values{"callback": {callback}, "state": {state}}.Encode(),
	)
	writeJSON(w, http.StatusOK, startResponse{
		AuthURL:  authURL,
		Callback: callback,
		State:    state,
	})
}

// HandleOAuthCallback receives the POST from CC Studio after the user
// grants access. It validates state, fetches identity + initial credit
// snapshot, and persists the account.
func (s *OAuthService) HandleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	s.applyCORS(w, r)

	if r.Header.Get("Content-Type") != "" && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		writeJSONError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}

	var payload cc.CallbackPayload
	if err := decodeJSON(r.Body, &payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}

	if payload.Error != "" {
		s.Logger.Warn("oauth callback rejected", "error", payload.Error, "desc", payload.ErrorDescription)
		writeJSON(w, http.StatusOK, map[string]any{
			"success": false,
			"error":   payload.Error,
		})
		return
	}

	if !s.States.Consume(payload.State) {
		writeJSONError(w, http.StatusBadRequest, "invalid_state", "state token missing, expired, or already used")
		return
	}

	if !looksLikeAPIKey(payload.APIKey) {
		writeJSONError(w, http.StatusBadRequest, "invalid_apikey", "apikey missing or malformed")
		return
	}

	acc, created, err := s.addAccount(r.Context(), payload.APIKey)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "verify_failed", err.Error())
		return
	}

	s.Logger.Info("account added via callback",
		"account_id", acc.ID,
		"user_name", acc.UserName,
		"credits", acc.LastKnownCredits,
		"created", created,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"account": toAccountResponse(acc, created),
	})
}

// HandleOAuthPreflight responds to OPTIONS /callback with the CORS
// envelope CC Studio's browser expects (includes Private Network Access
// for callbacks to 127.0.0.1).
func (s *OAuthService) HandleOAuthPreflight(w http.ResponseWriter, r *http.Request) {
	s.applyCORS(w, r)
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Private-Network", "true")
	w.Header().Set("Access-Control-Max-Age", "600")
	w.WriteHeader(http.StatusNoContent)
}

// HandlePasteKey is the VPS-mode fallback: user pastes a `user_...`
// apikey copied from CC Studio. We validate it with Whoami and persist
// the same way the OAuth callback would.
func (s *OAuthService) HandlePasteKey(w http.ResponseWriter, r *http.Request) {
	var req pasteKeyRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	req.APIKey = strings.TrimSpace(req.APIKey)
	if !looksLikeAPIKey(req.APIKey) {
		writeJSONError(w, http.StatusBadRequest, "invalid_apikey", "apikey must look like user_...")
		return
	}

	acc, created, err := s.addAccount(r.Context(), req.APIKey)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "verify_failed", err.Error())
		return
	}

	s.Logger.Info("account added via paste-key",
		"account_id", acc.ID,
		"user_name", acc.UserName,
		"credits", acc.LastKnownCredits,
		"created", created,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"account": toAccountResponse(acc, created),
	})
}

// addAccount verifies an apikey and upserts it into state. Returns the
// canonical store.Account after the write, and whether it was a fresh
// insert (true) or refresh of an existing row (false).
func (s *OAuthService) addAccount(ctx context.Context, apikey string) (*store.Account, bool, error) {
	verifyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	user, err := s.CC.Whoami(verifyCtx, apikey)
	if err != nil {
		return nil, false, fmt.Errorf("whoami: %w", err)
	}
	credits, err := s.CC.BillingCredits(verifyCtx, apikey)
	billingOK := err == nil
	if !billingOK {
		// Persist the account anyway with credits unknown. Critically
		// we must NOT stamp LastKnownCreditsAt — pool.Pick uses it to
		// distinguish "real $0 balance" from "billing endpoint never
		// responded". The 60s sync (pool/sync.go) will fill it in
		// once the upstream recovers.
		s.Logger.Warn("billing credits unavailable at add time",
			"account_id", user.ID,
			"err", err,
		)
		credits = &cc.Credits{}
	}

	var saved store.Account
	var created bool
	now := time.Now().UTC()

	err = s.Store.Update(func(st *store.State) error {
		for i := range st.Accounts {
			if st.Accounts[i].ID == user.ID {
				st.Accounts[i].APIKey = apikey
				st.Accounts[i].Name = user.Name
				st.Accounts[i].Email = user.Email
				st.Accounts[i].UserName = user.UserName
				// On refresh, only overwrite the known-credits pair when
				// billing succeeded. If it failed, the old known values
				// stay put — a transient billing blip should not turn a
				// healthy account into "unknown" or "$0".
				if billingOK {
					st.Accounts[i].LastKnownCredits = credits.Total()
					st.Accounts[i].LastKnownCreditsAt = now
				}
				saved = st.Accounts[i]
				return nil
			}
		}
		acc := store.Account{
			ID:               user.ID,
			Name:             user.Name,
			Email:            user.Email,
			UserName:         user.UserName,
			APIKey:           apikey,
			AddedAt:          now,
			LastKnownCredits: credits.Total(),
		}
		if billingOK {
			// Only stamp the timestamp when we actually know the balance.
			// Zero LastKnownCreditsAt is the "unknown" marker that
			// pool.Pick reads to give a never-synced account a chance.
			acc.LastKnownCreditsAt = now
		}
		st.Accounts = append(st.Accounts, acc)
		saved = acc
		created = true
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("persist account: %w", err)
	}
	if s.OnAccountChanged != nil {
		s.OnAccountChanged(saved.ID)
	}
	return &saved, created, nil
}

// callbackURL is always built against `localhost` because CC Studio's
// OAuth flow only accepts localhost-style callbacks (anything else is
// rejected with "callback URL is invalid"). For VPS deployments the
// browser reaches that localhost endpoint via an SSH tunnel — the
// dashboard JS detects the situation and prints a copy-paste tunnel
// command.
//
// If the operator explicitly sets --public-url to a localhost-style
// URL (rare; useful in tests or weird reverse-proxy chains), we
// honour it verbatim.
func (s *OAuthService) callbackURL() string {
	if s.PublicURL != "" {
		if u, err := url.Parse(s.PublicURL); err == nil {
			host := u.Hostname()
			if host == "localhost" || host == "127.0.0.1" {
				return strings.TrimRight(s.PublicURL, "/") + "/callback"
			}
		}
	}
	port := s.listenPort()
	if port == "" {
		port = "8080"
	}
	return "http://" + net.JoinHostPort("localhost", port) + "/callback"
}

func (s *OAuthService) listenPort() string {
	if s.Listen == "" {
		return ""
	}
	_, port, err := net.SplitHostPort(s.Listen)
	if err != nil {
		return ""
	}
	return port
}

// applyCORS sets the Access-Control-Allow-* headers when the request's
// Origin is in the trusted list. When the origin is unknown the
// browser will refuse the response, which is the desired behaviour —
// we don't echo arbitrary origins back.
func (s *OAuthService) applyCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if !allowedOAuthOrigins[origin] {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
}

func looksLikeAPIKey(s string) bool {
	return strings.HasPrefix(s, "user_") && len(s) >= len("user_")+10
}

func decodeJSON(r io.Reader, dst any) error {
	if r == nil {
		return errors.New("empty body")
	}
	// Stay lenient on unknown fields — CC Studio may add metadata to
	// the callback payload over time and we'd rather ignore them than
	// 400 a live OAuth flow.
	return json.NewDecoder(io.LimitReader(r, 64<<10)).Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
			"status":  status,
		},
	})
}

func toAccountResponse(acc *store.Account, created bool) accountResponse {
	return accountResponse{
		ID:               acc.ID,
		Name:             acc.Name,
		Email:            acc.Email,
		UserName:         acc.UserName,
		AddedAt:          acc.AddedAt,
		LastKnownCredits: acc.LastKnownCredits,
		Paused:           acc.Paused,
		Created:          created,
	}
}
