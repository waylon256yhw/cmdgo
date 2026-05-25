package cc

import (
	"encoding/json"
	"fmt"
)

// baseURL is the Command Code API root. Overridable in tests by
// constructing a Client with NewWithBaseURL.
const baseURL = "https://api.commandcode.ai"

// User is the identity blob returned by `GET /alpha/whoami`. See
// docs/cc-api.md §1.
type User struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	UserName string `json:"userName"`
}

// Credits is the body of `GET /alpha/billing/credits` (the field named
// `credits` in the outer envelope). All amounts are USD. See
// docs/cc-api.md §3.
type Credits struct {
	BelowThreshold   bool    `json:"belowThreshold"`
	CreditThreshold  float64 `json:"creditThreshold"`
	MonthlyCredits   float64 `json:"monthlyCredits"`
	PurchasedCredits float64 `json:"purchasedCredits"`
	FreeCredits      float64 `json:"freeCredits"`
}

// Total is the spendable balance — pool health checks use this against
// store.Settings.MinCreditsUSD.
func (c Credits) Total() float64 {
	return c.MonthlyCredits + c.PurchasedCredits + c.FreeCredits
}

// CallbackPayload is what CC Studio POSTs to our `/callback` endpoint
// once a user grants access. `Error` and `ErrorDescription` carry the
// access-denied path. See docs/cc-api.md §5.
type CallbackPayload struct {
	APIKey           string `json:"apiKey,omitempty"`
	State            string `json:"state,omitempty"`
	UserID           string `json:"userId,omitempty"`
	UserName         string `json:"userName,omitempty"`
	KeyName          string `json:"keyName,omitempty"`
	Error            string `json:"error,omitempty"`
	ErrorDescription string `json:"error_description,omitempty"`
}

// GenerateBody is the JSON document POSTed to `/alpha/generate`. The
// `Params` payload (model, messages, tools, system, ...) gets built by
// the protocol adapters in internal/proxy; we keep its shape loose here
// so adapters can pass through arbitrary content blocks verbatim.
type GenerateBody struct {
	Config         GenerateConfig  `json:"config"`
	Memory         string          `json:"memory"`
	Taste          string          `json:"taste"`
	Skills         json.RawMessage `json:"skills"`
	PermissionMode string          `json:"permissionMode"`
	Params         GenerateParams  `json:"params"`
}

// GenerateConfig mirrors what the `cmd` CLI sends about the user's
// working directory. cmdgo fills it with safe placeholders — the
// values do not affect generation, only CC's internal logging.
type GenerateConfig struct {
	WorkingDir    string   `json:"workingDir"`
	Date          string   `json:"date"`
	Environment   string   `json:"environment"`
	Structure     []string `json:"structure"`
	IsGitRepo     bool     `json:"isGitRepo"`
	CurrentBranch string   `json:"currentBranch"`
	MainBranch    string   `json:"mainBranch"`
	GitStatus     string   `json:"gitStatus"`
	RecentCommits []string `json:"recentCommits"`
}

// GenerateParams is the meat of a generate request. Messages, tools,
// and system are kept as RawMessage so adapters can construct them in
// the upstream's exact Vercel-AI-SDK shape without us re-modelling
// every content-block variant.
type GenerateParams struct {
	Model        string            `json:"model"`
	Messages     []json.RawMessage `json:"messages"`
	Tools        []json.RawMessage `json:"tools,omitempty"`
	System       json.RawMessage   `json:"system,omitempty"`
	MaxTokens    int               `json:"max_tokens,omitempty"`
	Stream       bool              `json:"stream"`
	CacheControl *CacheControl     `json:"cache_control,omitempty"`
}

// CacheControl is the one-liner we drop into every request to opt
// every Go-tier model into ephemeral caching, per plan.md §8.
type CacheControl struct {
	Type string `json:"type"`
}

// EphemeralCacheControl is the only instance the proxy ever needs.
var EphemeralCacheControl = &CacheControl{Type: "ephemeral"}

// APIError is CC's standard 4xx/5xx error envelope.
type APIError struct {
	HTTPStatus int          `json:"-"`
	Success    bool         `json:"success"`
	Body       APIErrorBody `json:"error"`
}

// APIErrorBody mirrors the inner `error` object.
type APIErrorBody struct {
	Code    string `json:"code"`
	Status  int    `json:"status"`
	Message string `json:"message"`
	Docs    string `json:"docs,omitempty"`
}

func (e *APIError) Error() string {
	if e == nil {
		return "cc: <nil APIError>"
	}
	if e.Body.Code != "" {
		return fmt.Sprintf("cc: %s (http %d): %s", e.Body.Code, e.HTTPStatus, e.Body.Message)
	}
	if e.Body.Message != "" {
		return fmt.Sprintf("cc: http %d: %s", e.HTTPStatus, e.Body.Message)
	}
	return fmt.Sprintf("cc: http %d", e.HTTPStatus)
}

// IsAPIErrorCode returns true if err is an *APIError whose Body.Code
// matches code. Cheaper than errors.As + type-assert at call sites.
func IsAPIErrorCode(err error, code string) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Body.Code == code
}
