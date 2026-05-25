package cc

import (
	"context"
	"errors"
	"net/http"
)

// Whoami calls `GET /alpha/whoami` and returns the user identity.
// Use it to validate apikeys captured via OAuth or paste-key.
func (c *Client) Whoami(ctx context.Context, apikey string) (*User, error) {
	var env struct {
		Success bool `json:"success"`
		User    User `json:"user"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/alpha/whoami", apikey, nil, &env); err != nil {
		return nil, err
	}
	if !env.Success {
		return nil, errors.New("cc: whoami returned success=false")
	}
	if env.User.ID == "" {
		return nil, errors.New("cc: whoami returned empty user id")
	}
	return &env.User, nil
}

// BillingCredits calls `GET /alpha/billing/credits`. Used to seed the
// LastKnownCredits field when an account is added, and by the 60s
// background poller in commit 6.
func (c *Client) BillingCredits(ctx context.Context, apikey string) (*Credits, error) {
	var env struct {
		Credits Credits `json:"credits"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/alpha/billing/credits", apikey, nil, &env); err != nil {
		return nil, err
	}
	return &env.Credits, nil
}
