package cc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Default header values cmdgo sends pretending to be the cmd CLI. The
// upstream rejects requests missing x-command-code-version, so these are
// not cosmetic.
const (
	DefaultCLIVersion    = "0.24.1"
	DefaultCLIEnv        = "production"
	DefaultProjectSlug   = "cmdgo"
	DefaultTasteLearning = "false"
	DefaultCoFlag        = "false"
)

// Client talks to https://api.commandcode.ai. It is safe to share
// across goroutines.
type Client struct {
	baseURL     string
	hc          *http.Client
	version     string
	env         string
	projectSlug string
}

// New returns a Client configured for production CC.
func New() *Client {
	return NewWithBaseURL(baseURL)
}

// NewWithBaseURL lets tests redirect to an httptest server.
func NewWithBaseURL(url string) *Client {
	return &Client{
		baseURL:     url,
		version:     DefaultCLIVersion,
		env:         DefaultCLIEnv,
		projectSlug: DefaultProjectSlug,
		hc: &http.Client{
			// No top-level timeout — /alpha/generate streams can run for
			// many minutes. Per-call deadlines come via context.
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   true,
			},
		},
	}
}

// SetProjectSlug overrides the x-project-slug header (visible in CC's
// admin UI). Useful for tagging traffic.
func (c *Client) SetProjectSlug(s string) {
	if s != "" {
		c.projectSlug = s
	}
}

func (c *Client) applyCommonHeaders(req *http.Request, apikey string) {
	req.Header.Set("Authorization", "Bearer "+apikey)
	req.Header.Set("x-command-code-version", c.version)
	req.Header.Set("x-cli-environment", c.env)
}

// doJSON runs a request that returns a small JSON document (whoami,
// usage, billing). 4xx/5xx return *APIError.
func (c *Client) doJSON(ctx context.Context, method, path, apikey string, body []byte, out any) error {
	var br io.Reader
	if body != nil {
		br = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, br)
	if err != nil {
		return err
	}
	c.applyCommonHeaders(req, apikey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("cc: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeAPIError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("cc: decode %s %s: %w", method, path, err)
	}
	return nil
}

// Generate opens a streaming `/alpha/generate` POST. The caller owns
// the response body and must Close it; use cc.NewScanner(resp.Body) to
// iterate SSE events.
//
// On non-2xx, the response body is consumed and a typed *APIError is
// returned instead so callers do not have to inspect status codes
// twice.
type GenerateOpts struct {
	APIKey    string
	SessionID string // value of the x-session-id header
	Body      *GenerateBody
}

func (c *Client) Generate(ctx context.Context, opts GenerateOpts) (*http.Response, error) {
	if opts.APIKey == "" {
		return nil, errors.New("cc: Generate requires APIKey")
	}
	if opts.Body == nil {
		return nil, errors.New("cc: Generate requires Body")
	}
	raw, err := json.Marshal(opts.Body)
	if err != nil {
		return nil, fmt.Errorf("cc: marshal generate body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/alpha/generate", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	c.applyCommonHeaders(req, opts.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("x-project-slug", c.projectSlug)
	req.Header.Set("x-taste-learning", DefaultTasteLearning)
	req.Header.Set("x-co-flag", DefaultCoFlag)
	if opts.SessionID != "" {
		req.Header.Set("x-session-id", opts.SessionID)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cc: generate: %w", err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, decodeAPIError(resp)
	}
	return resp, nil
}

func decodeAPIError(resp *http.Response) error {
	limited := io.LimitReader(resp.Body, 64<<10)
	raw, _ := io.ReadAll(limited)
	ae := &APIError{HTTPStatus: resp.StatusCode}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, ae)
	}
	if ae.Body.Message == "" {
		ae.Body.Message = http.StatusText(resp.StatusCode)
	}
	if ae.Body.Status == 0 {
		ae.Body.Status = resp.StatusCode
	}
	return ae
}
