// Package proxy is the protocol-translation layer between
// OpenAI/Anthropic-compatible clients and Command Code's
// `/alpha/generate`. It also owns the canonical request shape that
// commit 5's pool/retry logic consumes.
package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
)

// Canonical is the protocol-neutral form of one client request. The
// OpenAI and Anthropic handlers translate their input into this
// structure, then hand it to BuildCCBody to produce the wire payload
// that gets POSTed to /alpha/generate.
type Canonical struct {
	Model     string
	Messages  []json.RawMessage // CC content-block format
	Tools     []json.RawMessage // CC tool schema format
	System    json.RawMessage   // string or content-block array; nil if absent
	MaxTokens int
	// ClientToken is the proxy bearer that authenticated the inbound
	// request. Used as an affinity factor when pool routing lands in
	// commit 5; today it only feeds the session id.
	ClientToken string
	// Protocol identifies which adapter produced this canonical form,
	// used for traffic-log tagging.
	Protocol string
}

// BuildCCBody renders the canonical request into the JSON document
// `/alpha/generate` expects. cache_control is always injected per
// plan.md §8 — the one line that unlocks ephemeral cache on every
// Go-tier model that supports it.
func BuildCCBody(c *Canonical) *cc.GenerateBody {
	return &cc.GenerateBody{
		Config:         defaultGenerateConfig(),
		Memory:         "",
		Taste:          "",
		Skills:         nil,
		PermissionMode: "standard",
		Params: cc.GenerateParams{
			Model:        c.Model,
			Messages:     c.Messages,
			Tools:        c.Tools,
			System:       c.System,
			MaxTokens:    c.MaxTokens,
			Stream:       true,
			CacheControl: cc.EphemeralCacheControl,
		},
	}
}

// defaultGenerateConfig fills in the `config` block of the wire payload
// with safe placeholders. The values are not interpreted by CC for
// generation, only logged on their side.
func defaultGenerateConfig() cc.GenerateConfig {
	return cc.GenerateConfig{
		WorkingDir:    "/",
		Date:          time.Now().UTC().Format("2006-01-02"),
		Environment:   runtime.GOOS + "-" + runtime.GOARCH,
		Structure:     []string{},
		IsGitRepo:     false,
		CurrentBranch: "",
		MainBranch:    "",
		GitStatus:     "",
		RecentCommits: []string{},
	}
}

// SessionID derives the x-session-id header. Plan §8: stable hash of
// (clientToken | model | prefix-of-messages) so the same conversation
// from the same client lands on the same upstream pod. Returns a
// 32-char hex prefix of sha256.
func SessionID(clientToken, model string, messagesPrefix []byte) string {
	h := sha256.New()
	h.Write([]byte(clientToken))
	h.Write([]byte{'|'})
	h.Write([]byte(model))
	h.Write([]byte{'|'})
	h.Write(messagesPrefix)
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)[:32]
}

// MessagesPrefix returns the bytes used to seed SessionID — every
// message except the most recent one. An empty result is fine (single
// turn) and produces a stable per-(client, model) bucket.
func MessagesPrefix(messages []json.RawMessage) []byte {
	if len(messages) <= 1 {
		return nil
	}
	var buf bytes.Buffer
	for _, m := range messages[:len(messages)-1] {
		buf.Write(m)
	}
	return buf.Bytes()
}
