package store

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
)

// ProxyTokenPrefix is the human-recognisable prefix every proxy bearer
// token starts with. Lets accidental pastes (in logs, dashboards, browser
// URLs) be detected at a glance.
const ProxyTokenPrefix = "pcc_"

// GenerateProxyToken returns a fresh, cryptographically random proxy
// token of the form `pcc_<base64url(24 bytes)>` (~32 chars after the
// prefix).
func GenerateProxyToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return ProxyTokenPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// EnsureProxyToken returns the proxy token persisted in state. If none is
// stored yet (first boot, or the user manually cleared the field) a new
// one is generated and saved. The bool result reports whether a new
// token was generated this call — main uses it to decide whether to
// print the token to stderr.
func (s *Store) EnsureProxyToken() (token string, created bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if looksLikeProxyToken(s.st.ProxyToken) {
		return s.st.ProxyToken, false, nil
	}
	tok, gerr := GenerateProxyToken()
	if gerr != nil {
		return "", false, gerr
	}
	prev := s.st.ProxyToken
	s.st.ProxyToken = tok
	if perr := s.persistLocked(); perr != nil {
		s.st.ProxyToken = prev
		return "", false, perr
	}
	return tok, true, nil
}

// RotateProxyToken forces generation of a brand-new token, replacing the
// previously persisted one. Used by `POST /api/proxy-token/rotate` in
// commit 6.
func (s *Store) RotateProxyToken() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok, err := GenerateProxyToken()
	if err != nil {
		return "", err
	}
	prev := s.st.ProxyToken
	s.st.ProxyToken = tok
	if err := s.persistLocked(); err != nil {
		s.st.ProxyToken = prev
		return "", err
	}
	return tok, nil
}

func looksLikeProxyToken(s string) bool {
	return strings.HasPrefix(s, ProxyTokenPrefix) && len(s) > len(ProxyTokenPrefix)+8
}
