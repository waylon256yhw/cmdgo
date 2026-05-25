// Package config parses CLI flags and environment overrides into a single
// Config value consumed by main and the various subsystems. There are no
// secrets here — every field is observable in `cmdgo --help`.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// Config is the resolved runtime configuration for one cmdgo process.
type Config struct {
	// Listen is the address the HTTP server binds. Defaults to a localhost
	// port so the proxy is private by default.
	Listen string

	// DataPath is the JSON state file persisting accounts, settings, proxy
	// token, and the rolling traffic log.
	DataPath string

	// PublicURL is the externally-visible URL when cmdgo is fronted by a
	// reverse proxy / tunnel. Used to build OAuth callback URLs. Empty
	// means "derive from Listen".
	PublicURL string

	// CCBaseURL overrides the Command Code API root. Defaults to the
	// production URL embedded in cc.New. Mainly used by integration
	// tests that point cmdgo at an httptest server.
	CCBaseURL string
}

// Load parses args (typically os.Args[1:]) plus environment variables. The
// precedence is flags > env > defaults.
func Load(args []string) (*Config, error) {
	fs := flag.NewFlagSet("cmdgo", flag.ContinueOnError)
	listen := fs.String("listen", envOr("CMDGO_LISTEN", "127.0.0.1:8080"), "listen address (host:port)")
	dataPath := fs.String("data", envOr("CMDGO_DATA", defaultDataPath()), "JSON state file path")
	publicURL := fs.String("public-url", envOr("CMDGO_PUBLIC_URL", ""), "public URL (when fronted by a reverse proxy)")
	ccBaseURL := fs.String("cc-base-url", envOr("CMDGO_CC_BASE_URL", ""), "Command Code API base URL (override; testing only)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *listen == "" {
		return nil, errors.New("--listen must not be empty")
	}
	if *dataPath == "" {
		return nil, errors.New("--data must not be empty")
	}
	abs, err := filepath.Abs(*dataPath)
	if err != nil {
		return nil, fmt.Errorf("resolve --data: %w", err)
	}
	return &Config{
		Listen:    *listen,
		DataPath:  abs,
		PublicURL: *publicURL,
		CCBaseURL: *ccBaseURL,
	}, nil
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func defaultDataPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "cmdgo-state.json"
	}
	return filepath.Join(home, ".cmdgo", "state.json")
}
