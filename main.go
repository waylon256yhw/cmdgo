// Command cmdgo is a reverse proxy that exposes Command Code Go-tier
// accounts through OpenAI/Anthropic-compatible APIs. See docs/plan.md
// for the full design and docs/cc-api.md for the upstream reference.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/config"
	"github.com/waylon256yhw/cmdgo/internal/proxy"
	"github.com/waylon256yhw/cmdgo/internal/server"
	"github.com/waylon256yhw/cmdgo/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "cmdgo: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Load(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	st, err := store.New(cfg.DataPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}

	token, created, err := st.EnsureProxyToken()
	if err != nil {
		return fmt.Errorf("ensure proxy token: %w", err)
	}
	if created {
		fmt.Fprintf(os.Stderr, "cmdgo: generated new proxy token — save this, it is the only copy:\n  %s\n", token)
	} else {
		fmt.Fprintf(os.Stderr, "cmdgo: proxy token loaded from %s\n", cfg.DataPath)
	}

	ccClient := cc.New()
	if cfg.CCBaseURL != "" {
		ccClient = cc.NewWithBaseURL(cfg.CCBaseURL)
		logger.Info("cc base url override", "url", cfg.CCBaseURL)
	}
	oauth := &server.OAuthService{
		Store:     st,
		CC:        ccClient,
		States:    cc.NewStateStore(0),
		Logger:    logger,
		Listen:    cfg.Listen,
		PublicURL: cfg.PublicURL,
	}
	openai := &proxy.OpenAIHandler{Store: st, CC: ccClient, Logger: logger}
	anthropic := &proxy.AnthropicHandler{Store: st, CC: ccClient, Logger: logger}
	models := &proxy.ModelsHandler{}

	mux := newMux(st, oauth, openai, anthropic, models)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           server.WithAccessLog(logger)(mux),
		ReadHeaderTimeout: 15 * time.Second,
		// No WriteTimeout or IdleTimeout — both would clip in-flight SSE
		// streams. Per-request lifecycle is governed by the request
		// context (graceful shutdown drains it).
		BaseContext: func(net.Listener) context.Context { return context.Background() },
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening",
			"addr", cfg.Listen,
			"data", cfg.DataPath,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err, ok := <-errCh:
		if ok && err != nil {
			return err
		}
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

func newMux(st *store.Store, oauth *server.OAuthService, openai *proxy.OpenAIHandler, anthropic *proxy.AnthropicHandler, models *proxy.ModelsHandler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	requireBearer := server.RequireProxyToken(st)

	// /v1/* — proxy traffic. Chat completions / messages are behind
	// bearer; the models list is public so SDKs can introspect before
	// they hold the proxy token.
	mux.Handle("POST /v1/chat/completions", requireBearer(openai))
	mux.Handle("POST /v1/messages", requireBearer(anthropic))
	mux.Handle("GET /v1/models", models)

	// /callback — public, CC Studio POSTs here from the user's browser.
	mux.HandleFunc("POST /callback", oauth.HandleOAuthCallback)
	mux.HandleFunc("OPTIONS /callback", oauth.HandleOAuthPreflight)

	// /api/oauth/* — dashboard-driven, behind bearer.
	mux.Handle("POST /api/oauth/start", requireBearer(http.HandlerFunc(oauth.HandleOAuthStart)))
	mux.Handle("POST /api/oauth/paste-key", requireBearer(http.HandlerFunc(oauth.HandlePasteKey)))

	return mux
}
