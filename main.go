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

	"github.com/waylon256yhw/cmdgo/internal/config"
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

	mux := newMux(st)

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

func newMux(st *store.Store) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	v1Auth := server.RequireProxyToken(st)
	mux.Handle("POST /v1/chat/completions", v1Auth(http.HandlerFunc(notImplemented)))
	mux.Handle("POST /v1/messages", v1Auth(http.HandlerFunc(notImplemented)))
	// /v1/models is public (clients introspect before authenticating).
	mux.HandleFunc("GET /v1/models", notImplemented)

	return mux
}

func notImplemented(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = w.Write([]byte(`{"error":{"type":"not_implemented","message":"endpoint wiring lands in a later commit (M0 scaffold)"}}` + "\n"))
}
