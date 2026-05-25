package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
	"github.com/waylon256yhw/cmdgo/internal/store"
	"github.com/waylon256yhw/cmdgo/internal/web"
)

// DashboardService bundles dashboard render + REST handlers.
type DashboardService struct {
	Store       *store.Store
	Broadcaster *Broadcaster
	Logger      *slog.Logger
	Listen      string
	PublicURL   string

	templates *template.Template
	static    fs.FS
}

// NewDashboardService loads the embedded templates and static FS into
// the service.
func NewDashboardService(st *store.Store, b *Broadcaster, logger *slog.Logger, listen, publicURL string) *DashboardService {
	return &DashboardService{
		Store:       st,
		Broadcaster: b,
		Logger:      logger,
		Listen:      listen,
		PublicURL:   publicURL,
		templates:   web.Templates(),
		static:      web.Static(),
	}
}

// StaticFileServer serves the embedded /static/* assets.
func (d *DashboardService) StaticFileServer() http.Handler {
	return http.StripPrefix("/static/", http.FileServer(http.FS(d.static)))
}

// accountCardView is the per-card data the account_card template
// consumes. CreditsPct is a 0-100 number suitable for a CSS width;
// $10 corresponds to a full bar (Go-tier monthly credit allowance).
type accountCardView struct {
	store.Account
	CreditsPct int
}

func toAccountCard(a store.Account) accountCardView {
	pct := int(a.LastKnownCredits / 10.0 * 100)
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return accountCardView{Account: a, CreditsPct: pct}
}

// HandleIndex renders the dashboard shell. All live data is fetched
// by HTMX from /api/* in subsequent requests.
func (d *DashboardService) HandleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	base := d.PublicURL
	if base == "" {
		base = "http://" + d.Listen
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	data := map[string]any{
		"Title":        "dashboard",
		"Listen":       d.Listen,
		"DataPath":     d.Store.Path(),
		"Base":         base,
		"DefaultModel": cc.DefaultGoModel,
	}
	if err := d.templates.ExecuteTemplate(w, "layout", data); err != nil {
		d.Logger.Error("render dashboard", "err", err)
	}
}

// HandleAccounts renders the account-card grid (HTMX partial).
func (d *DashboardService) HandleAccounts(w http.ResponseWriter, r *http.Request) {
	snap := d.Store.Snapshot()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if len(snap.Accounts) == 0 {
		_, _ = w.Write([]byte(`
<div class="col-span-full panel p-10 text-center space-y-3">
  <div class="text-3xl">🛰️</div>
  <div class="text-cmd-text font-medium">No accounts yet</div>
  <div class="text-cmd-muted text-sm">Click <strong class="text-cmd-text">+ Add account</strong> to sign in with Command Code.</div>
</div>`))
		return
	}
	for _, acc := range snap.Accounts {
		if err := d.templates.ExecuteTemplate(w, "account_card", toAccountCard(acc)); err != nil {
			d.Logger.Error("render account card", "err", err, "account", acc.ID)
		}
	}
}

// HandleTraffic renders the most recent traffic rows.
func (d *DashboardService) HandleTraffic(w http.ResponseWriter, r *http.Request) {
	rows := d.Store.TrafficLog(50)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if len(rows) == 0 {
		_, _ = w.Write([]byte(`<tr><td colspan="10" class="px-4 py-6 text-center text-cmd-muted">No traffic yet. Send a request to <code>/v1/chat/completions</code>.</td></tr>`))
		return
	}
	for _, row := range rows {
		if err := d.templates.ExecuteTemplate(w, "traffic_row", row); err != nil {
			d.Logger.Error("render traffic row", "err", err)
		}
	}
}

// HandleEndpoints renders the endpoint cheat-sheet partial.
func (d *DashboardService) HandleEndpoints(w http.ResponseWriter, r *http.Request) {
	base := d.PublicURL
	if base == "" {
		base = "http://" + d.Listen
	}
	snap := d.Store.Snapshot()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = d.templates.ExecuteTemplate(w, "endpoints", map[string]any{
		"Base":         base,
		"AccountCount": len(snap.Accounts),
		"DefaultModel": cc.DefaultGoModel,
	})
}

// HandlePauseAccount toggles paused=true and re-renders the card.
func (d *DashboardService) HandlePauseAccount(w http.ResponseWriter, r *http.Request) {
	d.flipPaused(w, r, true)
}

// HandleResumeAccount toggles paused=false and re-renders the card.
func (d *DashboardService) HandleResumeAccount(w http.ResponseWriter, r *http.Request) {
	d.flipPaused(w, r, false)
}

func (d *DashboardService) flipPaused(w http.ResponseWriter, r *http.Request, paused bool) {
	id := r.PathValue("id")
	if err := d.Store.SetAccountPaused(id, paused); err != nil {
		if errors.Is(err, store.ErrAccountNotFound) {
			http.Error(w, "account not found", http.StatusNotFound)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "update_failed", err.Error())
		return
	}
	snap := d.Store.Snapshot()
	for _, acc := range snap.Accounts {
		if acc.ID == id {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_ = d.templates.ExecuteTemplate(w, "account_card", toAccountCard(acc))
			d.broadcastAccount(acc)
			return
		}
	}
	http.NotFound(w, r)
}

// HandleDeleteAccount removes the account; HTMX hx-swap="delete" pops
// the card from the DOM client-side.
func (d *DashboardService) HandleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := d.Store.DeleteAccount(id); err != nil {
		if errors.Is(err, store.ErrAccountNotFound) {
			http.Error(w, "account not found", http.StatusNotFound)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}
	d.Broadcaster.Broadcast(Event{Name: "account-update", Data: []byte("removed")})
	w.WriteHeader(http.StatusNoContent)
}

// HandleRotateToken issues a fresh proxy token and returns it as
// JSON. The browser is responsible for writing it into localStorage.
func (d *DashboardService) HandleRotateToken(w http.ResponseWriter, r *http.Request) {
	tok, err := d.Store.RotateProxyToken()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "rotate_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"proxyToken": tok})
}

// broadcastAccount renders a single account card and emits an
// account-update event to all SSE subscribers.
func (d *DashboardService) broadcastAccount(acc store.Account) {
	var buf bytes.Buffer
	if err := d.templates.ExecuteTemplate(&buf, "account_card", toAccountCard(acc)); err != nil {
		d.Logger.Warn("broadcast render", "err", err)
		return
	}
	d.Broadcaster.Broadcast(Event{Name: "account-update", Data: buf.Bytes()})
}

// BroadcastAccountSnapshot is callable from outside (e.g. credit sync)
// to refresh a card after a background mutation.
func (d *DashboardService) BroadcastAccountSnapshot(accountID string) {
	snap := d.Store.Snapshot()
	for _, acc := range snap.Accounts {
		if acc.ID == accountID {
			d.broadcastAccount(acc)
			return
		}
	}
}

// BroadcastTrafficRow renders a single traffic entry and emits a
// traffic-row event. Called by the proxy adapters after every
// request completes.
func (d *DashboardService) BroadcastTrafficRow(entry store.TrafficEntry) {
	var buf bytes.Buffer
	if err := d.templates.ExecuteTemplate(&buf, "traffic_row", entry); err != nil {
		d.Logger.Warn("broadcast traffic render", "err", err)
		return
	}
	d.Broadcaster.Broadcast(Event{Name: "traffic-row", Data: buf.Bytes()})
}

// RecordTraffic implements proxy.TrafficRecorder: persists the entry
// in the rolling 500-row log and broadcasts the rendered row to all
// connected dashboards.
func (d *DashboardService) RecordTraffic(entry store.TrafficEntry) {
	if entry.TS.IsZero() {
		entry.TS = time.Now().UTC()
	}
	if err := d.Store.AppendTrafficLog(entry); err != nil {
		d.Logger.Warn("persist traffic", "err", err)
	}
	d.BroadcastTrafficRow(entry)
}

// MarshalForLogs returns a pretty JSON dump of an account suitable
// for slog. Not currently used; preserved for diagnostics.
func MarshalForLogs(v any) string {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("<marshal failed: %v>", err)
	}
	return string(raw)
}

var _ = time.Now // keep time import alive for future relative-time helpers
