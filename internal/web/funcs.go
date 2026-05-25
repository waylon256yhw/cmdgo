package web

import (
	"fmt"
	"html/template"
	"strings"
	"time"
)

// funcMap returns the template helpers used by dashboard.html and the
// partials.
func funcMap() template.FuncMap {
	return template.FuncMap{
		"relTime":   relTime,
		"shortID":   shortID,
		"usd":       usd,
		"tokens":    tokens,
		"statusCls": statusCls,
		"upper":     strings.ToUpper,
	}
}

// relTime renders t as "12s ago", "5m ago", "2h ago", or absolute
// (YYYY-MM-DD) for anything older than a day.
func relTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	delta := time.Since(t)
	switch {
	case delta < time.Second:
		return "just now"
	case delta < time.Minute:
		return fmt.Sprintf("%ds ago", int(delta.Seconds()))
	case delta < time.Hour:
		return fmt.Sprintf("%dm ago", int(delta.Minutes()))
	case delta < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(delta.Hours()))
	default:
		return t.Format("2006-01-02")
	}
}

// shortID truncates an opaque UUID to 8 hex chars for compact display.
func shortID(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// usd formats a USD amount with 4 decimal places — enough resolution
// for Go-tier traffic where individual requests cost fractions of a
// cent.
func usd(n float64) string {
	return fmt.Sprintf("$%.4f", n)
}

// tokens formats integers with thousands separators (1234 → "1,234").
func tokens(n int) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	leading := len(s) % 3
	if leading == 0 {
		leading = 3
	}
	out = append(out, s[:leading]...)
	for i := leading; i < len(s); i += 3 {
		out = append(out, ',')
		out = append(out, s[i:i+3]...)
	}
	return string(out)
}

// statusCls maps an HTTP status code to a Tailwind colour class for
// the traffic-row badge.
func statusCls(status int) string {
	switch {
	case status == 0:
		return "bg-zinc-700 text-zinc-300"
	case status < 300:
		return "bg-emerald-500/20 text-emerald-300"
	case status < 400:
		return "bg-sky-500/20 text-sky-300"
	case status < 500:
		return "bg-amber-500/20 text-amber-300"
	default:
		return "bg-rose-500/20 text-rose-300"
	}
}
