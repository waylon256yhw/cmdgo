// Package web owns the embedded dashboard assets — HTMX/Alpine/Tailwind
// (CDN), the Go html/template files, and a small custom stylesheet.
// The binary is self-contained: nothing on disk to install.
package web

import (
	"embed"
	"html/template"
	"io/fs"
)

//go:embed templates/*.html templates/partials/*.html static/*
var assets embed.FS

// Templates parses all *.html under templates/ and templates/partials/
// into one Template tree so {{template "name"}} works across the
// whole bundle. Returns a usable *template.Template or panics — the
// templates ship with the binary, so a parse failure is a build-time
// bug.
func Templates() *template.Template {
	tmpl := template.New("cmdgo").Funcs(funcMap())
	tmpl = template.Must(tmpl.ParseFS(assets,
		"templates/*.html",
		"templates/partials/*.html",
	))
	return tmpl
}

// Static returns the read-only filesystem for /static/* assets.
func Static() fs.FS {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
	return sub
}
