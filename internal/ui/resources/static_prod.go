//go:build !dev

package resources

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

// Handler returns an HTTP handler for serving static files.
// In production mode, files are embedded in the binary.
func Handler() http.Handler {
	fsys, _ := fs.Sub(staticFS, "static")
	return http.StripPrefix("/static/", http.FileServer(http.FS(fsys)))
}

// StaticPath returns the URL path for a static asset.
func StaticPath(path string) string {
	return "/static/" + path
}
