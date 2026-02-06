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
	fileServer := http.FileServer(http.FS(fsys))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Cache embedded static assets for 1 year (they never change in prod)
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		http.StripPrefix("/static/", fileServer).ServeHTTP(w, r)
	})
}

// StaticPath returns the URL path for a static asset.
func StaticPath(path string) string {
	return "/static/" + path
}
