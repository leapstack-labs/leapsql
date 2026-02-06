//go:build dev

package resources

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

// getStaticDir derives the absolute path to the static directory
// relative to this source file, regardless of where the binary is run from.
func getStaticDir() string {
	// runtime.Caller(0) returns the path to this specific file (static_dev.go)
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		// Fallback if something goes wrong (rare)
		return StaticDirectoryPath
	}
	// static_dev.go is in internal/ui/resources/, static/ is a sibling directory
	return filepath.Join(filepath.Dir(filename), "static")
}

// Handler returns an HTTP handler for serving static files.
// In dev mode, files are served directly from the filesystem for hot reloading.
func Handler() http.Handler {
	staticDir := getStaticDir()
	slog.Info("static assets served from filesystem", "path", staticDir)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow browser caching with validation (Last-Modified/If-Modified-Since)
		// Use Cmd+Shift+R to force refresh when needed
		http.StripPrefix("/static/", http.FileServer(http.FS(os.DirFS(staticDir)))).ServeHTTP(w, r)
	})
}

// StaticPath returns the URL path for a static asset.
func StaticPath(path string) string {
	return "/static/" + path
}
