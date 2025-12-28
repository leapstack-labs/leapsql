// Package docs provides development server with watch mode for the documentation site.
package docs

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// QueryRequest represents a SQL query request from the frontend.
type QueryRequest struct {
	SQL    string        `json:"sql"`
	Params []interface{} `json:"params"`
}

// QueryResponse represents the response format matching sql.js.
type QueryResponse struct {
	Columns []string        `json:"columns"`
	Values  [][]interface{} `json:"values"`
}

// DevServer provides a development server with watch mode and live reload.
type DevServer struct {
	generator   *Generator
	modelsDir   string
	port        int
	docsDir     string
	mu          sync.RWMutex
	currentHTML []byte
	manifest    *Manifest
	metaDB      *MetadataDB
	clients     map[chan struct{}]struct{}
	clientsMu   sync.Mutex
}

// NewDevServer creates a new development server.
func NewDevServer(projectName, modelsDir string, port int) (*DevServer, error) {
	docsDir, err := GetDocsDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get docs directory: %w", err)
	}

	gen := NewGenerator(projectName)
	if err := gen.LoadModels(modelsDir); err != nil {
		return nil, fmt.Errorf("failed to load models: %w", err)
	}

	return &DevServer{
		generator: gen,
		modelsDir: modelsDir,
		port:      port,
		docsDir:   docsDir,
		clients:   make(map[chan struct{}]struct{}),
	}, nil
}

// Serve starts the development server with watch mode.
func (s *DevServer) Serve(ctx context.Context) error {
	// Initial build
	if err := s.rebuild(false); err != nil {
		return fmt.Errorf("initial build failed: %w", err)
	}

	// Start file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer func() { _ = watcher.Close() }()

	// Watch models directory
	if err := s.watchDir(watcher, s.modelsDir); err != nil {
		return fmt.Errorf("failed to watch models dir: %w", err)
	}

	// Watch source directory
	srcDir := filepath.Join(s.docsDir, "src")
	if err := s.watchDir(watcher, srcDir); err != nil {
		return fmt.Errorf("failed to watch src dir: %w", err)
	}

	// Start watcher goroutine
	go s.watchLoop(ctx, watcher)

	// Set up HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/__reload", s.handleSSE)
	mux.HandleFunc("/query", s.handleQuery)
	mux.HandleFunc("/manifest", s.handleManifest)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Handle shutdown gracefully
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		// Clean up database
		s.mu.Lock()
		if s.metaDB != nil {
			_ = s.metaDB.Close()
		}
		s.mu.Unlock()
	}()

	log.Printf("Dev server running at http://localhost:%d", s.port)
	log.Printf("Watching for changes in:")
	log.Printf("  - %s (models)", s.modelsDir)
	log.Printf("  - %s (source)", srcDir)
	log.Println("Press Ctrl+C to stop")

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// watchDir recursively adds a directory to the watcher.
func (s *DevServer) watchDir(watcher *fsnotify.Watcher, dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Skip node_modules and hidden directories
			if info.Name() == "node_modules" || (len(info.Name()) > 0 && info.Name()[0] == '.') {
				return filepath.SkipDir
			}
			return watcher.Add(path)
		}
		return nil
	})
}

// watchLoop handles file system events.
func (s *DevServer) watchLoop(ctx context.Context, watcher *fsnotify.Watcher) {
	// Debounce timer
	var debounceTimer *time.Timer
	var debounceModels bool

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Only handle write/create events for relevant files
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			ext := filepath.Ext(event.Name)
			isSource := ext == ".tsx" || ext == ".ts" || ext == ".css"
			isModel := ext == ".sql"

			if !isSource && !isModel {
				continue
			}

			// Track if models changed
			if isModel {
				debounceModels = true
			}

			// Debounce rebuilds
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			reloadModels := debounceModels
			debounceTimer = time.AfterFunc(100*time.Millisecond, func() {
				log.Printf("Change detected: %s", filepath.Base(event.Name))
				if err := s.rebuild(reloadModels); err != nil {
					log.Printf("Rebuild error: %v", err)
				} else {
					s.notifyClients()
				}
				debounceModels = false
			})

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Watcher error: %v", err)
		}
	}
}

// rebuild regenerates the HTML output and database.
// Uses atomic swap pattern: all expensive work happens outside the lock,
// then we atomically swap pointers to avoid race conditions.
func (s *DevServer) rebuild(reloadModels bool) error {
	// =========================================================================
	// Phase 1: Build all new state OUTSIDE the lock (can take 500ms+)
	// =========================================================================

	// Get current generator (need lock for read)
	s.mu.RLock()
	gen := s.generator
	s.mu.RUnlock()

	// Reload models if they changed (creates new generator)
	if reloadModels {
		gen = NewGenerator(gen.projectName)
		if err := gen.LoadModels(s.modelsDir); err != nil {
			return fmt.Errorf("failed to reload models: %w", err)
		}
	}

	// Build frontend (non-minified for dev) - slow operation
	buildResult, err := BuildFrontend(s.docsDir, false)
	if err != nil {
		return fmt.Errorf("failed to build frontend: %w", err)
	}

	// Generate catalog
	catalog := gen.GenerateCatalog()

	// Generate new manifest
	newManifest := GenerateManifest(catalog)

	// Build new in-memory database
	newDB, err := OpenMemoryDB()
	if err != nil {
		return fmt.Errorf("failed to open memory database: %w", err)
	}

	// Initialize schema and populate (if this fails, clean up newDB)
	if err := newDB.InitSchema(); err != nil {
		_ = newDB.Close()
		return fmt.Errorf("failed to init schema: %w", err)
	}
	if err := newDB.PopulateFromCatalog(catalog); err != nil {
		_ = newDB.Close()
		return fmt.Errorf("failed to populate database: %w", err)
	}

	// Marshal manifest to JSON for embedding
	manifestJSON, err := json.Marshal(newManifest)
	if err != nil {
		_ = newDB.Close()
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Parse template
	tmpl, err := template.New("docs").Parse(htmlTemplateV2)
	if err != nil {
		_ = newDB.Close()
		return fmt.Errorf("failed to parse template: %w", err)
	}

	// Inject live reload script
	jsWithReload := buildResult.JS + liveReloadScript

	data := templateDataV2{
		ProjectName:  gen.projectName,
		CSS:          template.CSS(buildResult.CSS), //nolint:gosec // G203: trusted build output
		JS:           template.JS(jsWithReload),     //nolint:gosec // G203: trusted build output
		ManifestJSON: template.JS(manifestJSON),     //nolint:gosec // G203: trusted build output
		DevMode:      true,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		_ = newDB.Close()
		return fmt.Errorf("failed to execute template: %w", err)
	}
	newHTML := buf.Bytes()

	// =========================================================================
	// Phase 2: Atomic swap (nanoseconds under lock)
	// =========================================================================
	s.mu.Lock()
	oldDB := s.metaDB
	s.metaDB = newDB
	s.manifest = newManifest
	s.currentHTML = newHTML
	s.generator = gen
	s.mu.Unlock()

	// =========================================================================
	// Phase 3: Cleanup old state OUTSIDE the lock
	// =========================================================================
	if oldDB != nil {
		if err := oldDB.Close(); err != nil {
			// Log but don't fail - new state is already live and working
			log.Printf("Warning: failed to close old database: %v", err)
		}
	}

	log.Println("Rebuild complete")
	return nil
}

// handleIndex serves the current HTML.
func (s *DevServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}

	s.mu.RLock()
	html := s.currentHTML
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	_, _ = w.Write(html)
}

// handleQuery executes SQL queries against the in-memory database.
func (s *DevServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Hold read lock for entire query operation to prevent DB from being
	// closed mid-query during a rebuild. This is safe for a dev server with
	// low query concurrency - rebuilds will just wait briefly for queries.
	s.mu.RLock()
	defer s.mu.RUnlock()

	db := s.metaDB
	if db == nil {
		http.Error(w, "Database not initialized", http.StatusServiceUnavailable)
		return
	}

	// Execute query with parameter binding (safe from injection)
	rows, err := db.DB().QueryContext(r.Context(), req.SQL, req.Params...)
	if err != nil {
		http.Error(w, "Query error: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer func() { _ = rows.Close() }()

	// Convert to {columns, values} format matching sql.js
	resp, err := rowsToQueryResponse(rows)
	if err != nil {
		http.Error(w, "Failed to process results: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleManifest returns the manifest JSON.
func (s *DevServer) handleManifest(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	manifest := s.manifest
	s.mu.RUnlock()

	if manifest == nil {
		http.Error(w, "Manifest not initialized", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(manifest)
}

// rowsToQueryResponse converts sql.Rows to QueryResponse format.
func rowsToQueryResponse(rows *sql.Rows) (*QueryResponse, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var values [][]interface{}

	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columnPtrs := make([]interface{}, len(columns))
	columnValues := make([]interface{}, len(columns))
	for i := range columnValues {
		columnPtrs[i] = &columnValues[i]
	}

	for rows.Next() {
		if err := rows.Scan(columnPtrs...); err != nil {
			return nil, err
		}

		// Copy values to a new slice
		row := make([]interface{}, len(columns))
		for i, val := range columnValues {
			// Handle SQL null values
			switch v := val.(type) {
			case nil:
				row[i] = nil
			case []byte:
				row[i] = string(v)
			default:
				row[i] = v
			}
		}
		values = append(values, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &QueryResponse{
		Columns: columns,
		Values:  values,
	}, nil
}

// handleSSE handles Server-Sent Events for live reload.
func (s *DevServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Create channel for this client
	ch := make(chan struct{}, 1)
	s.clientsMu.Lock()
	s.clients[ch] = struct{}{}
	s.clientsMu.Unlock()

	defer func() {
		s.clientsMu.Lock()
		delete(s.clients, ch)
		s.clientsMu.Unlock()
		close(ch)
	}()

	// Send initial ping
	_, _ = fmt.Fprintf(w, "data: connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ch:
			_, _ = fmt.Fprintf(w, "data: reload\n\n")
			flusher.Flush()
		}
	}
}

// notifyClients sends reload signal to all connected clients.
func (s *DevServer) notifyClients() {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	for ch := range s.clients {
		select {
		case ch <- struct{}{}:
		default:
			// Channel full, skip
		}
	}
}

// liveReloadScript is injected into the page for dev mode.
const liveReloadScript = `
;(function() {
  var es = new EventSource('/__reload');
  es.onmessage = function(e) {
    if (e.data === 'reload') {
      console.log('[dev] Reloading...');
      window.location.reload();
    }
  };
  es.onerror = function() {
    console.log('[dev] Connection lost, reconnecting...');
    setTimeout(function() { window.location.reload(); }, 1000);
  };
})();
`

// templateDataV2 holds data for the new HTML template with manifest.
type templateDataV2 struct {
	ProjectName  string
	CSS          template.CSS
	JS           template.JS
	ManifestJSON template.JS
	DevMode      bool
}

// htmlTemplateV2 is the new template that uses manifest instead of full catalog.
const htmlTemplateV2 = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{.ProjectName}} - Documentation</title>
  <style>{{.CSS}}</style>
</head>
<body>
  <div id="app">
    <div class="loading">Loading...</div>
  </div>
  <!-- Manifest for instant shell render -->
  <script>
    window.__MANIFEST__ = {{.ManifestJSON}};
    window.__DEV_MODE__ = {{.DevMode}};
  </script>
  <script>{{.JS}}</script>
</body>
</html>
`

// ServeDev is a convenience function to start the dev server.
func ServeDev(projectName, modelsDir string, port int) error {
	server, err := NewDevServer(projectName, modelsDir, port)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nShutting down...")
		cancel()
	}()

	return server.Serve(ctx)
}
