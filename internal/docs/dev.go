// Package docs provides development server with watch mode for the documentation site.
package docs

import (
	"bytes"
	"context"
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

// DevServer provides a development server with watch mode and live reload.
type DevServer struct {
	generator   *Generator
	modelsDir   string
	port        int
	docsDir     string
	mu          sync.RWMutex
	currentHTML []byte
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

// rebuild regenerates the HTML output.
func (s *DevServer) rebuild(reloadModels bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reload models if they changed
	if reloadModels {
		s.generator = NewGenerator(s.generator.projectName)
		if err := s.generator.LoadModels(s.modelsDir); err != nil {
			return fmt.Errorf("failed to reload models: %w", err)
		}
	}

	// Build frontend (non-minified for dev)
	buildResult, err := BuildFrontend(s.docsDir, false)
	if err != nil {
		return fmt.Errorf("failed to build frontend: %w", err)
	}

	// Generate catalog
	catalog := s.generator.GenerateCatalog()
	catalogJSON, err := json.Marshal(catalog)
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	// Parse template
	tmpl, err := template.New("docs").Parse(htmlTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	// Inject live reload script
	jsWithReload := buildResult.JS + liveReloadScript

	data := templateData{
		ProjectName: s.generator.projectName,
		CSS:         template.CSS(buildResult.CSS), //nolint:gosec // G203: trusted build output
		JS:          template.JS(jsWithReload),     //nolint:gosec // G203: trusted build output
		CatalogJSON: template.JS(catalogJSON),      //nolint:gosec // G203: trusted build output
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	s.currentHTML = buf.Bytes()
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
