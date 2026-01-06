// Package ui provides a web-based development UI for LeapSQL.
package ui

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/internal/ui/router"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"golang.org/x/sync/errgroup"
)

// Server is the main UI server.
type Server struct {
	engine       *engine.Engine
	store        core.Store
	sessionStore *sessions.CookieStore
	port         int
	watch        bool
	modelsDir    string
	logger       *slog.Logger
	notifier     *notifier.Notifier
}

// Config holds configuration for the UI server.
type Config struct {
	Engine        *engine.Engine
	Store         core.Store
	Port          int
	Watch         bool
	SessionSecret string
	Logger        *slog.Logger
	ModelsDir     string
}

// NewServer creates a new UI server instance.
func NewServer(cfg Config) *Server {
	sessionStore := sessions.NewCookieStore([]byte(cfg.SessionSecret))
	sessionStore.MaxAge(86400 * 30) // 30 days
	sessionStore.Options.Path = "/"
	sessionStore.Options.HttpOnly = true
	sessionStore.Options.SameSite = http.SameSiteLaxMode

	return &Server{
		engine:       cfg.Engine,
		store:        cfg.Store,
		sessionStore: sessionStore,
		port:         cfg.Port,
		watch:        cfg.Watch,
		modelsDir:    cfg.ModelsDir,
		logger:       cfg.Logger,
		notifier:     notifier.New(),
	}
}

// Serve starts the UI server and blocks until the context is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	s.logger.Info("starting UI server", "addr", fmt.Sprintf("http://localhost:%d", s.port))

	eg, egctx := errgroup.WithContext(ctx)

	r := chi.NewMux()
	r.Use(
		middleware.Logger,
		middleware.Recoverer,
		middleware.Compress(5),
	)

	if err := router.SetupRoutes(r, s.engine, s.store, s.sessionStore, s.notifier, s.IsDev()); err != nil {
		return fmt.Errorf("failed to setup routes: %w", err)
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
		BaseContext: func(_ net.Listener) context.Context {
			return egctx
		},
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start file watcher if enabled
	if s.watch {
		eg.Go(func() error {
			return s.watchFiles(egctx)
		})
	}

	// Start HTTP server
	eg.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("server error: %w", err)
		}
		return nil
	})

	// Graceful shutdown
	eg.Go(func() error {
		<-egctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s.logger.Debug("shutting down UI server...")
		return srv.Shutdown(shutdownCtx)
	})

	return eg.Wait()
}

// IsDev returns true if running in development mode.
func (s *Server) IsDev() bool {
	// Can be determined by build tag or config
	return true // For now, always dev mode
}

// Notifier returns the server's notifier for SSE updates.
func (s *Server) Notifier() *notifier.Notifier {
	return s.notifier
}

// watchFiles watches for file changes in the models directory.
func (s *Server) watchFiles(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()

	// Watch models directory recursively
	if err := watchDirRecursive(watcher, s.modelsDir); err != nil {
		s.logger.Error("failed to watch models directory", "error", err)
		// Don't fail - continue without watching
	}

	// Debounce timer
	var debounceTimer *time.Timer

	for {
		select {
		case <-ctx.Done():
			return nil

		case event := <-watcher.Events:
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			ext := filepath.Ext(event.Name)
			if ext != ".sql" && ext != ".star" {
				continue
			}

			// Debounce
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(100*time.Millisecond, func() {
				s.logger.Debug("file changed, re-discovering", "file", event.Name)

				// Re-discover models
				if _, err := s.engine.Discover(engine.DiscoveryOptions{}); err != nil {
					s.logger.Error("discover failed", "error", err)
				}

				// Notify all SSE clients
				s.notifyClients()
			})

		case err := <-watcher.Errors:
			s.logger.Error("watcher error", "error", err)
		}
	}
}

// notifyClients sends a notification to all connected SSE clients.
func (s *Server) notifyClients() {
	s.notifier.Broadcast()
}

// watchDirRecursive adds a directory and all subdirectories to the watcher.
func watchDirRecursive(watcher *fsnotify.Watcher, dir string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return watcher.Add(path)
		}
		return nil
	})
}
