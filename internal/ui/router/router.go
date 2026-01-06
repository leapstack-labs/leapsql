// Package router sets up HTTP routes for the UI server.
package router

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	databaseFeature "github.com/leapstack-labs/leapsql/internal/ui/features/database"
	explorerFeature "github.com/leapstack-labs/leapsql/internal/ui/features/explorer"
	graphFeature "github.com/leapstack-labs/leapsql/internal/ui/features/graph"
	homeFeature "github.com/leapstack-labs/leapsql/internal/ui/features/home"
	modelsFeature "github.com/leapstack-labs/leapsql/internal/ui/features/models"
	runsFeature "github.com/leapstack-labs/leapsql/internal/ui/features/runs"
	statequeryFeature "github.com/leapstack-labs/leapsql/internal/ui/features/statequery"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/internal/ui/resources"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// SetupRoutes configures all routes for the UI server.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore *sessions.CookieStore,
	notify *notifier.Notifier,
	isDev bool,
) error {
	// Hot reload endpoint for dev mode
	if isDev {
		setupReload(router)
	}

	// Static assets
	router.Handle("/static/*", resources.Handler())

	// Feature routes
	if err := homeFeature.SetupRoutes(router, eng, store, sessionStore, notify, isDev); err != nil {
		return err
	}

	if err := explorerFeature.SetupRoutes(router, eng, store, sessionStore, notify); err != nil {
		return err
	}

	if err := modelsFeature.SetupRoutes(router, eng, store, sessionStore, notify, isDev); err != nil {
		return err
	}

	if err := graphFeature.SetupRoutes(router, eng, store, sessionStore); err != nil {
		return err
	}

	if err := databaseFeature.SetupRoutes(router, eng, store, sessionStore); err != nil {
		return err
	}

	if err := runsFeature.SetupRoutes(router, eng, store, sessionStore); err != nil {
		return err
	}

	if err := statequeryFeature.SetupRoutes(router, store, sessionStore); err != nil {
		return err
	}

	return nil
}

func setupReload(router chi.Router) {
	reloadChan := make(chan struct{}, 1)
	var hotReloadOnce sync.Once

	router.Get("/reload", func(w http.ResponseWriter, r *http.Request) {
		sse := datastar.NewSSE(w, r)
		reload := func() { _ = sse.ExecuteScript("window.location.reload()") }
		hotReloadOnce.Do(reload)
		select {
		case <-reloadChan:
			reload()
		case <-r.Context().Done():
		}
	})

	router.Get("/hotreload", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case reloadChan <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
}
