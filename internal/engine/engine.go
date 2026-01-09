// Package engine provides the SQL model execution engine.
// It handles dependency resolution, topological execution, and incremental builds.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/macro"
	"github.com/leapstack-labs/leapsql/internal/registry"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/pkg/adapter"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// RunObserver receives notifications about run state changes.
// Implementations can use these callbacks to update UIs, trigger notifications, etc.
type RunObserver interface {
	OnRunStarted(run *core.Run)
	OnModelRunUpdated(runID string, modelRun *core.ModelRun)
	OnRunCompleted(run *core.Run)
}

// Engine orchestrates the execution of SQL models.
type Engine struct {
	// Database adapter (lazy initialized)
	db          adapter.Adapter
	dbConfig    core.AdapterConfig
	dbConnected bool
	dbMu        sync.Mutex

	// SQL dialect for the connected adapter (set after connection)
	dialect *core.Dialect

	// Structured logger
	logger *slog.Logger

	store         core.Store
	statePath     string // Path to state.db
	modelsDir     string
	seedsDir      string
	macrosDir     string
	environment   string
	target        *starctx.TargetInfo
	graph         *dag.Graph
	models        map[string]*core.Model
	registry      *registry.ModelRegistry
	macroRegistry *macro.Registry

	// Observer for run lifecycle events (optional)
	observer   RunObserver
	observerMu sync.RWMutex
}

// Config holds engine configuration.
type Config struct {
	// ModelsDir is the path to the models directory
	ModelsDir string
	// SeedsDir is the path to the seeds (raw data) directory
	SeedsDir string
	// MacrosDir is the path to the macros directory (optional)
	MacrosDir string
	// StatePath is the path to the SQLite state database
	StatePath string
	// Environment is the current environment (dev, staging, prod)
	Environment string
	// Target contains adapter/database configuration
	Target *starctx.TargetInfo
	// AdapterConfig contains the full adapter configuration
	AdapterConfig *core.AdapterConfig
	// Logger is the structured logger (optional, uses discard if nil)
	Logger *slog.Logger

	// DatabasePath is the path to the DuckDB database (empty for in-memory).
	//
	// Deprecated: Use Target configuration instead.
	DatabasePath string
}

// New creates a new engine with lazy database connection.
// The database adapter is only connected when Run() or LoadSeeds() is called.
func New(cfg Config) (*Engine, error) {
	// Initialize logger (use discard handler if nil)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	logger.Debug("initializing engine", "models_dir", cfg.ModelsDir, "environment", cfg.Environment)

	// Create state store (always needed)
	store := state.NewSQLiteStore(logger)
	if err := store.Open(cfg.StatePath); err != nil {
		return nil, fmt.Errorf("failed to open state store: %w", err)
	}

	if err := store.InitSchema(); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("failed to initialize state schema: %w", err)
	}

	// Load macros if macros directory is specified
	var macroRegistry *macro.Registry
	if cfg.MacrosDir != "" {
		var err error
		macroRegistry, err = macro.LoadAndRegister(cfg.MacrosDir)
		if err != nil {
			// Log warning but don't fail - macros are optional
			if !os.IsNotExist(err) {
				_ = store.Close()
				return nil, fmt.Errorf("failed to load macros: %w", err)
			}
			macroRegistry = macro.NewRegistry()
		}
	} else {
		macroRegistry = macro.NewRegistry()
	}

	// Set default environment
	env := cfg.Environment
	if env == "" {
		env = "dev"
	}

	// Require explicit target or adapter configuration
	if cfg.Target == nil && cfg.AdapterConfig == nil {
		_ = store.Close()
		return nil, fmt.Errorf("target configuration required: set 'target' in leapsql.yaml or provide AdapterConfig")
	}
	target := cfg.Target

	// Build adapter config
	var dbConfig core.AdapterConfig
	if cfg.AdapterConfig != nil {
		dbConfig = *cfg.AdapterConfig
	} else {
		// Build from target info and legacy DatabasePath for backward compatibility
		dbConfig = core.AdapterConfig{
			Type:     target.Type,
			Path:     cfg.DatabasePath,
			Database: target.Database,
			Schema:   target.Schema,
		}
		// If Path is empty but Database is set (for file-based DBs), use Database as Path
		if dbConfig.Path == "" && dbConfig.Database != "" && dbConfig.Type == "duckdb" {
			dbConfig.Path = dbConfig.Database
		}
	}

	// Require adapter type to be set
	if dbConfig.Type == "" {
		_ = store.Close()
		return nil, fmt.Errorf("adapter type required: specify target.type in leapsql.yaml (e.g., 'duckdb', 'postgres')")
	}

	// Require valid dialect for the adapter type
	d, ok := dialect.Get(dbConfig.Type)
	if !ok {
		_ = store.Close()
		return nil, fmt.Errorf("unknown adapter type %q: supported types are 'duckdb', 'postgres'", dbConfig.Type)
	}

	return &Engine{
		db:            nil, // Lazy
		dbConfig:      dbConfig,
		dbConnected:   false,
		dialect:       d,
		logger:        logger,
		store:         store,
		statePath:     cfg.StatePath,
		modelsDir:     cfg.ModelsDir,
		seedsDir:      cfg.SeedsDir,
		macrosDir:     cfg.MacrosDir,
		environment:   env,
		target:        target,
		graph:         dag.NewGraph(),
		models:        make(map[string]*core.Model),
		registry:      registry.NewModelRegistry(),
		macroRegistry: macroRegistry,
	}, nil
}

// ensureDBConnected lazily connects to the database.
func (e *Engine) ensureDBConnected(ctx context.Context) error {
	e.dbMu.Lock()
	defer e.dbMu.Unlock()

	if e.dbConnected {
		return nil
	}

	e.logger.Debug("connecting to database", "adapter_type", e.dbConfig.Type)

	// Use adapter registry to create the appropriate adapter
	db, err := adapter.NewAdapter(e.dbConfig, e.logger)
	if err != nil {
		return fmt.Errorf("failed to create database adapter: %w", err)
	}

	if err := db.Connect(ctx, e.dbConfig); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	e.db = db
	e.dbConnected = true

	// Get dialect from adapter config, then resolve full dialect
	cfg := db.DialectConfig()
	if cfg == nil {
		return fmt.Errorf("adapter returned nil dialect config for type %q", e.dbConfig.Type)
	}

	d, ok := dialect.Get(cfg.Name)
	if !ok {
		return fmt.Errorf("dialect %q not registered (returned by adapter %q)", cfg.Name, e.dbConfig.Type)
	}
	e.dialect = d

	e.logger.Debug("database connected", "dialect", e.dialect.Name)

	return nil
}

// Close releases all resources.
func (e *Engine) Close() error {
	e.logger.Debug("closing engine")

	var errs []error
	if e.db != nil {
		if err := e.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.store != nil {
		if err := e.store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing engine: %v", errs)
	}
	return nil
}

// DiscoverLegacy provides backward compatibility for code that uses the old Discover() signature.
//
// Deprecated: Use Discover(opts DiscoveryOptions) instead.
func (e *Engine) DiscoverLegacy() error {
	_, err := e.Discover(DiscoveryOptions{})
	return err
}

// --- Getters (public accessors) ---

// GetGraph returns the dependency graph.
func (e *Engine) GetGraph() *dag.Graph {
	return e.graph
}

// GetModels returns all discovered models.
func (e *Engine) GetModels() map[string]*core.Model {
	return e.models
}

// GetStateStore returns the state store.
func (e *Engine) GetStateStore() core.Store {
	return e.store
}

// GetStatePath returns the path to the state database file.
func (e *Engine) GetStatePath() string {
	return e.statePath
}

// GetDialect returns the SQL dialect for the connected adapter.
// Returns nil if the database is not yet connected.
func (e *Engine) GetDialect() *core.Dialect {
	return e.dialect
}

// GetAdapter returns the database adapter.
// Returns nil if the database is not yet connected.
// Call Query() on the returned adapter to execute SQL queries.
func (e *Engine) GetAdapter() adapter.Adapter {
	return e.db
}

// EnsureConnected ensures the database adapter is connected.
// This is useful when you need to run queries without running models.
func (e *Engine) EnsureConnected(ctx context.Context) error {
	return e.ensureDBConnected(ctx)
}

// SetRunObserver sets an observer to receive run lifecycle events.
// Pass nil to remove the observer. Thread-safe.
func (e *Engine) SetRunObserver(observer RunObserver) {
	e.observerMu.Lock()
	defer e.observerMu.Unlock()
	e.observer = observer
}

// getObserver returns the current observer (thread-safe).
func (e *Engine) getObserver() RunObserver {
	e.observerMu.RLock()
	defer e.observerMu.RUnlock()
	return e.observer
}
