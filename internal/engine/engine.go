// Package engine provides the SQL model execution engine.
// It handles dependency resolution, topological execution, and incremental builds.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/leapstack-labs/leapsql/internal/adapter"
	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/macro"
	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/internal/registry"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/internal/template"
	"github.com/leapstack-labs/leapsql/pkg/sql"
)

// Engine orchestrates the execution of SQL models.
type Engine struct {
	// Database adapter (lazy initialized)
	db          adapter.Adapter
	dbConfig    adapter.Config
	dbConnected bool
	dbMu        sync.Mutex

	// SQL dialect for the connected adapter (set after connection)
	dialect *sql.Dialect

	store         state.Store
	modelsDir     string
	seedsDir      string
	macrosDir     string
	environment   string
	target        *starctx.TargetInfo
	graph         *dag.Graph
	models        map[string]*parser.ModelConfig
	registry      *registry.ModelRegistry
	macroRegistry *macro.Registry
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
	AdapterConfig *adapter.Config

	// DatabasePath is the path to the DuckDB database (empty for in-memory).
	//
	// Deprecated: Use Target configuration instead.
	DatabasePath string
}

// New creates a new engine with lazy database connection.
// The database adapter is only connected when Run() or LoadSeeds() is called.
func New(cfg Config) (*Engine, error) {
	// Create state store (always needed)
	store := state.NewSQLiteStore()
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

	// Set default target
	target := cfg.Target
	if target == nil {
		target = &starctx.TargetInfo{
			Type:     "duckdb",
			Schema:   "main",
			Database: "",
		}
	}

	// Build adapter config
	var dbConfig adapter.Config
	if cfg.AdapterConfig != nil {
		dbConfig = *cfg.AdapterConfig
	} else {
		// Build from target info and legacy DatabasePath for backward compatibility
		dbConfig = adapter.Config{
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

	// Ensure adapter type is set
	if dbConfig.Type == "" {
		dbConfig.Type = "duckdb"
	}

	return &Engine{
		db:            nil, // Lazy
		dbConfig:      dbConfig,
		dbConnected:   false,
		store:         store,
		modelsDir:     cfg.ModelsDir,
		seedsDir:      cfg.SeedsDir,
		macrosDir:     cfg.MacrosDir,
		environment:   env,
		target:        target,
		graph:         dag.NewGraph(),
		models:        make(map[string]*parser.ModelConfig),
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

	// Use adapter registry to create the appropriate adapter
	db, err := adapter.NewAdapter(e.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to create database adapter: %w", err)
	}

	if err := db.Connect(ctx, e.dbConfig); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	e.db = db
	e.dbConnected = true

	// Set dialect based on adapter type
	dialectName := db.DialectName()
	if dialect, ok := sql.GetDialect(dialectName); ok {
		e.dialect = dialect
	} else {
		// Fallback to DuckDB dialect as default
		e.dialect = sql.DefaultDialect()
	}

	return nil
}

// Close releases all resources.
func (e *Engine) Close() error {
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

// LoadSeeds loads all CSV files from the seeds directory into the database.
func (e *Engine) LoadSeeds(ctx context.Context) error {
	if e.seedsDir == "" {
		return nil
	}

	// Ensure database is connected before loading seeds
	if err := e.ensureDBConnected(ctx); err != nil {
		return err
	}

	entries, err := os.ReadDir(e.seedsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No seeds directory is OK
		}
		return fmt.Errorf("failed to read seeds directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}

		tableName := strings.TrimSuffix(entry.Name(), ".csv")
		csvPath := filepath.Join(e.seedsDir, entry.Name())

		if err := e.db.LoadCSV(ctx, tableName, csvPath); err != nil {
			return fmt.Errorf("failed to load seed %s: %w", entry.Name(), err)
		}
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

// Run executes all models in topological order.
func (e *Engine) Run(ctx context.Context, env string) (*state.Run, error) {
	// Ensure database is connected before execution
	if err := e.ensureDBConnected(ctx); err != nil {
		return nil, err
	}

	// Create a new run
	run, err := e.store.CreateRun(env)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	// Get topological order
	sorted, err := e.graph.TopologicalSort()
	if err != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, fmt.Sprintf("failed to sort: %v", err))
		return run, err
	}

	// Execute each model
	var runErr error
	for _, node := range sorted {
		m := node.Data.(*parser.ModelConfig)

		// Get model from state store
		model, err := e.store.GetModelByPath(m.Path)
		if err != nil {
			runErr = fmt.Errorf("failed to get model %s: %w", m.Path, err)
			break
		}

		// Record model run start
		modelRun := &state.ModelRun{
			RunID:   run.ID,
			ModelID: model.ID,
			Status:  state.ModelRunStatusRunning,
		}
		if err := e.store.RecordModelRun(modelRun); err != nil {
			runErr = fmt.Errorf("failed to record model run: %w", err)
			break
		}

		// Execute the model
		startTime := time.Now()
		rowsAffected, execErr := e.executeModel(ctx, m, model)
		executionMS := int64(time.Since(startTime).Milliseconds())

		// Update model run status
		if execErr != nil {
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusFailed, 0, execErr.Error())
			runErr = execErr
		} else {
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusSuccess, rowsAffected, "")
		}

		_ = executionMS // Note: execution time tracked but not stored in current schema

		if runErr != nil {
			break
		}
	}

	// Complete the run
	if runErr != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, runErr.Error())
	} else {
		_ = e.store.CompleteRun(run.ID, state.RunStatusCompleted, "")
	}

	// Refresh run from store
	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// RunSelected executes only the specified models and their downstream dependents.
// Upstream dependencies must already exist in the database.
func (e *Engine) RunSelected(ctx context.Context, env string, modelPaths []string, includeDownstream bool) (*state.Run, error) {
	// Ensure database is connected before execution
	if err := e.ensureDBConnected(ctx); err != nil {
		return nil, err
	}

	var affected []string
	if includeDownstream {
		// Get affected nodes (selected + downstream)
		affected = e.graph.GetAffectedNodes(modelPaths)
	} else {
		// Only run the selected models
		affected = modelPaths
	}

	// Create subgraph with affected nodes
	subgraph := e.graph.Subgraph(affected)

	// Create a new run
	run, err := e.store.CreateRun(env)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	// Get topological order of subgraph
	sorted, err := subgraph.TopologicalSort()
	if err != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, fmt.Sprintf("failed to sort: %v", err))
		return run, err
	}

	var runErr error
	for _, node := range sorted {
		m := e.models[node.ID]
		if m == nil {
			continue
		}

		model, err := e.store.GetModelByPath(m.Path)
		if err != nil {
			runErr = fmt.Errorf("failed to get model %s: %w", m.Path, err)
			break
		}

		modelRun := &state.ModelRun{
			RunID:   run.ID,
			ModelID: model.ID,
			Status:  state.ModelRunStatusRunning,
		}
		if err := e.store.RecordModelRun(modelRun); err != nil {
			runErr = fmt.Errorf("failed to record model run: %w", err)
			break
		}

		startTime := time.Now()
		rowsAffected, execErr := e.executeModel(ctx, m, model)
		_ = int64(time.Since(startTime).Milliseconds())

		if execErr != nil {
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusFailed, 0, execErr.Error())
			runErr = execErr
		} else {
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusSuccess, rowsAffected, "")
		}

		if runErr != nil {
			break
		}
	}

	if runErr != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, runErr.Error())
	} else {
		_ = e.store.CompleteRun(run.ID, state.RunStatusCompleted, "")
	}

	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// executeModel executes a single model and returns rows affected.
func (e *Engine) executeModel(ctx context.Context, m *parser.ModelConfig, model *state.Model) (int64, error) {
	sql := e.buildSQL(m, model)

	switch m.Materialized {
	case "table":
		return e.executeTable(ctx, m.Path, sql)
	case "view":
		return e.executeView(ctx, m.Path, sql)
	case "incremental":
		return e.executeIncremental(ctx, m, model, sql)
	default:
		return 0, fmt.Errorf("unknown materialization: %s", m.Materialized)
	}
}

// buildSQL prepares the SQL for execution using template rendering.
func (e *Engine) buildSQL(m *parser.ModelConfig, model *state.Model) string {
	// Create execution context for this model
	ctx := e.createExecutionContext(m)

	// Render the template
	rendered, err := template.RenderString(m.SQL, m.FilePath, ctx)
	if err != nil {
		// Fallback to legacy string replacement if template fails
		// This provides backward compatibility
		return e.buildSQLLegacy(m, model)
	}

	return rendered
}

// buildSQLLegacy provides backward compatibility with simple string replacement.
func (e *Engine) buildSQLLegacy(m *parser.ModelConfig, _ *state.Model) string {
	sql := m.SQL

	// Replace {{ this }} with the model's table name
	tableName := pathToTableName(m.Path)
	sql = strings.ReplaceAll(sql, "{{ this }}", tableName)

	return sql
}

// createExecutionContext builds a Starlark execution context for template rendering.
func (e *Engine) createExecutionContext(m *parser.ModelConfig) *starctx.ExecutionContext {
	// Build config dict from model config
	config := starctx.BuildConfigDict(
		m.Name,
		m.Materialized,
		m.UniqueKey,
		m.Owner,
		m.Schema,
		m.Tags,
		m.Meta,
	)

	// Build this info
	thisInfo := &starctx.ThisInfo{
		Name:   m.Name,
		Schema: e.getModelSchema(m),
	}

	// Create context with macros
	ctx := starctx.NewContext(
		config,
		e.environment,
		e.target,
		thisInfo,
		starctx.WithMacroRegistry(e.macroRegistry),
	)

	return ctx
}

// getModelSchema extracts the schema from a model path.
func (e *Engine) getModelSchema(m *parser.ModelConfig) string {
	// If schema is explicitly set, use it
	if m.Schema != "" {
		return m.Schema
	}
	// Otherwise derive from path (e.g., "staging.customers" -> "staging")
	parts := strings.Split(m.Path, ".")
	if len(parts) > 1 {
		return parts[0]
	}
	return e.target.Schema // Default to target schema
}

// executeTable creates or replaces a table.
func (e *Engine) executeTable(ctx context.Context, path, sql string) (int64, error) {
	tableName := pathToTableName(path)

	// Drop existing table
	_ = e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create schema if needed
	parts := strings.Split(path, ".")
	if len(parts) > 1 {
		schema := parts[0]
		_ = e.db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	}

	// Create new table
	createSQL := fmt.Sprintf("CREATE TABLE %s AS %s", tableName, sql)
	if err := e.db.Exec(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Get row count
	rows, err := e.db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		return 0, nil // Table created but can't get count
	}
	defer func() { _ = rows.Close() }()

	var count int64
	if rows.Next() {
		_ = rows.Scan(&count)
	}

	return count, nil
}

// executeView creates or replaces a view.
func (e *Engine) executeView(ctx context.Context, path, sql string) (int64, error) {
	tableName := pathToTableName(path)

	// Drop existing view
	_ = e.db.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", tableName))

	// Create schema if needed
	parts := strings.Split(path, ".")
	if len(parts) > 1 {
		schema := parts[0]
		_ = e.db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	}

	// Create new view
	createSQL := fmt.Sprintf("CREATE VIEW %s AS %s", tableName, sql)
	if err := e.db.Exec(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create view %s: %w", tableName, err)
	}

	return 0, nil // Views don't affect rows
}

// executeIncremental handles incremental model execution.
func (e *Engine) executeIncremental(ctx context.Context, m *parser.ModelConfig, _ *state.Model, sql string) (int64, error) {
	tableName := pathToTableName(m.Path)

	// Check if table exists
	_, err := e.db.GetTableMetadata(ctx, tableName)
	tableExists := err == nil

	if !tableExists {
		// First run - create table with full data
		return e.executeTable(ctx, m.Path, sql)
	}

	// Table exists - check if we have incremental SQL
	incrementalSQL := sql
	if len(m.Conditionals) > 0 {
		// Apply incremental conditional
		for _, cond := range m.Conditionals {
			if strings.Contains(cond.Condition, "is_incremental") {
				// Process template replacements in conditional content
				condContent := cond.Content
				condContent = strings.ReplaceAll(condContent, "{{ this }}", tableName)
				incrementalSQL = sql + "\n" + condContent
				break
			}
		}
	}

	// Insert new rows using unique key for deduplication
	if m.UniqueKey != "" {
		// Merge/upsert pattern
		tempTable := tableName + "_temp"

		// Create temp table with new data
		_ = e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS %s", tempTable, incrementalSQL)
		if err := e.db.Exec(ctx, createTempSQL); err != nil {
			return 0, fmt.Errorf("failed to create temp table: %w", err)
		}

		// Delete matching rows from target
		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s)",
			tableName, m.UniqueKey, m.UniqueKey, tempTable)
		_ = e.db.Exec(ctx, deleteSQL)

		// Insert all rows from temp
		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableName, tempTable)
		if err := e.db.Exec(ctx, insertSQL); err != nil {
			return 0, fmt.Errorf("failed to insert incremental rows: %w", err)
		}

		// Get count from temp table
		rows, err := e.db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tempTable))
		var count int64
		if err == nil {
			defer func() { _ = rows.Close() }()
			if rows.Next() {
				_ = rows.Scan(&count)
			}
		}

		// Clean up temp table
		_ = e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))

		return count, nil
	}

	// No unique key - simple append
	insertSQL := fmt.Sprintf("INSERT INTO %s %s", tableName, incrementalSQL)
	if err := e.db.Exec(ctx, insertSQL); err != nil {
		return 0, fmt.Errorf("failed to insert rows: %w", err)
	}

	return 0, nil
}

// GetGraph returns the dependency graph.
func (e *Engine) GetGraph() *dag.Graph {
	return e.graph
}

// GetModels returns all discovered models.
func (e *Engine) GetModels() map[string]*parser.ModelConfig {
	return e.models
}

// GetStateStore returns the state store.
func (e *Engine) GetStateStore() state.Store {
	return e.store
}

// GetDialect returns the SQL dialect for the connected adapter.
// Returns nil if the database is not yet connected.
func (e *Engine) GetDialect() *sql.Dialect {
	return e.dialect
}

// RenderModel renders the SQL for a model with all templates expanded.
func (e *Engine) RenderModel(modelPath string) (string, error) {
	m, ok := e.models[modelPath]
	if !ok {
		return "", fmt.Errorf("model not found: %s", modelPath)
	}

	model, err := e.store.GetModelByPath(modelPath)
	if err != nil {
		// Model not in state store, create minimal version
		model = &state.Model{Path: modelPath, Name: m.Name}
	}

	return e.buildSQL(m, model), nil
}

// pathToTableName converts a model path to a SQL table name.
// e.g., "staging.customers" -> "staging.customers"
func pathToTableName(path string) string {
	return path
}

// hashContent generates a SHA256 hash of content.
func hashContent(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:8]) // Use first 8 bytes for brevity
}
