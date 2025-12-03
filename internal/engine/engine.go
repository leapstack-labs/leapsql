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
	"time"

	"github.com/user/dbgo/internal/adapter"
	"github.com/user/dbgo/internal/dag"
	"github.com/user/dbgo/internal/parser"
	"github.com/user/dbgo/internal/state"
)

// Engine orchestrates the execution of SQL models.
type Engine struct {
	db        adapter.Adapter
	store     state.StateStore
	modelsDir string
	seedsDir  string
	graph     *dag.Graph
	models    map[string]*parser.ModelConfig
}

// Config holds engine configuration.
type Config struct {
	// ModelsDir is the path to the models directory
	ModelsDir string
	// SeedsDir is the path to the seeds (raw data) directory
	SeedsDir string
	// DatabasePath is the path to the DuckDB database (empty for in-memory)
	DatabasePath string
	// StatePath is the path to the SQLite state database
	StatePath string
}

// New creates a new engine with the given configuration.
func New(cfg Config) (*Engine, error) {
	ctx := context.Background()

	// Create DuckDB adapter
	db := adapter.NewDuckDBAdapter()
	if err := db.Connect(ctx, adapter.Config{Path: cfg.DatabasePath}); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create state store
	store := state.NewSQLiteStore()
	if err := store.Open(cfg.StatePath); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open state store: %w", err)
	}

	if err := store.InitSchema(); err != nil {
		db.Close()
		store.Close()
		return nil, fmt.Errorf("failed to initialize state schema: %w", err)
	}

	return &Engine{
		db:        db,
		store:     store,
		modelsDir: cfg.ModelsDir,
		seedsDir:  cfg.SeedsDir,
		graph:     dag.NewGraph(),
		models:    make(map[string]*parser.ModelConfig),
	}, nil
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

// Discover scans the models directory and builds the dependency graph.
func (e *Engine) Discover() error {
	scanner := parser.NewScanner(e.modelsDir)
	models, err := scanner.ScanDir(e.modelsDir)
	if err != nil {
		return fmt.Errorf("failed to scan models: %w", err)
	}

	// Clear existing graph
	e.graph = dag.NewGraph()
	e.models = make(map[string]*parser.ModelConfig)

	// Add all models as nodes
	for _, m := range models {
		e.graph.AddNode(m.Path, m)
		e.models[m.Path] = m
	}

	// Add dependency edges
	for _, m := range models {
		for _, imp := range m.Imports {
			if _, exists := e.graph.GetNode(imp); exists {
				if err := e.graph.AddEdge(imp, m.Path); err != nil {
					return fmt.Errorf("failed to add dependency %s -> %s: %w", imp, m.Path, err)
				}
			}
		}
	}

	// Check for cycles
	if hasCycle, cyclePath := e.graph.HasCycle(); hasCycle {
		return fmt.Errorf("circular dependency detected: %v", cyclePath)
	}

	// Register models in state store
	for _, m := range models {
		model := &state.Model{
			Path:         m.Path,
			Name:         m.Name,
			Materialized: m.Materialized,
			UniqueKey:    m.UniqueKey,
			ContentHash:  hashContent(m.RawContent),
		}
		if err := e.store.RegisterModel(model); err != nil {
			return fmt.Errorf("failed to register model %s: %w", m.Path, err)
		}
	}

	return nil
}

// Run executes all models in topological order.
func (e *Engine) Run(ctx context.Context, env string) (*state.Run, error) {
	// Create a new run
	run, err := e.store.CreateRun(env)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	// Get topological order
	sorted, err := e.graph.TopologicalSort()
	if err != nil {
		e.store.CompleteRun(run.ID, state.RunStatusFailed, fmt.Sprintf("failed to sort: %v", err))
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
			e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusFailed, 0, execErr.Error())
			runErr = execErr
		} else {
			e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusSuccess, rowsAffected, "")
		}

		_ = executionMS // Note: execution time tracked but not stored in current schema

		if runErr != nil {
			break
		}
	}

	// Complete the run
	if runErr != nil {
		e.store.CompleteRun(run.ID, state.RunStatusFailed, runErr.Error())
	} else {
		e.store.CompleteRun(run.ID, state.RunStatusCompleted, "")
	}

	// Refresh run from store
	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// RunSelected executes only the specified models and their downstream dependents.
// Upstream dependencies must already exist in the database.
func (e *Engine) RunSelected(ctx context.Context, env string, modelPaths []string, includeDownstream bool) (*state.Run, error) {
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
		e.store.CompleteRun(run.ID, state.RunStatusFailed, fmt.Sprintf("failed to sort: %v", err))
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
			e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusFailed, 0, execErr.Error())
			runErr = execErr
		} else {
			e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusSuccess, rowsAffected, "")
		}

		if runErr != nil {
			break
		}
	}

	if runErr != nil {
		e.store.CompleteRun(run.ID, state.RunStatusFailed, runErr.Error())
	} else {
		e.store.CompleteRun(run.ID, state.RunStatusCompleted, "")
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

// buildSQL prepares the SQL for execution, replacing placeholders.
func (e *Engine) buildSQL(m *parser.ModelConfig, model *state.Model) string {
	sql := m.SQL

	// Replace {{ this }} with the model's table name
	tableName := pathToTableName(m.Path)
	sql = strings.ReplaceAll(sql, "{{ this }}", tableName)

	// Replace ref('path') with actual table names
	for _, imp := range m.Imports {
		refPattern := fmt.Sprintf("{{ ref('%s') }}", imp)
		sql = strings.ReplaceAll(sql, refPattern, pathToTableName(imp))
	}

	return sql
}

// executeTable creates or replaces a table.
func (e *Engine) executeTable(ctx context.Context, path, sql string) (int64, error) {
	tableName := pathToTableName(path)

	// Drop existing table
	e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create schema if needed
	parts := strings.Split(path, ".")
	if len(parts) > 1 {
		schema := parts[0]
		e.db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
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
	defer rows.Close()

	var count int64
	if rows.Next() {
		rows.Scan(&count)
	}

	return count, nil
}

// executeView creates or replaces a view.
func (e *Engine) executeView(ctx context.Context, path, sql string) (int64, error) {
	tableName := pathToTableName(path)

	// Drop existing view
	e.db.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", tableName))

	// Create schema if needed
	parts := strings.Split(path, ".")
	if len(parts) > 1 {
		schema := parts[0]
		e.db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	}

	// Create new view
	createSQL := fmt.Sprintf("CREATE VIEW %s AS %s", tableName, sql)
	if err := e.db.Exec(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create view %s: %w", tableName, err)
	}

	return 0, nil // Views don't affect rows
}

// executeIncremental handles incremental model execution.
func (e *Engine) executeIncremental(ctx context.Context, m *parser.ModelConfig, model *state.Model, sql string) (int64, error) {
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
				for _, imp := range m.Imports {
					refPattern := fmt.Sprintf("{{ ref('%s') }}", imp)
					condContent = strings.ReplaceAll(condContent, refPattern, pathToTableName(imp))
				}
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
		e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS %s", tempTable, incrementalSQL)
		if err := e.db.Exec(ctx, createTempSQL); err != nil {
			return 0, fmt.Errorf("failed to create temp table: %w", err)
		}

		// Delete matching rows from target
		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s)",
			tableName, m.UniqueKey, m.UniqueKey, tempTable)
		e.db.Exec(ctx, deleteSQL)

		// Insert all rows from temp
		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableName, tempTable)
		if err := e.db.Exec(ctx, insertSQL); err != nil {
			return 0, fmt.Errorf("failed to insert incremental rows: %w", err)
		}

		// Get count from temp table
		rows, err := e.db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tempTable))
		var count int64
		if err == nil {
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&count)
			}
		}

		// Clean up temp table
		e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))

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
