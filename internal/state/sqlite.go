package state

//go:generate sqlc generate

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver
)

//go:embed schema.sql
var schemaSQL string

// SQLiteStore implements Store using SQLite with sqlc-generated queries.
type SQLiteStore struct {
	db      *sql.DB
	queries *sqlcgen.Queries
	path    string
	logger  *slog.Logger
}

// NewSQLiteStore creates a new SQLite state store instance.
func NewSQLiteStore(logger *slog.Logger) *SQLiteStore {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &SQLiteStore{logger: logger}
}

// Open opens a connection to the SQLite database.
// Use ":memory:" for an in-memory database.
func (s *SQLiteStore) Open(path string) error {
	s.logger.Debug("opening state database", slog.String("path", path))

	// Enable foreign keys and WAL mode for better performance
	var dsn string
	if path != ":memory:" {
		dsn = fmt.Sprintf("%s?_foreign_keys=on&_journal_mode=WAL", path)
	} else {
		dsn = ":memory:?_foreign_keys=on"
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Test connection
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	s.db = db
	s.path = path
	s.queries = sqlcgen.New(db)
	return nil
}

// Close closes the SQLite database connection.
func (s *SQLiteStore) Close() error {
	if s.db != nil {
		s.logger.Debug("closing state database", slog.String("path", s.path))
		return s.db.Close()
	}
	return nil
}

// InitSchema initializes the database schema.
func (s *SQLiteStore) InitSchema() error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	s.logger.Debug("initializing database schema")

	_, err := s.db.ExecContext(context.Background(), schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}
	return nil
}

// generateID creates a new UUID.
func generateID() string {
	return uuid.New().String()
}

// ctx returns a background context for operations.
func ctx() context.Context {
	return context.Background()
}

// --- Run operations ---

// CreateRun creates a new pipeline run.
func (s *SQLiteStore) CreateRun(env string) (*Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	id := generateID()
	now := time.Now().UTC()

	s.logger.Debug("creating run", slog.String("id", id), slog.String("environment", env))

	row, err := s.queries.CreateRun(ctx(), sqlcgen.CreateRunParams{
		ID:          id,
		Environment: env,
		Status:      string(RunStatusRunning),
		StartedAt:   now,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	return convertRun(row), nil
}

// GetRun retrieves a run by ID.
func (s *SQLiteStore) GetRun(id string) (*Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetRun(ctx(), id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("run not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get run: %w", err)
	}

	return convertRun(row), nil
}

// CompleteRun marks a run as completed with the given status.
func (s *SQLiteStore) CompleteRun(id string, status RunStatus, errMsg string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	now := time.Now().UTC()
	var errorPtr *string
	if errMsg != "" {
		errorPtr = &errMsg
	}

	return s.queries.CompleteRun(ctx(), sqlcgen.CompleteRunParams{
		Status:      string(status),
		CompletedAt: &now,
		Error:       errorPtr,
		ID:          id,
	})
}

// GetLatestRun retrieves the most recent run for an environment.
func (s *SQLiteStore) GetLatestRun(env string) (*Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetLatestRun(ctx(), env)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No runs found, return nil without error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest run: %w", err)
	}

	return convertRun(row), nil
}

// --- Model operations ---

// RegisterModel registers a new model or updates an existing one.
func (s *SQLiteStore) RegisterModel(model *Model) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	// Default materialized to "table" if not set
	if model.Materialized == "" {
		model.Materialized = "table"
	}

	// Serialize complex fields to JSON
	tagsJSON := serializeJSONPtr(model.Tags)
	testsJSON := serializeJSONPtr(model.Tests)
	metaJSON := serializeJSONPtr(model.Meta)

	now := time.Now().UTC()

	// Check if model already exists by path
	existing, err := s.GetModelByPath(model.Path)
	if err != nil {
		return fmt.Errorf("failed to check existing model: %w", err)
	}

	if existing != nil {
		// Update existing model, preserve the ID
		model.ID = existing.ID
		model.CreatedAt = existing.CreatedAt
		model.UpdatedAt = now

		return s.queries.UpdateModel(ctx(), sqlcgen.UpdateModelParams{
			Name:         model.Name,
			Materialized: model.Materialized,
			UniqueKey:    nullableString(model.UniqueKey),
			ContentHash:  model.ContentHash,
			FilePath:     nullableString(model.FilePath),
			Owner:        nullableString(model.Owner),
			SchemaName:   nullableString(model.Schema),
			Tags:         tagsJSON,
			Tests:        testsJSON,
			Meta:         metaJSON,
			UpdatedAt:    model.UpdatedAt,
			ID:           model.ID,
		})
	}

	// Insert new model
	if model.ID == "" {
		model.ID = generateID()
	}
	model.CreatedAt = now
	model.UpdatedAt = now

	return s.queries.InsertModel(ctx(), sqlcgen.InsertModelParams{
		ID:           model.ID,
		Path:         model.Path,
		Name:         model.Name,
		Materialized: model.Materialized,
		UniqueKey:    nullableString(model.UniqueKey),
		ContentHash:  model.ContentHash,
		FilePath:     nullableString(model.FilePath),
		Owner:        nullableString(model.Owner),
		SchemaName:   nullableString(model.Schema),
		Tags:         tagsJSON,
		Tests:        testsJSON,
		Meta:         metaJSON,
		CreatedAt:    model.CreatedAt,
		UpdatedAt:    model.UpdatedAt,
	})
}

// GetModelByID retrieves a model by ID.
func (s *SQLiteStore) GetModelByID(id string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetModelByID(ctx(), id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("model not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model: %w", err)
	}

	return convertModel(row)
}

// GetModelByPath retrieves a model by its path.
func (s *SQLiteStore) GetModelByPath(path string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetModelByPath(ctx(), path)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // Not found, return nil without error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model: %w", err)
	}

	return convertModel(row)
}

// GetModelByFilePath retrieves a model by its file system path.
func (s *SQLiteStore) GetModelByFilePath(filePath string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetModelByFilePath(ctx(), &filePath)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // Not found, return nil without error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model by file path: %w", err)
	}

	return convertModel(row)
}

// UpdateModelHash updates the content hash of a model.
func (s *SQLiteStore) UpdateModelHash(id string, contentHash string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.UpdateModelHash(ctx(), sqlcgen.UpdateModelHashParams{
		ContentHash: contentHash,
		UpdatedAt:   time.Now().UTC(),
		ID:          id,
	})
}

// ListModels retrieves all registered models.
func (s *SQLiteStore) ListModels() ([]*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.ListModels(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to list models: %w", err)
	}

	models := make([]*Model, 0, len(rows))
	for _, row := range rows {
		model, err := convertModel(row)
		if err != nil {
			return nil, err
		}
		models = append(models, model)
	}

	return models, nil
}

// DeleteModelByFilePath deletes a model by its file system path.
func (s *SQLiteStore) DeleteModelByFilePath(filePath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	// First delete associated column lineage and dependencies
	model, err := s.GetModelByFilePath(filePath)
	if err != nil {
		return err
	}
	if model == nil {
		return nil // Model not found, nothing to delete
	}

	// Delete column lineage
	if err := s.queries.DeleteColumnLineageByModelPath(ctx(), model.Path); err != nil {
		return fmt.Errorf("failed to delete column lineage: %w", err)
	}

	// Delete model columns
	if err := s.queries.DeleteModelColumnsByModelPath(ctx(), model.Path); err != nil {
		return fmt.Errorf("failed to delete model columns: %w", err)
	}

	// Delete dependencies
	if err := s.queries.DeleteDependenciesByModelOrParent(ctx(), sqlcgen.DeleteDependenciesByModelOrParentParams{
		FilePath:   &filePath,
		FilePath_2: &filePath,
	}); err != nil {
		return fmt.Errorf("failed to delete dependencies: %w", err)
	}

	// Delete the model
	return s.queries.DeleteModelByFilePath(ctx(), &filePath)
}

// --- Model run operations ---

// RecordModelRun records a new model execution.
func (s *SQLiteStore) RecordModelRun(modelRun *ModelRun) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	if modelRun.ID == "" {
		modelRun.ID = generateID()
	}
	modelRun.StartedAt = time.Now().UTC()

	var errorPtr *string
	if modelRun.Error != "" {
		errorPtr = &modelRun.Error
	}

	return s.queries.RecordModelRun(ctx(), sqlcgen.RecordModelRunParams{
		ID:           modelRun.ID,
		RunID:        modelRun.RunID,
		ModelID:      modelRun.ModelID,
		Status:       string(modelRun.Status),
		RowsAffected: &modelRun.RowsAffected,
		StartedAt:    modelRun.StartedAt,
		Error:        errorPtr,
		ExecutionMs:  &modelRun.ExecutionMS,
	})
}

// UpdateModelRun updates the status of a model run.
func (s *SQLiteStore) UpdateModelRun(id string, status ModelRunStatus, rowsAffected int64, errMsg string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	now := time.Now().UTC()
	var errorPtr *string
	if errMsg != "" {
		errorPtr = &errMsg
	}

	// Get started_at to calculate execution time
	startedAt, err := s.queries.GetModelRunStartedAt(ctx(), id)
	if err != nil {
		return fmt.Errorf("failed to get model run start time: %w", err)
	}

	executionMS := now.Sub(startedAt).Milliseconds()

	return s.queries.UpdateModelRun(ctx(), sqlcgen.UpdateModelRunParams{
		Status:       string(status),
		RowsAffected: &rowsAffected,
		CompletedAt:  &now,
		Error:        errorPtr,
		ExecutionMs:  &executionMS,
		ID:           id,
	})
}

// GetModelRunsForRun retrieves all model runs for a given pipeline run.
func (s *SQLiteStore) GetModelRunsForRun(runID string) ([]*ModelRun, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.GetModelRunsForRun(ctx(), runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get model runs: %w", err)
	}

	modelRuns := make([]*ModelRun, 0, len(rows))
	for _, row := range rows {
		modelRuns = append(modelRuns, convertModelRun(row))
	}

	return modelRuns, nil
}

// GetLatestModelRun retrieves the most recent run for a model.
func (s *SQLiteStore) GetLatestModelRun(modelID string) (*ModelRun, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetLatestModelRun(ctx(), modelID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No runs found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest model run: %w", err)
	}

	return convertModelRun(row), nil
}

// --- Dependency operations ---

// SetDependencies sets the parent dependencies for a model.
// This replaces any existing dependencies.
func (s *SQLiteStore) SetDependencies(modelID string, parentIDs []string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.queries.WithTx(tx)

	// Delete existing dependencies
	if err := qtx.DeleteDependenciesByModelID(ctx(), modelID); err != nil {
		return fmt.Errorf("failed to delete existing dependencies: %w", err)
	}

	// Insert new dependencies
	for _, parentID := range parentIDs {
		if err := qtx.InsertDependency(ctx(), sqlcgen.InsertDependencyParams{
			ModelID:  modelID,
			ParentID: parentID,
		}); err != nil {
			return fmt.Errorf("failed to insert dependency: %w", err)
		}
	}

	return tx.Commit()
}

// GetDependencies retrieves the parent IDs for a model.
func (s *SQLiteStore) GetDependencies(modelID string) ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	return s.queries.GetDependencies(ctx(), modelID)
}

// GetDependents retrieves the IDs of models that depend on the given model.
func (s *SQLiteStore) GetDependents(modelID string) ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	return s.queries.GetDependents(ctx(), modelID)
}

// --- Environment operations ---

// CreateEnvironment creates a new environment.
func (s *SQLiteStore) CreateEnvironment(name string) (*Environment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	now := time.Now().UTC()

	row, err := s.queries.CreateEnvironment(ctx(), sqlcgen.CreateEnvironmentParams{
		Name:      name,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create environment: %w", err)
	}

	return convertEnvironment(row), nil
}

// GetEnvironment retrieves an environment by name.
func (s *SQLiteStore) GetEnvironment(name string) (*Environment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetEnvironment(ctx(), name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get environment: %w", err)
	}

	return convertEnvironment(row), nil
}

// UpdateEnvironmentRef updates the commit reference for an environment.
func (s *SQLiteStore) UpdateEnvironmentRef(name string, commitRef string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.UpdateEnvironmentRef(ctx(), sqlcgen.UpdateEnvironmentRefParams{
		CommitRef: &commitRef,
		UpdatedAt: time.Now().UTC(),
		Name:      name,
	})
}

// --- Column lineage operations ---

// SaveModelColumns saves column lineage information for a model.
// This replaces any existing column information for the model.
func (s *SQLiteStore) SaveModelColumns(modelPath string, columns []ColumnInfo) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.queries.WithTx(tx)

	// Delete existing column lineage first (due to foreign key)
	if err := qtx.DeleteColumnLineageByModelPath(ctx(), modelPath); err != nil {
		return fmt.Errorf("failed to delete existing column lineage: %w", err)
	}

	// Delete existing columns
	if err := qtx.DeleteModelColumnsByModelPath(ctx(), modelPath); err != nil {
		return fmt.Errorf("failed to delete existing columns: %w", err)
	}

	// Insert new columns
	for _, col := range columns {
		if err := qtx.InsertModelColumn(ctx(), sqlcgen.InsertModelColumnParams{
			ModelPath:     modelPath,
			ColumnName:    col.Name,
			ColumnIndex:   int64(col.Index),
			TransformType: nullableString(col.TransformType),
			FunctionName:  nullableString(col.Function),
		}); err != nil {
			return fmt.Errorf("failed to insert column %s: %w", col.Name, err)
		}

		// Insert source lineage for this column
		for _, src := range col.Sources {
			if src.Table == "" && src.Column == "" {
				continue // Skip empty sources
			}
			if err := qtx.InsertColumnLineage(ctx(), sqlcgen.InsertColumnLineageParams{
				ModelPath:    modelPath,
				ColumnName:   col.Name,
				SourceTable:  src.Table,
				SourceColumn: src.Column,
			}); err != nil {
				return fmt.Errorf("failed to insert lineage for column %s: %w", col.Name, err)
			}
		}
	}

	return tx.Commit()
}

// GetModelColumns retrieves column lineage information for a model.
func (s *SQLiteStore) GetModelColumns(modelPath string) ([]ColumnInfo, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	// Get all columns for the model
	colRows, err := s.queries.GetModelColumns(ctx(), modelPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Build columns with index map for lineage lookup
	columnsIdxMap := make(map[string]int)
	columns := make([]ColumnInfo, 0, len(colRows))

	for _, row := range colRows {
		col := ColumnInfo{
			Name:          row.ColumnName,
			Index:         int(row.ColumnIndex),
			TransformType: derefString(row.TransformType),
			Function:      derefString(row.FunctionName),
		}
		columnsIdxMap[col.Name] = len(columns)
		columns = append(columns, col)
	}

	// Get lineage for all columns
	lineageRows, err := s.queries.GetColumnLineage(ctx(), modelPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get column lineage: %w", err)
	}

	for _, row := range lineageRows {
		if idx, ok := columnsIdxMap[row.ColumnName]; ok {
			columns[idx].Sources = append(columns[idx].Sources, SourceRef{
				Table:  row.SourceTable,
				Column: row.SourceColumn,
			})
		}
	}

	return columns, nil
}

// DeleteModelColumns deletes all column information for a model.
func (s *SQLiteStore) DeleteModelColumns(modelPath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.queries.WithTx(tx)

	// Delete lineage first (foreign key constraint)
	if err := qtx.DeleteColumnLineageByModelPath(ctx(), modelPath); err != nil {
		return fmt.Errorf("failed to delete column lineage: %w", err)
	}

	// Delete columns
	if err := qtx.DeleteModelColumnsByModelPath(ctx(), modelPath); err != nil {
		return fmt.Errorf("failed to delete columns: %w", err)
	}

	return tx.Commit()
}

// TraceColumnBackward traces a column back to its ultimate sources.
// It follows the lineage recursively to find all upstream columns.
func (s *SQLiteStore) TraceColumnBackward(modelPath, columnName string) ([]TraceResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.TraceColumnBackward(ctx(), sqlcgen.TraceColumnBackwardParams{
		ModelPath:  modelPath,
		ColumnName: columnName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to trace column backward: %w", err)
	}

	results := make([]TraceResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, TraceResult{
			ModelPath:  row.ModelPath,
			ColumnName: row.ColumnName,
			Depth:      int(row.Depth),
			IsExternal: row.IsExternal == 1,
		})
	}

	return results, nil
}

// TraceColumnForward traces where a column flows to downstream.
// It follows the lineage recursively to find all downstream consumers.
func (s *SQLiteStore) TraceColumnForward(modelPath, columnName string) ([]TraceResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.TraceColumnForward(ctx(), sqlcgen.TraceColumnForwardParams{
		Path:         modelPath,
		SourceColumn: columnName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to trace column forward: %w", err)
	}

	results := make([]TraceResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, TraceResult{
			ModelPath:  row.ModelPath,
			ColumnName: row.ColumnName,
			Depth:      int(row.Depth),
		})
	}

	return results, nil
}

// --- Macro operations ---

// SaveMacroNamespace stores a macro namespace and its functions.
// This replaces any existing functions for the namespace.
func (s *SQLiteStore) SaveMacroNamespace(ns *MacroNamespace, functions []*MacroFunction) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.queries.WithTx(tx)

	// Upsert namespace
	if err := qtx.UpsertMacroNamespace(ctx(), sqlcgen.UpsertMacroNamespaceParams{
		Name:     ns.Name,
		FilePath: ns.FilePath,
		Package:  nullableString(ns.Package),
	}); err != nil {
		return fmt.Errorf("failed to upsert namespace: %w", err)
	}

	// Delete old functions for this namespace
	if err := qtx.DeleteMacroFunctionsByNamespace(ctx(), ns.Name); err != nil {
		return fmt.Errorf("failed to delete old functions: %w", err)
	}

	// Insert functions
	for _, fn := range functions {
		argsJSON, _ := json.Marshal(fn.Args)
		line := int64(fn.Line)
		if err := qtx.InsertMacroFunction(ctx(), sqlcgen.InsertMacroFunctionParams{
			Namespace: ns.Name,
			Name:      fn.Name,
			Args:      string(argsJSON),
			Docstring: nullableString(fn.Docstring),
			Line:      &line,
		}); err != nil {
			return fmt.Errorf("failed to insert function %s: %w", fn.Name, err)
		}
	}

	return tx.Commit()
}

// GetMacroNamespaces returns all macro namespaces.
func (s *SQLiteStore) GetMacroNamespaces() ([]*MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.GetMacroNamespaces(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to get namespaces: %w", err)
	}

	namespaces := make([]*MacroNamespace, 0, len(rows))
	for _, row := range rows {
		namespaces = append(namespaces, convertMacroNamespace(row))
	}

	return namespaces, nil
}

// GetMacroNamespace returns a single macro namespace by name.
func (s *SQLiteStore) GetMacroNamespace(name string) (*MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetMacroNamespace(ctx(), name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	return convertMacroNamespace(row), nil
}

// GetMacroFunctions returns all functions for a namespace.
func (s *SQLiteStore) GetMacroFunctions(namespace string) ([]*MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.GetMacroFunctions(ctx(), namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get functions: %w", err)
	}

	functions := make([]*MacroFunction, 0, len(rows))
	for _, row := range rows {
		functions = append(functions, convertMacroFunction(row))
	}

	return functions, nil
}

// GetMacroFunction returns a single function by namespace and name.
func (s *SQLiteStore) GetMacroFunction(namespace, name string) (*MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetMacroFunction(ctx(), sqlcgen.GetMacroFunctionParams{
		Namespace: namespace,
		Name:      name,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get function: %w", err)
	}

	return convertMacroFunction(row), nil
}

// MacroFunctionExists checks if a macro function exists.
func (s *SQLiteStore) MacroFunctionExists(namespace, name string) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database not opened")
	}

	count, err := s.queries.MacroFunctionExists(ctx(), sqlcgen.MacroFunctionExistsParams{
		Namespace: namespace,
		Name:      name,
	})
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// SearchMacroNamespaces searches namespaces by prefix.
func (s *SQLiteStore) SearchMacroNamespaces(prefix string) ([]*MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.SearchMacroNamespaces(ctx(), &prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to search namespaces: %w", err)
	}

	namespaces := make([]*MacroNamespace, 0, len(rows))
	for _, row := range rows {
		namespaces = append(namespaces, convertMacroNamespace(row))
	}

	return namespaces, nil
}

// SearchMacroFunctions searches functions within a namespace by prefix.
func (s *SQLiteStore) SearchMacroFunctions(namespace, prefix string) ([]*MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.SearchMacroFunctions(ctx(), sqlcgen.SearchMacroFunctionsParams{
		Namespace: namespace,
		Column2:   &prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search functions: %w", err)
	}

	functions := make([]*MacroFunction, 0, len(rows))
	for _, row := range rows {
		functions = append(functions, convertMacroFunction(row))
	}

	return functions, nil
}

// DeleteMacroNamespace deletes a namespace and all its functions.
func (s *SQLiteStore) DeleteMacroNamespace(name string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.DeleteMacroNamespace(ctx(), name)
}

// DeleteMacroNamespaceByFilePath deletes a namespace by its file path.
func (s *SQLiteStore) DeleteMacroNamespaceByFilePath(filePath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.DeleteMacroNamespaceByFilePath(ctx(), filePath)
}

// --- File hash operations for incremental discovery ---

// GetContentHash retrieves the content hash for a file path.
func (s *SQLiteStore) GetContentHash(filePath string) (string, error) {
	if s.db == nil {
		return "", fmt.Errorf("database not opened")
	}

	hash, err := s.queries.GetContentHash(ctx(), filePath)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil // Not found, return empty string
	}
	if err != nil {
		return "", fmt.Errorf("failed to get content hash: %w", err)
	}

	return hash, nil
}

// SetContentHash stores the content hash for a file path.
func (s *SQLiteStore) SetContentHash(filePath, hash, fileType string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.SetContentHash(ctx(), sqlcgen.SetContentHashParams{
		FilePath:    filePath,
		ContentHash: hash,
		FileType:    fileType,
	})
}

// DeleteContentHash removes the content hash for a file path.
func (s *SQLiteStore) DeleteContentHash(filePath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.DeleteContentHash(ctx(), filePath)
}

// --- File path listing operations ---

// ListModelFilePaths returns all file paths of tracked models.
func (s *SQLiteStore) ListModelFilePaths() ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.ListModelFilePaths(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to list model file paths: %w", err)
	}

	paths := make([]string, 0, len(rows))
	for _, row := range rows {
		if row != nil {
			paths = append(paths, *row)
		}
	}

	return paths, nil
}

// ListMacroFilePaths returns all file paths of tracked macro namespaces.
func (s *SQLiteStore) ListMacroFilePaths() ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	return s.queries.ListMacroFilePaths(ctx())
}

// Ensure SQLiteStore implements Store interface
var _ Store = (*SQLiteStore)(nil)

// --- Helper functions for type conversion ---

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func serializeJSONPtr(v any) *string {
	if v == nil {
		return nil
	}

	// Check for empty slices and maps
	switch val := v.(type) {
	case []string:
		if len(val) == 0 {
			return nil
		}
	case []TestConfig:
		if len(val) == 0 {
			return nil
		}
	case map[string]any:
		if len(val) == 0 {
			return nil
		}
	}

	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(data)
	return &s
}

func deserializeJSON(data *string, target any) error {
	if data == nil || *data == "" {
		return nil
	}
	return json.Unmarshal([]byte(*data), target)
}

func convertRun(row sqlcgen.Run) *Run {
	run := &Run{
		ID:          row.ID,
		Environment: row.Environment,
		Status:      RunStatus(row.Status),
		StartedAt:   row.StartedAt,
		CompletedAt: row.CompletedAt,
	}
	if row.Error != nil {
		run.Error = *row.Error
	}
	return run
}

func convertModel(row sqlcgen.Model) (*Model, error) {
	model := &Model{
		ID:           row.ID,
		Path:         row.Path,
		Name:         row.Name,
		Materialized: row.Materialized,
		ContentHash:  row.ContentHash,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}

	if row.UniqueKey != nil {
		model.UniqueKey = *row.UniqueKey
	}
	if row.FilePath != nil {
		model.FilePath = *row.FilePath
	}
	if row.Owner != nil {
		model.Owner = *row.Owner
	}
	if row.SchemaName != nil {
		model.Schema = *row.SchemaName
	}

	// Deserialize JSON fields
	if err := deserializeJSON(row.Tags, &model.Tags); err != nil {
		return nil, fmt.Errorf("failed to deserialize tags: %w", err)
	}
	if err := deserializeJSON(row.Tests, &model.Tests); err != nil {
		return nil, fmt.Errorf("failed to deserialize tests: %w", err)
	}
	if err := deserializeJSON(row.Meta, &model.Meta); err != nil {
		return nil, fmt.Errorf("failed to deserialize meta: %w", err)
	}

	return model, nil
}

func convertModelRun(row sqlcgen.ModelRun) *ModelRun {
	mr := &ModelRun{
		ID:          row.ID,
		RunID:       row.RunID,
		ModelID:     row.ModelID,
		Status:      ModelRunStatus(row.Status),
		StartedAt:   row.StartedAt,
		CompletedAt: row.CompletedAt,
	}

	if row.RowsAffected != nil {
		mr.RowsAffected = *row.RowsAffected
	}
	if row.Error != nil {
		mr.Error = *row.Error
	}
	if row.ExecutionMs != nil {
		mr.ExecutionMS = *row.ExecutionMs
	}

	return mr
}

func convertEnvironment(row sqlcgen.Environment) *Environment {
	env := &Environment{
		Name:      row.Name,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}

	if row.CommitRef != nil {
		env.CommitRef = *row.CommitRef
	}

	return env
}

func convertMacroNamespace(row sqlcgen.MacroNamespace) *MacroNamespace {
	ns := &MacroNamespace{
		Name:     row.Name,
		FilePath: row.FilePath,
	}

	if row.Package != nil {
		ns.Package = *row.Package
	}
	if row.UpdatedAt != nil {
		ns.UpdatedAt = row.UpdatedAt.Format(time.RFC3339)
	}

	return ns
}

func convertMacroFunction(row sqlcgen.MacroFunction) *MacroFunction {
	fn := &MacroFunction{
		Namespace: row.Namespace,
		Name:      row.Name,
	}

	// Parse args JSON
	if row.Args != "" {
		_ = json.Unmarshal([]byte(row.Args), &fn.Args)
	}

	if row.Docstring != nil {
		fn.Docstring = *row.Docstring
	}
	if row.Line != nil {
		fn.Line = int(*row.Line)
	}

	return fn
}
