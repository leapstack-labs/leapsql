package state

import (
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schemaSQL string

// SQLiteStore implements StateStore using SQLite.
type SQLiteStore struct {
	db   *sql.DB
	path string
}

// NewSQLiteStore creates a new SQLite state store instance.
func NewSQLiteStore() *SQLiteStore {
	return &SQLiteStore{}
}

// Open opens a connection to the SQLite database.
// Use ":memory:" for an in-memory database.
func (s *SQLiteStore) Open(path string) error {
	// Enable foreign keys and WAL mode for better performance
	dsn := path
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
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	s.db = db
	s.path = path
	return nil
}

// Close closes the SQLite database connection.
func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// InitSchema initializes the database schema.
func (s *SQLiteStore) InitSchema() error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	_, err := s.db.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}
	return nil
}

// generateID creates a new UUID.
func generateID() string {
	return uuid.New().String()
}

// --- Run operations ---

// CreateRun creates a new pipeline run.
func (s *SQLiteStore) CreateRun(env string) (*Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	run := &Run{
		ID:          generateID(),
		Environment: env,
		Status:      RunStatusRunning,
		StartedAt:   time.Now().UTC(),
	}

	_, err := s.db.Exec(
		`INSERT INTO runs (id, environment, status, started_at) VALUES (?, ?, ?, ?)`,
		run.ID, run.Environment, run.Status, run.StartedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	return run, nil
}

// GetRun retrieves a run by ID.
func (s *SQLiteStore) GetRun(id string) (*Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	run := &Run{}
	var completedAt sql.NullTime
	var errMsg sql.NullString

	err := s.db.QueryRow(
		`SELECT id, environment, status, started_at, completed_at, error FROM runs WHERE id = ?`,
		id,
	).Scan(&run.ID, &run.Environment, &run.Status, &run.StartedAt, &completedAt, &errMsg)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("run not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get run: %w", err)
	}

	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	if errMsg.Valid {
		run.Error = errMsg.String
	}

	return run, nil
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

	result, err := s.db.Exec(
		`UPDATE runs SET status = ?, completed_at = ?, error = ? WHERE id = ?`,
		status, now, errorPtr, id,
	)
	if err != nil {
		return fmt.Errorf("failed to complete run: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("run not found: %s", id)
	}

	return nil
}

// GetLatestRun retrieves the most recent run for an environment.
func (s *SQLiteStore) GetLatestRun(env string) (*Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	run := &Run{}
	var completedAt sql.NullTime
	var errMsg sql.NullString

	err := s.db.QueryRow(
		`SELECT id, environment, status, started_at, completed_at, error 
		 FROM runs WHERE environment = ? ORDER BY started_at DESC LIMIT 1`,
		env,
	).Scan(&run.ID, &run.Environment, &run.Status, &run.StartedAt, &completedAt, &errMsg)

	if err == sql.ErrNoRows {
		return nil, nil // No runs found, return nil without error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest run: %w", err)
	}

	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	if errMsg.Valid {
		run.Error = errMsg.String
	}

	return run, nil
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

		_, err := s.db.Exec(
			`UPDATE models SET name = ?, materialized = ?, unique_key = ?, content_hash = ?, updated_at = ? WHERE id = ?`,
			model.Name, model.Materialized, model.UniqueKey, model.ContentHash, model.UpdatedAt, model.ID,
		)
		if err != nil {
			return fmt.Errorf("failed to update model: %w", err)
		}
	} else {
		// Insert new model
		if model.ID == "" {
			model.ID = generateID()
		}
		model.CreatedAt = now
		model.UpdatedAt = now

		_, err := s.db.Exec(
			`INSERT INTO models (id, path, name, materialized, unique_key, content_hash, created_at, updated_at) 
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			model.ID, model.Path, model.Name, model.Materialized, model.UniqueKey, model.ContentHash, model.CreatedAt, model.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert model: %w", err)
		}
	}

	return nil
}

// GetModelByID retrieves a model by ID.
func (s *SQLiteStore) GetModelByID(id string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	model := &Model{}
	var uniqueKey sql.NullString

	err := s.db.QueryRow(
		`SELECT id, path, name, materialized, unique_key, content_hash, created_at, updated_at 
		 FROM models WHERE id = ?`,
		id,
	).Scan(&model.ID, &model.Path, &model.Name, &model.Materialized, &uniqueKey, &model.ContentHash, &model.CreatedAt, &model.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("model not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model: %w", err)
	}

	if uniqueKey.Valid {
		model.UniqueKey = uniqueKey.String
	}

	return model, nil
}

// GetModelByPath retrieves a model by its path.
func (s *SQLiteStore) GetModelByPath(path string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	model := &Model{}
	var uniqueKey sql.NullString

	err := s.db.QueryRow(
		`SELECT id, path, name, materialized, unique_key, content_hash, created_at, updated_at 
		 FROM models WHERE path = ?`,
		path,
	).Scan(&model.ID, &model.Path, &model.Name, &model.Materialized, &uniqueKey, &model.ContentHash, &model.CreatedAt, &model.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil // Not found, return nil without error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model: %w", err)
	}

	if uniqueKey.Valid {
		model.UniqueKey = uniqueKey.String
	}

	return model, nil
}

// UpdateModelHash updates the content hash of a model.
func (s *SQLiteStore) UpdateModelHash(id string, contentHash string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	result, err := s.db.Exec(
		`UPDATE models SET content_hash = ?, updated_at = ? WHERE id = ?`,
		contentHash, time.Now().UTC(), id,
	)
	if err != nil {
		return fmt.Errorf("failed to update model hash: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("model not found: %s", id)
	}

	return nil
}

// ListModels retrieves all registered models.
func (s *SQLiteStore) ListModels() ([]*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(
		`SELECT id, path, name, materialized, unique_key, content_hash, created_at, updated_at FROM models ORDER BY path`,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list models: %w", err)
	}
	defer rows.Close()

	var models []*Model
	for rows.Next() {
		model := &Model{}
		var uniqueKey sql.NullString

		err := rows.Scan(&model.ID, &model.Path, &model.Name, &model.Materialized, &uniqueKey, &model.ContentHash, &model.CreatedAt, &model.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan model: %w", err)
		}

		if uniqueKey.Valid {
			model.UniqueKey = uniqueKey.String
		}
		models = append(models, model)
	}

	return models, rows.Err()
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

	_, err := s.db.Exec(
		`INSERT INTO model_runs (id, run_id, model_id, status, rows_affected, started_at, error, execution_ms) 
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		modelRun.ID, modelRun.RunID, modelRun.ModelID, modelRun.Status, modelRun.RowsAffected, modelRun.StartedAt, modelRun.Error, modelRun.ExecutionMS,
	)
	if err != nil {
		return fmt.Errorf("failed to record model run: %w", err)
	}

	return nil
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

	// Calculate execution time
	var startedAt time.Time
	err := s.db.QueryRow(`SELECT started_at FROM model_runs WHERE id = ?`, id).Scan(&startedAt)
	if err != nil {
		return fmt.Errorf("failed to get model run start time: %w", err)
	}

	executionMS := now.Sub(startedAt).Milliseconds()

	result, err := s.db.Exec(
		`UPDATE model_runs SET status = ?, rows_affected = ?, completed_at = ?, error = ?, execution_ms = ? WHERE id = ?`,
		status, rowsAffected, now, errorPtr, executionMS, id,
	)
	if err != nil {
		return fmt.Errorf("failed to update model run: %w", err)
	}

	rowsUpdated, _ := result.RowsAffected()
	if rowsUpdated == 0 {
		return fmt.Errorf("model run not found: %s", id)
	}

	return nil
}

// GetModelRunsForRun retrieves all model runs for a given pipeline run.
func (s *SQLiteStore) GetModelRunsForRun(runID string) ([]*ModelRun, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(
		`SELECT id, run_id, model_id, status, rows_affected, started_at, completed_at, error, execution_ms 
		 FROM model_runs WHERE run_id = ? ORDER BY started_at`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get model runs: %w", err)
	}
	defer rows.Close()

	var modelRuns []*ModelRun
	for rows.Next() {
		mr := &ModelRun{}
		var completedAt sql.NullTime
		var errMsg sql.NullString

		err := rows.Scan(&mr.ID, &mr.RunID, &mr.ModelID, &mr.Status, &mr.RowsAffected, &mr.StartedAt, &completedAt, &errMsg, &mr.ExecutionMS)
		if err != nil {
			return nil, fmt.Errorf("failed to scan model run: %w", err)
		}

		if completedAt.Valid {
			mr.CompletedAt = &completedAt.Time
		}
		if errMsg.Valid {
			mr.Error = errMsg.String
		}
		modelRuns = append(modelRuns, mr)
	}

	return modelRuns, rows.Err()
}

// GetLatestModelRun retrieves the most recent run for a model.
func (s *SQLiteStore) GetLatestModelRun(modelID string) (*ModelRun, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	mr := &ModelRun{}
	var completedAt sql.NullTime
	var errMsg sql.NullString

	err := s.db.QueryRow(
		`SELECT id, run_id, model_id, status, rows_affected, started_at, completed_at, error, execution_ms 
		 FROM model_runs WHERE model_id = ? ORDER BY started_at DESC LIMIT 1`,
		modelID,
	).Scan(&mr.ID, &mr.RunID, &mr.ModelID, &mr.Status, &mr.RowsAffected, &mr.StartedAt, &completedAt, &errMsg, &mr.ExecutionMS)

	if err == sql.ErrNoRows {
		return nil, nil // No runs found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest model run: %w", err)
	}

	if completedAt.Valid {
		mr.CompletedAt = &completedAt.Time
	}
	if errMsg.Valid {
		mr.Error = errMsg.String
	}

	return mr, nil
}

// --- Dependency operations ---

// SetDependencies sets the parent dependencies for a model.
// This replaces any existing dependencies.
func (s *SQLiteStore) SetDependencies(modelID string, parentIDs []string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete existing dependencies
	_, err = tx.Exec(`DELETE FROM dependencies WHERE model_id = ?`, modelID)
	if err != nil {
		return fmt.Errorf("failed to delete existing dependencies: %w", err)
	}

	// Insert new dependencies
	for _, parentID := range parentIDs {
		_, err = tx.Exec(`INSERT INTO dependencies (model_id, parent_id) VALUES (?, ?)`, modelID, parentID)
		if err != nil {
			return fmt.Errorf("failed to insert dependency: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetDependencies retrieves the parent IDs for a model.
func (s *SQLiteStore) GetDependencies(modelID string) ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(`SELECT parent_id FROM dependencies WHERE model_id = ?`, modelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependencies: %w", err)
	}
	defer rows.Close()

	var parentIDs []string
	for rows.Next() {
		var parentID string
		if err := rows.Scan(&parentID); err != nil {
			return nil, fmt.Errorf("failed to scan dependency: %w", err)
		}
		parentIDs = append(parentIDs, parentID)
	}

	return parentIDs, rows.Err()
}

// GetDependents retrieves the IDs of models that depend on the given model.
func (s *SQLiteStore) GetDependents(modelID string) ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(`SELECT model_id FROM dependencies WHERE parent_id = ?`, modelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependents: %w", err)
	}
	defer rows.Close()

	var dependentIDs []string
	for rows.Next() {
		var dependentID string
		if err := rows.Scan(&dependentID); err != nil {
			return nil, fmt.Errorf("failed to scan dependent: %w", err)
		}
		dependentIDs = append(dependentIDs, dependentID)
	}

	return dependentIDs, rows.Err()
}

// --- Environment operations ---

// CreateEnvironment creates a new environment.
func (s *SQLiteStore) CreateEnvironment(name string) (*Environment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	env := &Environment{
		Name:      name,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	_, err := s.db.Exec(
		`INSERT INTO environments (name, created_at, updated_at) VALUES (?, ?, ?)`,
		env.Name, env.CreatedAt, env.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create environment: %w", err)
	}

	return env, nil
}

// GetEnvironment retrieves an environment by name.
func (s *SQLiteStore) GetEnvironment(name string) (*Environment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	env := &Environment{}
	var commitRef sql.NullString

	err := s.db.QueryRow(
		`SELECT name, commit_ref, created_at, updated_at FROM environments WHERE name = ?`,
		name,
	).Scan(&env.Name, &commitRef, &env.CreatedAt, &env.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get environment: %w", err)
	}

	if commitRef.Valid {
		env.CommitRef = commitRef.String
	}

	return env, nil
}

// UpdateEnvironmentRef updates the commit reference for an environment.
func (s *SQLiteStore) UpdateEnvironmentRef(name string, commitRef string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	result, err := s.db.Exec(
		`UPDATE environments SET commit_ref = ?, updated_at = ? WHERE name = ?`,
		commitRef, time.Now().UTC(), name,
	)
	if err != nil {
		return fmt.Errorf("failed to update environment ref: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("environment not found: %s", name)
	}

	return nil
}

// Ensure SQLiteStore implements StateStore interface
var _ StateStore = (*SQLiteStore)(nil)
