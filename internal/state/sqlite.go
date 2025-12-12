package state

import (
	"database/sql"
	_ "embed"
	"encoding/json"
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

	// Serialize complex fields to JSON
	tagsJSON, err := serializeJSON(model.Tags)
	if err != nil {
		return fmt.Errorf("failed to serialize tags: %w", err)
	}
	testsJSON, err := serializeJSON(model.Tests)
	if err != nil {
		return fmt.Errorf("failed to serialize tests: %w", err)
	}
	metaJSON, err := serializeJSON(model.Meta)
	if err != nil {
		return fmt.Errorf("failed to serialize meta: %w", err)
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
			`UPDATE models SET name = ?, materialized = ?, unique_key = ?, content_hash = ?, 
			 owner = ?, schema_name = ?, tags = ?, tests = ?, meta = ?, updated_at = ? 
			 WHERE id = ?`,
			model.Name, model.Materialized, model.UniqueKey, model.ContentHash,
			nullString(model.Owner), nullString(model.Schema), tagsJSON, testsJSON, metaJSON,
			model.UpdatedAt, model.ID,
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
			`INSERT INTO models (id, path, name, materialized, unique_key, content_hash, 
			 owner, schema_name, tags, tests, meta, created_at, updated_at) 
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			model.ID, model.Path, model.Name, model.Materialized, model.UniqueKey, model.ContentHash,
			nullString(model.Owner), nullString(model.Schema), tagsJSON, testsJSON, metaJSON,
			model.CreatedAt, model.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert model: %w", err)
		}
	}

	return nil
}

// nullString returns a sql.NullString for optional string fields.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}

// serializeJSON serializes a value to JSON, returning nil for empty values.
func serializeJSON(v any) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{Valid: false}, nil
	}

	// Check for empty slices and maps
	switch val := v.(type) {
	case []string:
		if len(val) == 0 {
			return sql.NullString{Valid: false}, nil
		}
	case []TestConfig:
		if len(val) == 0 {
			return sql.NullString{Valid: false}, nil
		}
	case map[string]any:
		if len(val) == 0 {
			return sql.NullString{Valid: false}, nil
		}
	}

	data, err := json.Marshal(v)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(data), Valid: true}, nil
}

// GetModelByID retrieves a model by ID.
func (s *SQLiteStore) GetModelByID(id string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	model := &Model{}
	var uniqueKey, owner, schema, tagsJSON, testsJSON, metaJSON sql.NullString

	err := s.db.QueryRow(
		`SELECT id, path, name, materialized, unique_key, content_hash, 
		 owner, schema_name, tags, tests, meta, created_at, updated_at 
		 FROM models WHERE id = ?`,
		id,
	).Scan(&model.ID, &model.Path, &model.Name, &model.Materialized, &uniqueKey, &model.ContentHash,
		&owner, &schema, &tagsJSON, &testsJSON, &metaJSON, &model.CreatedAt, &model.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("model not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model: %w", err)
	}

	// Deserialize optional fields
	if uniqueKey.Valid {
		model.UniqueKey = uniqueKey.String
	}
	if owner.Valid {
		model.Owner = owner.String
	}
	if schema.Valid {
		model.Schema = schema.String
	}

	// Deserialize JSON fields
	if err := deserializeJSON(tagsJSON, &model.Tags); err != nil {
		return nil, fmt.Errorf("failed to deserialize tags: %w", err)
	}
	if err := deserializeJSON(testsJSON, &model.Tests); err != nil {
		return nil, fmt.Errorf("failed to deserialize tests: %w", err)
	}
	if err := deserializeJSON(metaJSON, &model.Meta); err != nil {
		return nil, fmt.Errorf("failed to deserialize meta: %w", err)
	}

	return model, nil
}

// deserializeJSON deserializes a JSON string into a target value.
func deserializeJSON(data sql.NullString, target any) error {
	if !data.Valid || data.String == "" {
		return nil
	}
	return json.Unmarshal([]byte(data.String), target)
}

// GetModelByPath retrieves a model by its path.
func (s *SQLiteStore) GetModelByPath(path string) (*Model, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	model := &Model{}
	var uniqueKey, owner, schema, tagsJSON, testsJSON, metaJSON sql.NullString

	err := s.db.QueryRow(
		`SELECT id, path, name, materialized, unique_key, content_hash, 
		 owner, schema_name, tags, tests, meta, created_at, updated_at 
		 FROM models WHERE path = ?`,
		path,
	).Scan(&model.ID, &model.Path, &model.Name, &model.Materialized, &uniqueKey, &model.ContentHash,
		&owner, &schema, &tagsJSON, &testsJSON, &metaJSON, &model.CreatedAt, &model.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil // Not found, return nil without error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get model: %w", err)
	}

	// Deserialize optional fields
	if uniqueKey.Valid {
		model.UniqueKey = uniqueKey.String
	}
	if owner.Valid {
		model.Owner = owner.String
	}
	if schema.Valid {
		model.Schema = schema.String
	}

	// Deserialize JSON fields
	if err := deserializeJSON(tagsJSON, &model.Tags); err != nil {
		return nil, fmt.Errorf("failed to deserialize tags: %w", err)
	}
	if err := deserializeJSON(testsJSON, &model.Tests); err != nil {
		return nil, fmt.Errorf("failed to deserialize tests: %w", err)
	}
	if err := deserializeJSON(metaJSON, &model.Meta); err != nil {
		return nil, fmt.Errorf("failed to deserialize meta: %w", err)
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
		`SELECT id, path, name, materialized, unique_key, content_hash, 
		 owner, schema_name, tags, tests, meta, created_at, updated_at 
		 FROM models ORDER BY path`,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list models: %w", err)
	}
	defer rows.Close()

	var models []*Model
	for rows.Next() {
		model := &Model{}
		var uniqueKey, owner, schema, tagsJSON, testsJSON, metaJSON sql.NullString

		err := rows.Scan(&model.ID, &model.Path, &model.Name, &model.Materialized, &uniqueKey, &model.ContentHash,
			&owner, &schema, &tagsJSON, &testsJSON, &metaJSON, &model.CreatedAt, &model.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan model: %w", err)
		}

		// Deserialize optional fields
		if uniqueKey.Valid {
			model.UniqueKey = uniqueKey.String
		}
		if owner.Valid {
			model.Owner = owner.String
		}
		if schema.Valid {
			model.Schema = schema.String
		}

		// Deserialize JSON fields
		if err := deserializeJSON(tagsJSON, &model.Tags); err != nil {
			return nil, fmt.Errorf("failed to deserialize tags: %w", err)
		}
		if err := deserializeJSON(testsJSON, &model.Tests); err != nil {
			return nil, fmt.Errorf("failed to deserialize tests: %w", err)
		}
		if err := deserializeJSON(metaJSON, &model.Meta); err != nil {
			return nil, fmt.Errorf("failed to deserialize meta: %w", err)
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

// --- Column lineage operations ---

// SaveModelColumns saves column lineage information for a model.
// This replaces any existing column information for the model.
func (s *SQLiteStore) SaveModelColumns(modelPath string, columns []ColumnInfo) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete existing column lineage first (due to foreign key)
	_, err = tx.Exec(`DELETE FROM column_lineage WHERE model_path = ?`, modelPath)
	if err != nil {
		return fmt.Errorf("failed to delete existing column lineage: %w", err)
	}

	// Delete existing columns
	_, err = tx.Exec(`DELETE FROM model_columns WHERE model_path = ?`, modelPath)
	if err != nil {
		return fmt.Errorf("failed to delete existing columns: %w", err)
	}

	// Insert new columns
	colStmt, err := tx.Prepare(`INSERT INTO model_columns (model_path, column_name, column_index, transform_type, function_name) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare column insert: %w", err)
	}
	defer colStmt.Close()

	lineageStmt, err := tx.Prepare(`INSERT INTO column_lineage (model_path, column_name, source_table, source_column) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare lineage insert: %w", err)
	}
	defer lineageStmt.Close()

	for _, col := range columns {
		// Insert column
		_, err = colStmt.Exec(modelPath, col.Name, col.Index, col.TransformType, col.Function)
		if err != nil {
			return fmt.Errorf("failed to insert column %s: %w", col.Name, err)
		}

		// Insert source lineage for this column
		for _, src := range col.Sources {
			if src.Table == "" && src.Column == "" {
				continue // Skip empty sources
			}
			_, err = lineageStmt.Exec(modelPath, col.Name, src.Table, src.Column)
			if err != nil {
				return fmt.Errorf("failed to insert lineage for column %s: %w", col.Name, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetModelColumns retrieves column lineage information for a model.
func (s *SQLiteStore) GetModelColumns(modelPath string) ([]ColumnInfo, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	// Get all columns for the model
	colRows, err := s.db.Query(
		`SELECT column_name, column_index, transform_type, function_name 
		 FROM model_columns 
		 WHERE model_path = ? 
		 ORDER BY column_index`,
		modelPath,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer colRows.Close()

	// Use index map instead of pointer map to avoid slice reallocation issues
	columnsIdxMap := make(map[string]int)
	var columns []ColumnInfo

	for colRows.Next() {
		var col ColumnInfo
		var transformType, functionName sql.NullString

		if err := colRows.Scan(&col.Name, &col.Index, &transformType, &functionName); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		if transformType.Valid {
			col.TransformType = transformType.String
		}
		if functionName.Valid {
			col.Function = functionName.String
		}

		columnsIdxMap[col.Name] = len(columns)
		columns = append(columns, col)
	}

	if err := colRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Get lineage for all columns
	lineageRows, err := s.db.Query(
		`SELECT column_name, source_table, source_column 
		 FROM column_lineage 
		 WHERE model_path = ?`,
		modelPath,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get column lineage: %w", err)
	}
	defer lineageRows.Close()

	for lineageRows.Next() {
		var colName, sourceTable, sourceColumn string
		if err := lineageRows.Scan(&colName, &sourceTable, &sourceColumn); err != nil {
			return nil, fmt.Errorf("failed to scan lineage: %w", err)
		}

		if idx, ok := columnsIdxMap[colName]; ok {
			columns[idx].Sources = append(columns[idx].Sources, SourceRef{
				Table:  sourceTable,
				Column: sourceColumn,
			})
		}
	}

	return columns, lineageRows.Err()
}

// DeleteModelColumns deletes all column information for a model.
func (s *SQLiteStore) DeleteModelColumns(modelPath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete lineage first (foreign key constraint)
	_, err = tx.Exec(`DELETE FROM column_lineage WHERE model_path = ?`, modelPath)
	if err != nil {
		return fmt.Errorf("failed to delete column lineage: %w", err)
	}

	// Delete columns
	_, err = tx.Exec(`DELETE FROM model_columns WHERE model_path = ?`, modelPath)
	if err != nil {
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

	query := `
WITH RECURSIVE trace AS (
    -- Start: get direct sources of the target column
    SELECT 
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        1 as depth
    FROM column_lineage cl
    WHERE cl.model_path = ? AND cl.column_name = ?
    
    UNION ALL
    
    -- Recurse: follow source_table -> model -> its sources
    SELECT 
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        t.depth + 1
    FROM trace t
    JOIN models m ON (m.name = t.source_table OR m.path = t.source_table)
    JOIN column_lineage cl ON cl.model_path = m.path AND cl.column_name = t.source_column
    WHERE t.depth < 20
)
SELECT DISTINCT 
    source_table,
    source_column,
    depth,
    CASE WHEN m.path IS NULL THEN 1 ELSE 0 END as is_external
FROM trace t
LEFT JOIN models m ON (m.name = t.source_table OR m.path = t.source_table)
ORDER BY depth, source_table, source_column`

	rows, err := s.db.Query(query, modelPath, columnName)
	if err != nil {
		return nil, fmt.Errorf("failed to trace column backward: %w", err)
	}
	defer rows.Close()

	var results []TraceResult
	for rows.Next() {
		var r TraceResult
		var isExternal int
		if err := rows.Scan(&r.ModelPath, &r.ColumnName, &r.Depth, &isExternal); err != nil {
			return nil, fmt.Errorf("failed to scan trace result: %w", err)
		}
		r.IsExternal = isExternal == 1
		results = append(results, r)
	}

	return results, rows.Err()
}

// TraceColumnForward traces where a column flows to downstream.
// It follows the lineage recursively to find all downstream consumers.
func (s *SQLiteStore) TraceColumnForward(modelPath, columnName string) ([]TraceResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	query := `
WITH RECURSIVE trace AS (
    -- Start: find columns that reference this model/column as a source
    SELECT 
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        1 as depth
    FROM column_lineage cl
    JOIN models m ON (m.name = cl.source_table OR m.path = cl.source_table)
    WHERE m.path = ? AND cl.source_column = ?
    
    UNION ALL
    
    -- Recurse: find what references the columns we found
    SELECT 
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        t.depth + 1
    FROM trace t
    JOIN models m ON m.path = t.model_path
    JOIN column_lineage cl ON (cl.source_table = m.name OR cl.source_table = m.path)
                          AND cl.source_column = t.column_name
    WHERE t.depth < 20
)
SELECT DISTINCT model_path, column_name, depth
FROM trace
ORDER BY depth, model_path, column_name`

	rows, err := s.db.Query(query, modelPath, columnName)
	if err != nil {
		return nil, fmt.Errorf("failed to trace column forward: %w", err)
	}
	defer rows.Close()

	var results []TraceResult
	for rows.Next() {
		var r TraceResult
		if err := rows.Scan(&r.ModelPath, &r.ColumnName, &r.Depth); err != nil {
			return nil, fmt.Errorf("failed to scan trace result: %w", err)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// --- Macro operations ---

// SaveMacroNamespace stores a macro namespace and its functions.
// This replaces any existing functions for the namespace.
func (s *SQLiteStore) SaveMacroNamespace(ns *MacroNamespace, functions []*MacroFunction) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Upsert namespace
	_, err = tx.Exec(`
		INSERT INTO macro_namespaces (name, file_path, package, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(name) DO UPDATE SET
			file_path = excluded.file_path,
			package = excluded.package,
			updated_at = CURRENT_TIMESTAMP
	`, ns.Name, ns.FilePath, ns.Package)
	if err != nil {
		return fmt.Errorf("failed to upsert namespace: %w", err)
	}

	// Delete old functions for this namespace
	_, err = tx.Exec("DELETE FROM macro_functions WHERE namespace = ?", ns.Name)
	if err != nil {
		return fmt.Errorf("failed to delete old functions: %w", err)
	}

	// Insert functions
	stmt, err := tx.Prepare(`
		INSERT INTO macro_functions (namespace, name, args, docstring, line)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, fn := range functions {
		argsJSON, _ := json.Marshal(fn.Args)
		_, err := stmt.Exec(ns.Name, fn.Name, string(argsJSON), fn.Docstring, fn.Line)
		if err != nil {
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

	rows, err := s.db.Query(`
		SELECT name, file_path, package, updated_at 
		FROM macro_namespaces 
		ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get namespaces: %w", err)
	}
	defer rows.Close()

	var namespaces []*MacroNamespace
	for rows.Next() {
		var ns MacroNamespace
		var pkg sql.NullString
		if err := rows.Scan(&ns.Name, &ns.FilePath, &pkg, &ns.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan namespace: %w", err)
		}
		if pkg.Valid {
			ns.Package = pkg.String
		}
		namespaces = append(namespaces, &ns)
	}

	return namespaces, rows.Err()
}

// GetMacroNamespace returns a single macro namespace by name.
func (s *SQLiteStore) GetMacroNamespace(name string) (*MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	var ns MacroNamespace
	var pkg sql.NullString

	err := s.db.QueryRow(`
		SELECT name, file_path, package, updated_at 
		FROM macro_namespaces 
		WHERE name = ?
	`, name).Scan(&ns.Name, &ns.FilePath, &pkg, &ns.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	if pkg.Valid {
		ns.Package = pkg.String
	}

	return &ns, nil
}

// GetMacroFunctions returns all functions for a namespace.
func (s *SQLiteStore) GetMacroFunctions(namespace string) ([]*MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(`
		SELECT namespace, name, args, docstring, line 
		FROM macro_functions 
		WHERE namespace = ?
		ORDER BY name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get functions: %w", err)
	}
	defer rows.Close()

	var functions []*MacroFunction
	for rows.Next() {
		var fn MacroFunction
		var argsJSON string
		var docstring sql.NullString
		if err := rows.Scan(&fn.Namespace, &fn.Name, &argsJSON, &docstring, &fn.Line); err != nil {
			return nil, fmt.Errorf("failed to scan function: %w", err)
		}
		json.Unmarshal([]byte(argsJSON), &fn.Args)
		if docstring.Valid {
			fn.Docstring = docstring.String
		}
		functions = append(functions, &fn)
	}

	return functions, rows.Err()
}

// GetMacroFunction returns a single function by namespace and name.
func (s *SQLiteStore) GetMacroFunction(namespace, name string) (*MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	var fn MacroFunction
	var argsJSON string
	var docstring sql.NullString

	err := s.db.QueryRow(`
		SELECT namespace, name, args, docstring, line 
		FROM macro_functions 
		WHERE namespace = ? AND name = ?
	`, namespace, name).Scan(&fn.Namespace, &fn.Name, &argsJSON, &docstring, &fn.Line)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get function: %w", err)
	}

	json.Unmarshal([]byte(argsJSON), &fn.Args)
	if docstring.Valid {
		fn.Docstring = docstring.String
	}

	return &fn, nil
}

// MacroFunctionExists checks if a macro function exists.
func (s *SQLiteStore) MacroFunctionExists(namespace, name string) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database not opened")
	}

	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM macro_functions 
		WHERE namespace = ? AND name = ?
	`, namespace, name).Scan(&count)

	return count > 0, err
}

// SearchMacroNamespaces searches namespaces by prefix.
func (s *SQLiteStore) SearchMacroNamespaces(prefix string) ([]*MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(`
		SELECT name, file_path, package, updated_at 
		FROM macro_namespaces 
		WHERE name LIKE ? || '%'
		ORDER BY name
	`, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to search namespaces: %w", err)
	}
	defer rows.Close()

	var namespaces []*MacroNamespace
	for rows.Next() {
		var ns MacroNamespace
		var pkg sql.NullString
		if err := rows.Scan(&ns.Name, &ns.FilePath, &pkg, &ns.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan namespace: %w", err)
		}
		if pkg.Valid {
			ns.Package = pkg.String
		}
		namespaces = append(namespaces, &ns)
	}

	return namespaces, rows.Err()
}

// SearchMacroFunctions searches functions within a namespace by prefix.
func (s *SQLiteStore) SearchMacroFunctions(namespace, prefix string) ([]*MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.db.Query(`
		SELECT namespace, name, args, docstring, line 
		FROM macro_functions 
		WHERE namespace = ? AND name LIKE ? || '%'
		ORDER BY name
	`, namespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to search functions: %w", err)
	}
	defer rows.Close()

	var functions []*MacroFunction
	for rows.Next() {
		var fn MacroFunction
		var argsJSON string
		var docstring sql.NullString
		if err := rows.Scan(&fn.Namespace, &fn.Name, &argsJSON, &docstring, &fn.Line); err != nil {
			return nil, fmt.Errorf("failed to scan function: %w", err)
		}
		json.Unmarshal([]byte(argsJSON), &fn.Args)
		if docstring.Valid {
			fn.Docstring = docstring.String
		}
		functions = append(functions, &fn)
	}

	return functions, rows.Err()
}

// DeleteMacroNamespace deletes a namespace and all its functions.
func (s *SQLiteStore) DeleteMacroNamespace(name string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	// The CASCADE in the schema should handle deleting functions
	_, err := s.db.Exec("DELETE FROM macro_namespaces WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %w", err)
	}

	return nil
}
