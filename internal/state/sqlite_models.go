package state

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// RegisterModel registers a new model or updates an existing one.
func (s *SQLiteStore) RegisterModel(model *Model) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	// Ensure embedded Model is not nil
	if model.Model == nil {
		model.Model = &core.Model{}
	}

	// Default materialized to "table" if not set
	if model.Materialized == "" {
		model.Materialized = "table"
	}

	// Serialize complex fields to JSON (accessing via embedded Model)
	tagsJSON := serializeJSONPtr(model.Tags)
	testsJSON := serializeJSONPtr(model.Tests)
	metaJSON := serializeJSONPtr(model.Meta)

	// Convert bool to int64 for SQLite
	var usesSelectStar *int64
	if model.UsesSelectStar {
		one := int64(1)
		usesSelectStar = &one
	} else {
		zero := int64(0)
		usesSelectStar = &zero
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

		return s.queries.UpdateModel(ctx(), sqlcgen.UpdateModelParams{
			Name:           model.Name,
			Materialized:   model.Materialized,
			UniqueKey:      nullableString(model.UniqueKey),
			ContentHash:    model.ContentHash,
			FilePath:       nullableString(model.FilePath),
			Owner:          nullableString(model.Owner),
			SchemaName:     nullableString(model.Schema),
			Tags:           tagsJSON,
			Tests:          testsJSON,
			Meta:           metaJSON,
			UsesSelectStar: usesSelectStar,
			UpdatedAt:      model.UpdatedAt,
			ID:             model.ID,
		})
	}

	// Insert new model
	if model.ID == "" {
		model.ID = generateID()
	}
	model.CreatedAt = now
	model.UpdatedAt = now

	return s.queries.InsertModel(ctx(), sqlcgen.InsertModelParams{
		ID:             model.ID,
		Path:           model.Path,
		Name:           model.Name,
		Materialized:   model.Materialized,
		UniqueKey:      nullableString(model.UniqueKey),
		ContentHash:    model.ContentHash,
		FilePath:       nullableString(model.FilePath),
		Owner:          nullableString(model.Owner),
		SchemaName:     nullableString(model.Schema),
		Tags:           tagsJSON,
		Tests:          testsJSON,
		Meta:           metaJSON,
		UsesSelectStar: usesSelectStar,
		CreatedAt:      model.CreatedAt,
		UpdatedAt:      model.UpdatedAt,
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

// convertModel converts a sqlcgen.Model to a state.Model (core.PersistedModel).
func convertModel(row sqlcgen.Model) (*Model, error) {
	// Create the embedded core.Model first
	coreModel := &core.Model{
		Path:         row.Path,
		Name:         row.Name,
		Materialized: row.Materialized,
	}

	if row.UniqueKey != nil {
		coreModel.UniqueKey = *row.UniqueKey
	}
	if row.FilePath != nil {
		coreModel.FilePath = *row.FilePath
	}
	if row.Owner != nil {
		coreModel.Owner = *row.Owner
	}
	if row.SchemaName != nil {
		coreModel.Schema = *row.SchemaName
	}
	// Convert int64 to bool for UsesSelectStar
	if row.UsesSelectStar != nil && *row.UsesSelectStar == 1 {
		coreModel.UsesSelectStar = true
	}

	// Deserialize JSON fields into core.Model
	if err := deserializeJSON(row.Tags, &coreModel.Tags); err != nil {
		return nil, fmt.Errorf("failed to deserialize tags: %w", err)
	}
	if err := deserializeJSON(row.Tests, &coreModel.Tests); err != nil {
		return nil, fmt.Errorf("failed to deserialize tests: %w", err)
	}
	if err := deserializeJSON(row.Meta, &coreModel.Meta); err != nil {
		return nil, fmt.Errorf("failed to deserialize meta: %w", err)
	}

	// Create the PersistedModel with embedded Model
	model := &Model{
		Model:       coreModel,
		ID:          row.ID,
		ContentHash: row.ContentHash,
		CreatedAt:   row.CreatedAt,
		UpdatedAt:   row.UpdatedAt,
	}

	return model, nil
}

// convertModelRun converts a sqlcgen.ModelRun to a state.ModelRun.
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
