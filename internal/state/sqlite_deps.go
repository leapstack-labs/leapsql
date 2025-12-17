package state

import (
	"context"
	"fmt"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
)

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
