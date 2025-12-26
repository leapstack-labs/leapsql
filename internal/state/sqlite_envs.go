package state

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// CreateEnvironment creates a new environment.
func (s *SQLiteStore) CreateEnvironment(name string) (*core.Environment, error) {
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
func (s *SQLiteStore) GetEnvironment(name string) (*core.Environment, error) {
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

// convertEnvironment converts a sqlcgen.Environment to a core.Environment.
func convertEnvironment(row sqlcgen.Environment) *core.Environment {
	env := &core.Environment{
		Name:      row.Name,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}

	if row.CommitRef != nil {
		env.CommitRef = *row.CommitRef
	}

	return env
}
