package state

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
)

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

// convertRun converts a sqlcgen.Run to a state.Run.
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
