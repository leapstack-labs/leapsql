package state

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// CreateRun creates a new pipeline run.
func (s *SQLiteStore) CreateRun(env string) (*core.Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	id := generateID()
	now := time.Now().UTC()

	s.logger.Debug("creating run", slog.String("id", id), slog.String("environment", env))

	row, err := s.queries.CreateRun(ctx(), sqlcgen.CreateRunParams{
		ID:          id,
		Environment: env,
		Status:      string(core.RunStatusRunning),
		StartedAt:   now,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	return convertRun(row), nil
}

// GetRun retrieves a run by ID.
func (s *SQLiteStore) GetRun(id string) (*core.Run, error) {
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
func (s *SQLiteStore) CompleteRun(id string, status core.RunStatus, errMsg string) error {
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
func (s *SQLiteStore) GetLatestRun(env string) (*core.Run, error) {
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

// ListRuns retrieves the most recent runs up to the given limit.
func (s *SQLiteStore) ListRuns(limit int) ([]*core.Run, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.ListRuns(ctx(), int64(limit))
	if err != nil {
		return nil, fmt.Errorf("failed to list runs: %w", err)
	}

	runs := make([]*core.Run, len(rows))
	for i, row := range rows {
		runs[i] = convertRun(row)
	}

	return runs, nil
}

// convertRun converts a sqlcgen.Run to a core.Run.
func convertRun(row sqlcgen.Run) *core.Run {
	run := &core.Run{
		ID:          row.ID,
		Environment: row.Environment,
		Status:      core.RunStatus(row.Status),
		StartedAt:   row.StartedAt,
		CompletedAt: row.CompletedAt,
	}
	if row.Error != nil {
		run.Error = *row.Error
	}
	return run
}
