package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SaveColumnSnapshot stores the known-good column state after a successful run.
// This is used by the PL05 schema drift rule to detect changes in source columns.
func (s *SQLiteStore) SaveColumnSnapshot(runID string, modelPath string, sourceTable string, columns []string) error {
	if s.db == nil {
		return fmt.Errorf("database not open")
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Insert each column
	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR REPLACE INTO column_snapshots 
		(model_path, source_table, column_name, column_index, run_id)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for i, col := range columns {
		if _, err := stmt.ExecContext(ctx, modelPath, sourceTable, col, i, runID); err != nil {
			return fmt.Errorf("insert snapshot for column %s: %w", col, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// GetColumnSnapshot returns the last known-good columns for a source.
// Returns the column names in order and the run ID when they were captured.
func (s *SQLiteStore) GetColumnSnapshot(modelPath string, sourceTable string) ([]string, string, error) {
	if s.db == nil {
		return nil, "", fmt.Errorf("database not open")
	}

	ctx := context.Background()

	// Get the most recent run ID for this model/source combination
	var runID string
	err := s.db.QueryRowContext(ctx, `
		SELECT run_id FROM column_snapshots
		WHERE model_path = ? AND source_table = ?
		ORDER BY snapshot_at DESC
		LIMIT 1
	`, modelPath, sourceTable).Scan(&runID)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", nil // No snapshot exists
	}
	if err != nil {
		return nil, "", fmt.Errorf("get latest snapshot run: %w", err)
	}

	// Get all columns for this run
	rows, err := s.db.QueryContext(ctx, `
		SELECT column_name FROM column_snapshots
		WHERE model_path = ? AND source_table = ? AND run_id = ?
		ORDER BY column_index
	`, modelPath, sourceTable, runID)
	if err != nil {
		return nil, "", fmt.Errorf("query snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, "", fmt.Errorf("scan column: %w", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("rows error: %w", err)
	}

	return columns, runID, nil
}

// DeleteOldSnapshots removes snapshots older than the last N runs.
// This keeps the database size manageable while retaining recent history.
func (s *SQLiteStore) DeleteOldSnapshots(keepRuns int) error {
	if s.db == nil {
		return fmt.Errorf("database not open")
	}

	ctx := context.Background()
	// Delete snapshots from runs not in the most recent N
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM column_snapshots
		WHERE run_id NOT IN (
			SELECT DISTINCT run_id FROM column_snapshots
			ORDER BY snapshot_at DESC
			LIMIT ?
		)
	`, keepRuns)
	if err != nil {
		return fmt.Errorf("delete old snapshots: %w", err)
	}

	return nil
}
