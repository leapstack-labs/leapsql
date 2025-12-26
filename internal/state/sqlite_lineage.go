package state

import (
	"context"
	"fmt"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SaveModelColumns saves column lineage information for a model.
// This replaces any existing column information for the model.
func (s *SQLiteStore) SaveModelColumns(modelPath string, columns []core.ColumnInfo) error {
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
			TransformType: nullableString(string(col.TransformType)),
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
func (s *SQLiteStore) GetModelColumns(modelPath string) ([]core.ColumnInfo, error) {
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
	columns := make([]core.ColumnInfo, 0, len(colRows))

	for _, row := range colRows {
		col := core.ColumnInfo{
			Name:          row.ColumnName,
			Index:         int(row.ColumnIndex),
			TransformType: core.TransformType(derefString(row.TransformType)),
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
			columns[idx].Sources = append(columns[idx].Sources, core.SourceRef{
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
func (s *SQLiteStore) TraceColumnBackward(modelPath, columnName string) ([]core.TraceResult, error) {
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

	results := make([]core.TraceResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, core.TraceResult{
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
func (s *SQLiteStore) TraceColumnForward(modelPath, columnName string) ([]core.TraceResult, error) {
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

	results := make([]core.TraceResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, core.TraceResult{
			ModelPath:  row.ModelPath,
			ColumnName: row.ColumnName,
			Depth:      int(row.Depth),
		})
	}

	return results, nil
}
