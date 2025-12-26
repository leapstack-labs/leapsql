package state

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// BatchGetAllColumns returns all columns for all models in one query.
// This reduces N+1 query problems when building project context.
// Returns a map of modelPath -> []core.ColumnInfo.
func (s *SQLiteStore) BatchGetAllColumns() (map[string][]core.ColumnInfo, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	// Get all columns
	colRows, err := s.queries.BatchGetAllColumns(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to batch get columns: %w", err)
	}

	// Build column map
	result := make(map[string][]core.ColumnInfo)
	columnsIdxMap := make(map[string]map[string]int) // modelPath -> columnName -> index in slice

	for _, row := range colRows {
		col := core.ColumnInfo{
			Name:          row.ColumnName,
			Index:         int(row.ColumnIndex),
			TransformType: core.TransformType(derefString(row.TransformType)),
			Function:      derefString(row.FunctionName),
		}

		if _, ok := columnsIdxMap[row.ModelPath]; !ok {
			columnsIdxMap[row.ModelPath] = make(map[string]int)
		}
		columnsIdxMap[row.ModelPath][col.Name] = len(result[row.ModelPath])
		result[row.ModelPath] = append(result[row.ModelPath], col)
	}

	// Get all column lineage
	lineageRows, err := s.queries.BatchGetAllColumnLineage(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to batch get column lineage: %w", err)
	}

	// Attach sources to columns
	for _, row := range lineageRows {
		if modelIdx, ok := columnsIdxMap[row.ModelPath]; ok {
			if idx, ok := modelIdx[row.ColumnName]; ok {
				result[row.ModelPath][idx].Sources = append(
					result[row.ModelPath][idx].Sources,
					core.SourceRef{
						Table:  row.SourceTable,
						Column: row.SourceColumn,
					},
				)
			}
		}
	}

	return result, nil
}

// BatchGetAllDependencies returns all dependencies in one query.
// Returns a map of modelID -> []parentID (models this model depends on).
func (s *SQLiteStore) BatchGetAllDependencies() (map[string][]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.BatchGetAllDependencies(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to batch get dependencies: %w", err)
	}

	result := make(map[string][]string)
	for _, row := range rows {
		result[row.ModelID] = append(result[row.ModelID], row.ParentID)
	}

	return result, nil
}

// BatchGetAllDependents returns all dependents in one query.
// Returns a map of modelID -> []dependentID (models that depend on this model).
func (s *SQLiteStore) BatchGetAllDependents() (map[string][]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.BatchGetAllDependents(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to batch get dependents: %w", err)
	}

	result := make(map[string][]string)
	for _, row := range rows {
		result[row.ParentID] = append(result[row.ParentID], row.ModelID)
	}

	return result, nil
}

// Ensure SQLiteStore implements Store interface batch methods
var _ interface {
	BatchGetAllColumns() (map[string][]core.ColumnInfo, error)
	BatchGetAllDependencies() (map[string][]string, error)
	BatchGetAllDependents() (map[string][]string, error)
} = (*SQLiteStore)(nil)

// Blank import to ensure sqlcgen is used
var _ sqlcgen.BatchGetAllDependentsRow
