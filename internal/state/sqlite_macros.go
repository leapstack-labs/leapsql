package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SaveMacroNamespace stores a macro namespace and its functions.
// This replaces any existing functions for the namespace.
func (s *SQLiteStore) SaveMacroNamespace(ns *core.MacroNamespace, functions []*core.MacroFunction) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.queries.WithTx(tx)

	// Upsert namespace
	if err := qtx.UpsertMacroNamespace(ctx(), sqlcgen.UpsertMacroNamespaceParams{
		Name:     ns.Name,
		FilePath: ns.FilePath,
		Package:  nullableString(ns.Package),
	}); err != nil {
		return fmt.Errorf("failed to upsert namespace: %w", err)
	}

	// Delete old functions for this namespace
	if err := qtx.DeleteMacroFunctionsByNamespace(ctx(), ns.Name); err != nil {
		return fmt.Errorf("failed to delete old functions: %w", err)
	}

	// Insert functions
	for _, fn := range functions {
		argsJSON, _ := json.Marshal(fn.Args)
		line := int64(fn.Line)
		if err := qtx.InsertMacroFunction(ctx(), sqlcgen.InsertMacroFunctionParams{
			Namespace: ns.Name,
			Name:      fn.Name,
			Args:      string(argsJSON),
			Docstring: nullableString(fn.Docstring),
			Line:      &line,
		}); err != nil {
			return fmt.Errorf("failed to insert function %s: %w", fn.Name, err)
		}
	}

	return tx.Commit()
}

// GetMacroNamespaces returns all macro namespaces.
func (s *SQLiteStore) GetMacroNamespaces() ([]*core.MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.GetMacroNamespaces(ctx())
	if err != nil {
		return nil, fmt.Errorf("failed to get namespaces: %w", err)
	}

	namespaces := make([]*core.MacroNamespace, 0, len(rows))
	for _, row := range rows {
		namespaces = append(namespaces, convertMacroNamespace(row))
	}

	return namespaces, nil
}

// GetMacroNamespace returns a single macro namespace by name.
func (s *SQLiteStore) GetMacroNamespace(name string) (*core.MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetMacroNamespace(ctx(), name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	return convertMacroNamespace(row), nil
}

// GetMacroFunctions returns all functions for a namespace.
func (s *SQLiteStore) GetMacroFunctions(namespace string) ([]*core.MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.GetMacroFunctions(ctx(), namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get functions: %w", err)
	}

	functions := make([]*core.MacroFunction, 0, len(rows))
	for _, row := range rows {
		functions = append(functions, convertMacroFunction(row))
	}

	return functions, nil
}

// GetMacroFunction returns a single function by namespace and name.
func (s *SQLiteStore) GetMacroFunction(namespace, name string) (*core.MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	row, err := s.queries.GetMacroFunction(ctx(), sqlcgen.GetMacroFunctionParams{
		Namespace: namespace,
		Name:      name,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get function: %w", err)
	}

	return convertMacroFunction(row), nil
}

// MacroFunctionExists checks if a macro function exists.
func (s *SQLiteStore) MacroFunctionExists(namespace, name string) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database not opened")
	}

	count, err := s.queries.MacroFunctionExists(ctx(), sqlcgen.MacroFunctionExistsParams{
		Namespace: namespace,
		Name:      name,
	})
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// SearchMacroNamespaces searches namespaces by prefix.
func (s *SQLiteStore) SearchMacroNamespaces(prefix string) ([]*core.MacroNamespace, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.SearchMacroNamespaces(ctx(), &prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to search namespaces: %w", err)
	}

	namespaces := make([]*core.MacroNamespace, 0, len(rows))
	for _, row := range rows {
		namespaces = append(namespaces, convertMacroNamespace(row))
	}

	return namespaces, nil
}

// SearchMacroFunctions searches functions within a namespace by prefix.
func (s *SQLiteStore) SearchMacroFunctions(namespace, prefix string) ([]*core.MacroFunction, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	rows, err := s.queries.SearchMacroFunctions(ctx(), sqlcgen.SearchMacroFunctionsParams{
		Namespace: namespace,
		Column2:   &prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search functions: %w", err)
	}

	functions := make([]*core.MacroFunction, 0, len(rows))
	for _, row := range rows {
		functions = append(functions, convertMacroFunction(row))
	}

	return functions, nil
}

// DeleteMacroNamespace deletes a namespace and all its functions.
func (s *SQLiteStore) DeleteMacroNamespace(name string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.DeleteMacroNamespace(ctx(), name)
}

// DeleteMacroNamespaceByFilePath deletes a namespace by its file path.
func (s *SQLiteStore) DeleteMacroNamespaceByFilePath(filePath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.DeleteMacroNamespaceByFilePath(ctx(), filePath)
}

// ListMacroFilePaths returns all file paths of tracked macro namespaces.
func (s *SQLiteStore) ListMacroFilePaths() ([]string, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database not opened")
	}

	return s.queries.ListMacroFilePaths(ctx())
}

// convertMacroNamespace converts a sqlcgen.MacroNamespace to a core.MacroNamespace.
func convertMacroNamespace(row sqlcgen.MacroNamespace) *core.MacroNamespace {
	ns := &core.MacroNamespace{
		Name:     row.Name,
		FilePath: row.FilePath,
	}

	if row.Package != nil {
		ns.Package = *row.Package
	}
	if row.UpdatedAt != nil {
		ns.UpdatedAt = row.UpdatedAt.Format(time.RFC3339)
	}

	return ns
}

// convertMacroFunction converts a sqlcgen.MacroFunction to a core.MacroFunction.
func convertMacroFunction(row sqlcgen.MacroFunction) *core.MacroFunction {
	fn := &core.MacroFunction{
		Namespace: row.Namespace,
		Name:      row.Name,
	}

	// Parse args JSON
	if row.Args != "" {
		_ = json.Unmarshal([]byte(row.Args), &fn.Args)
	}

	if row.Docstring != nil {
		fn.Docstring = *row.Docstring
	}
	if row.Line != nil {
		fn.Line = int(*row.Line)
	}

	return fn
}
