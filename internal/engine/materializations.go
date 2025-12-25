package engine

// materializations.go - Execution strategies for different materialization types

import (
	"context"
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/loader"
	"github.com/leapstack-labs/leapsql/internal/state"
)

// executeTable creates or replaces a table.
func (e *Engine) executeTable(ctx context.Context, path, sql string) (int64, error) {
	tableName := pathToTableName(path)

	// Drop existing table
	_ = e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create schema if needed
	parts := strings.Split(path, ".")
	if len(parts) > 1 {
		schema := parts[0]
		_ = e.db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	}

	// Create new table
	createSQL := fmt.Sprintf("CREATE TABLE %s AS %s", tableName, sql)
	if err := e.db.Exec(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Get row count
	rows, err := e.db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		return 0, nil // Table created but can't get count
	}
	defer func() { _ = rows.Close() }()

	var count int64
	if rows.Next() {
		_ = rows.Scan(&count)
	}

	return count, nil
}

// executeView creates or replaces a view.
func (e *Engine) executeView(ctx context.Context, path, sql string) (int64, error) {
	tableName := pathToTableName(path)

	// Drop existing view
	_ = e.db.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", tableName))

	// Create schema if needed
	parts := strings.Split(path, ".")
	if len(parts) > 1 {
		schema := parts[0]
		_ = e.db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	}

	// Create new view
	createSQL := fmt.Sprintf("CREATE VIEW %s AS %s", tableName, sql)
	if err := e.db.Exec(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create view %s: %w", tableName, err)
	}

	return 0, nil // Views don't affect rows
}

// executeIncremental handles incremental model execution.
func (e *Engine) executeIncremental(ctx context.Context, m *loader.ModelConfig, _ *state.Model, sql string) (int64, error) {
	tableName := pathToTableName(m.Path)

	// Check if table exists
	_, err := e.db.GetTableMetadata(ctx, tableName)
	tableExists := err == nil

	if !tableExists {
		// First run - create table with full data
		return e.executeTable(ctx, m.Path, sql)
	}

	// Table exists - check if we have incremental SQL
	incrementalSQL := sql
	if len(m.Conditionals) > 0 {
		// Apply incremental conditional
		for _, cond := range m.Conditionals {
			if strings.Contains(cond.Condition, "is_incremental") {
				// Process template replacements in conditional content
				condContent := cond.Content
				condContent = strings.ReplaceAll(condContent, "{{ this }}", tableName)
				incrementalSQL = sql + "\n" + condContent
				break
			}
		}
	}

	// Insert new rows using unique key for deduplication
	if m.UniqueKey != "" {
		// Merge/upsert pattern
		tempTable := tableName + "_temp"

		// Create temp table with new data
		_ = e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS %s", tempTable, incrementalSQL)
		if err := e.db.Exec(ctx, createTempSQL); err != nil {
			return 0, fmt.Errorf("failed to create temp table: %w", err)
		}

		// Delete matching rows from target
		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s)",
			tableName, m.UniqueKey, m.UniqueKey, tempTable)
		_ = e.db.Exec(ctx, deleteSQL)

		// Insert all rows from temp
		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableName, tempTable)
		if err := e.db.Exec(ctx, insertSQL); err != nil {
			return 0, fmt.Errorf("failed to insert incremental rows: %w", err)
		}

		// Get count from temp table
		rows, err := e.db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tempTable))
		var count int64
		if err == nil {
			defer func() { _ = rows.Close() }()
			if rows.Next() {
				_ = rows.Scan(&count)
			}
		}

		// Clean up temp table
		_ = e.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))

		return count, nil
	}

	// No unique key - simple append
	insertSQL := fmt.Sprintf("INSERT INTO %s %s", tableName, incrementalSQL)
	if err := e.db.Exec(ctx, insertSQL); err != nil {
		return 0, fmt.Errorf("failed to insert rows: %w", err)
	}

	return 0, nil
}
