package adapter

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// BaseSQLAdapter provides common database/sql functionality for adapters.
// Embed this struct in concrete adapter implementations to get standard
// Close, Exec, and Query implementations.
type BaseSQLAdapter struct {
	DB     *sql.DB
	Cfg    core.AdapterConfig
	Logger *slog.Logger
}

// Close closes the database connection.
func (b *BaseSQLAdapter) Close() error {
	if b.DB != nil {
		if b.Logger != nil {
			b.Logger.Debug("closing database connection")
		}
		return b.DB.Close()
	}
	return nil
}

// Exec executes a SQL statement that doesn't return rows.
func (b *BaseSQLAdapter) Exec(ctx context.Context, sqlStr string) error {
	if b.DB == nil {
		return fmt.Errorf("database connection not established")
	}
	_, err := b.DB.ExecContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("failed to execute SQL: %w", err)
	}
	return nil
}

// Query executes a SQL statement that returns rows.
func (b *BaseSQLAdapter) Query(ctx context.Context, sqlStr string) (*core.Rows, error) {
	if b.DB == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	//nolint:rowserrcheck // rows.Err() must be checked by caller after iteration completes
	rows, err := b.DB.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return &core.Rows{Rows: rows}, nil
}

// IsConnected returns true if the database connection is established.
func (b *BaseSQLAdapter) IsConnected() bool {
	return b.DB != nil
}

// ParseQualifiedName splits a table reference into schema and name.
// Uses the dialect's default schema if not specified.
func ParseQualifiedName(table string, d *dialect.Dialect) (schema, name string) {
	if parts := strings.Split(table, "."); len(parts) == 2 {
		return parts[0], parts[1]
	}
	return d.DefaultSchema, table
}

// GetTableMetadataCommon provides a shared implementation of GetTableMetadata.
// Uses information_schema.columns with dialect-appropriate placeholders.
// This can be called by concrete adapters to avoid code duplication.
func (b *BaseSQLAdapter) GetTableMetadataCommon(ctx context.Context, table string, d *dialect.Dialect) (*core.TableMetadata, error) {
	if b.DB == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	schema, tableName := ParseQualifiedName(table, d)

	// Build query with appropriate placeholders
	// The placeholders come from the dialect and are safe (? or $N)
	//nolint:gosec // Placeholders are safe - they come from dialect.FormatPlaceholder
	query := fmt.Sprintf(`
		SELECT 
			column_name,
			data_type,
			is_nullable,
			ordinal_position
		FROM information_schema.columns 
		WHERE table_schema = %s AND table_name = %s
		ORDER BY ordinal_position
	`, d.FormatPlaceholder(1), d.FormatPlaceholder(2))

	rows, err := b.DB.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []core.Column
	for rows.Next() {
		var col core.Column
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			return nil, fmt.Errorf("failed to scan column metadata: %w", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column metadata: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found", table)
	}

	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName) //nolint:gosec // Table names are from metadata
	var rowCount int64
	if err := b.DB.QueryRowContext(ctx, countQuery).Scan(&rowCount); err != nil {
		// Non-fatal error, just set to 0
		rowCount = 0
	}

	return &core.TableMetadata{
		Schema:   schema,
		Name:     tableName,
		Columns:  columns,
		RowCount: rowCount,
	}, nil
}
