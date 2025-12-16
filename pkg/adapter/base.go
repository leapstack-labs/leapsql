package adapter

import (
	"context"
	"database/sql"
	"fmt"
)

// BaseSQLAdapter provides common database/sql functionality for adapters.
// Embed this struct in concrete adapter implementations to get standard
// Close, Exec, and Query implementations.
type BaseSQLAdapter struct {
	DB  *sql.DB
	Cfg Config
}

// Close closes the database connection.
func (b *BaseSQLAdapter) Close() error {
	if b.DB != nil {
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
func (b *BaseSQLAdapter) Query(ctx context.Context, sqlStr string) (*Rows, error) {
	if b.DB == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	//nolint:rowserrcheck // rows.Err() must be checked by caller after iteration completes
	rows, err := b.DB.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return &Rows{Rows: rows}, nil
}

// IsConnected returns true if the database connection is established.
func (b *BaseSQLAdapter) IsConnected() bool {
	return b.DB != nil
}
