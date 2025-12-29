// Package state provides state management with database migrations.
package state

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate runs all pending database migrations.
func (s *SQLiteStore) Migrate() error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	// Configure goose for embedded migrations
	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("sqlite"); err != nil {
		return fmt.Errorf("failed to set dialect: %w", err)
	}

	// Run migrations
	if err := goose.Up(s.db, "migrations"); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// MigrateWithDB runs migrations using a raw database connection.
// This is useful for testing or when you have a db connection from elsewhere.
func MigrateWithDB(db *sql.DB) error {
	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("sqlite"); err != nil {
		return fmt.Errorf("failed to set dialect: %w", err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// GetMigrationVersion returns the current migration version.
func (s *SQLiteStore) GetMigrationVersion() (int64, error) {
	if s.db == nil {
		return 0, fmt.Errorf("database not opened")
	}

	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("sqlite"); err != nil {
		return 0, fmt.Errorf("failed to set dialect: %w", err)
	}

	return goose.GetDBVersion(s.db)
}
