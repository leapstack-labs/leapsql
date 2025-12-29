package state

//go:generate sqlc generate

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
	_ "modernc.org/sqlite" // sqlite3 driver (pure Go)
)

//go:embed schema.sql
var schemaSQL string

// SQLiteStore implements Store using SQLite with sqlc-generated queries.
type SQLiteStore struct {
	db      *sql.DB
	queries *sqlcgen.Queries
	path    string
	logger  *slog.Logger
}

// NewSQLiteStore creates a new SQLite state store instance.
func NewSQLiteStore(logger *slog.Logger) *SQLiteStore {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &SQLiteStore{logger: logger}
}

// Open opens a connection to the SQLite database.
// Use ":memory:" for an in-memory database.
func (s *SQLiteStore) Open(path string) error {
	s.logger.Debug("opening state database", slog.String("path", path))

	// Enable foreign keys and WAL mode for better performance
	var dsn string
	if path != ":memory:" {
		dsn = fmt.Sprintf("%s?_foreign_keys=on&_journal_mode=WAL", path)
	} else {
		dsn = ":memory:?_foreign_keys=on"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Test connection
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	s.db = db
	s.path = path
	s.queries = sqlcgen.New(db)
	return nil
}

// Close closes the SQLite database connection.
func (s *SQLiteStore) Close() error {
	if s.db != nil {
		s.logger.Debug("closing state database", slog.String("path", s.path))
		return s.db.Close()
	}
	return nil
}

// InitSchema initializes the database schema.
func (s *SQLiteStore) InitSchema() error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	s.logger.Debug("initializing database schema")

	_, err := s.db.ExecContext(context.Background(), schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}
	return nil
}

// Ensure SQLiteStore implements Store interface
var _ core.Store = (*SQLiteStore)(nil)

// --- Helper functions ---

// generateID creates a new UUID.
func generateID() string {
	return uuid.New().String()
}

// ctx returns a background context for operations.
func ctx() context.Context {
	return context.Background()
}

// nullableString converts an empty string to nil, otherwise returns a pointer.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// derefString safely dereferences a string pointer.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// serializeJSONPtr serializes a value to JSON and returns a string pointer.
func serializeJSONPtr(v any) *string {
	if v == nil {
		return nil
	}

	// Check for empty slices and maps
	switch val := v.(type) {
	case []string:
		if len(val) == 0 {
			return nil
		}
	case []core.TestConfig:
		if len(val) == 0 {
			return nil
		}
	case map[string]any:
		if len(val) == 0 {
			return nil
		}
	}

	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(data)
	return &s
}

// deserializeJSON deserializes JSON from a string pointer into a target.
func deserializeJSON(data *string, target any) error {
	if data == nil || *data == "" {
		return nil
	}
	return json.Unmarshal([]byte(*data), target)
}
