package state

//go:generate sqlc generate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
	_ "modernc.org/sqlite" // sqlite3 driver (pure Go)
)

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

// InitSchema initializes the database schema using Goose migrations.
func (s *SQLiteStore) InitSchema() error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	s.logger.Debug("initializing database schema via migrations")

	// Use Goose migrations for schema management
	if err := s.Migrate(); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// DB returns the underlying database connection.
// This is useful for direct queries or testing.
func (s *SQLiteStore) DB() *sql.DB {
	return s.db
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

// SearchModelResult represents a model search result from FTS5.
type SearchModelResult struct {
	Path         string
	Name         string
	Folder       string
	Materialized string
	Description  *string
}

// SearchModels searches for models using FTS5 full-text search.
// The query uses FTS5 MATCH syntax (e.g., "customer*" for prefix match).
func (s *SQLiteStore) SearchModels(query string) ([]SearchModelResult, error) {
	const fts5Query = `
		SELECT v.path, v.name, v.folder, v.materialized, v.description
		FROM models m
		JOIN models_fts fts ON m.rowid = fts.rowid
		JOIN v_models v ON m.path = v.path
		WHERE models_fts MATCH ?
		ORDER BY rank
		LIMIT 20
	`

	rows, err := s.db.QueryContext(ctx(), fts5Query, query)
	if err != nil {
		return nil, fmt.Errorf("search models: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []SearchModelResult
	for rows.Next() {
		var r SearchModelResult
		if err := rows.Scan(&r.Path, &r.Name, &r.Folder, &r.Materialized, &r.Description); err != nil {
			return nil, fmt.Errorf("scan search result: %w", err)
		}
		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search results: %w", err)
	}

	return results, nil
}
