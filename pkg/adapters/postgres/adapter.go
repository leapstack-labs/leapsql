// Package postgres provides a PostgreSQL database adapter for LeapSQL.
package postgres

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/leapstack-labs/leapsql/pkg/adapter"
	pgdialect "github.com/leapstack-labs/leapsql/pkg/adapters/postgres/dialect"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// Adapter implements the adapter.Adapter interface for PostgreSQL.
type Adapter struct {
	adapter.BaseSQLAdapter
}

// New creates a new PostgreSQL adapter instance.
// If logger is nil, a discard logger is used.
func New(logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Adapter{
		BaseSQLAdapter: adapter.BaseSQLAdapter{Logger: logger},
	}
}

// Dialect returns the SQL dialect configuration for this adapter.
func (a *Adapter) Dialect() *dialect.Dialect {
	return pgdialect.Postgres
}

// Connect establishes a connection to PostgreSQL.
func (a *Adapter) Connect(ctx context.Context, cfg core.AdapterConfig) error {
	dsn := buildPostgresDSN(cfg)

	a.Logger.Debug("connecting to postgres", slog.String("host", cfg.Host), slog.String("database", cfg.Database))

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	a.DB = db
	a.Cfg = cfg
	return nil
}

// buildPostgresDSN constructs a PostgreSQL connection string.
func buildPostgresDSN(cfg core.AdapterConfig) string {
	// Build key=value format: host=localhost port=5432 user=postgres ...
	host := cfg.Host
	if host == "" {
		host = "localhost"
	}

	port := cfg.Port
	if port == 0 {
		port = 5432
	}

	sslmode := "disable"
	if cfg.Options != nil {
		if mode, ok := cfg.Options["sslmode"]; ok {
			sslmode = mode
		}
	}

	// Build DSN
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=%s",
		host, port, cfg.Database, sslmode)

	if cfg.Username != "" {
		dsn += fmt.Sprintf(" user=%s", cfg.Username)
	}
	if cfg.Password != "" {
		dsn += fmt.Sprintf(" password=%s", cfg.Password)
	}

	return dsn
}

// GetTableMetadata retrieves metadata for a specified table.
func (a *Adapter) GetTableMetadata(ctx context.Context, table string) (*core.TableMetadata, error) {
	return a.GetTableMetadataCommon(ctx, table, a.Dialect())
}

// LoadCSV loads data from a CSV file into a table using COPY FROM STDIN.
// All columns are created as TEXT type for robustness.
func (a *Adapter) LoadCSV(ctx context.Context, tableName string, filePath string) error {
	if a.DB == nil {
		return fmt.Errorf("database connection not established")
	}

	// Get absolute path
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Open CSV file
	file, err := os.Open(absPath) //nolint:gosec // absPath is derived from user-provided filePath, which is expected
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Read CSV header to get column names
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Create table with TEXT columns
	if err := a.createTextTable(ctx, tableName, headers); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Reset file to beginning
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to reset file: %w", err)
	}

	// Use COPY FROM STDIN to load data
	if err := a.copyFromCSV(ctx, tableName, file); err != nil {
		return fmt.Errorf("failed to copy data: %w", err)
	}

	return nil
}

// createTextTable creates or replaces a table with all TEXT columns.
func (a *Adapter) createTextTable(ctx context.Context, tableName string, columns []string) error {
	// Drop existing table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := a.DB.ExecContext(ctx, dropSQL); err != nil {
		return err
	}

	// Build CREATE TABLE with TEXT columns
	var colDefs []string
	for _, col := range columns {
		// Sanitize column name using dialect
		safeName := sanitizeIdentifier(col, a.Dialect())
		colDefs = append(colDefs, fmt.Sprintf("%s TEXT", safeName))
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(colDefs, ", "))
	_, err := a.DB.ExecContext(ctx, createSQL)
	return err
}

// copyFromCSV uses PostgreSQL COPY to load CSV data.
func (a *Adapter) copyFromCSV(ctx context.Context, tableName string, file *os.File) error {
	// Get the underlying pgx connection for COPY support
	conn, err := a.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Use raw connection for COPY
	return conn.Raw(func(driverConn any) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()

		// Read entire file content
		content, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Execute COPY FROM STDIN
		copySQL := fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT csv, HEADER true)", tableName)
		_, err = pgxConn.PgConn().CopyFrom(ctx, strings.NewReader(string(content)), copySQL)
		return err
	})
}

// sanitizeIdentifier makes a column name safe for SQL using the dialect's reserved word list.
func sanitizeIdentifier(name string, d *dialect.Dialect) string {
	// Replace problematic characters
	safe := strings.ReplaceAll(name, " ", "_")
	safe = strings.ReplaceAll(safe, "-", "_")
	// Quote if it contains special chars or is a reserved word
	if strings.ContainsAny(safe, "()[]{}") || d.IsReservedWord(safe) {
		return d.QuoteIdentifier(safe)
	}
	return safe
}

// Ensure Adapter implements adapter.Adapter interface
var _ adapter.Adapter = (*Adapter)(nil)
