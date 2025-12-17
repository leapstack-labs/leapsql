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

// DialectName returns the SQL dialect for this adapter.
func (a *Adapter) DialectName() string {
	return "postgres"
}

// Connect establishes a connection to PostgreSQL.
func (a *Adapter) Connect(ctx context.Context, cfg adapter.Config) error {
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
func buildPostgresDSN(cfg adapter.Config) string {
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
func (a *Adapter) GetTableMetadata(ctx context.Context, table string) (*adapter.Metadata, error) {
	if a.DB == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Parse schema.table if provided
	schema := "public"
	tableName := table
	if parts := strings.Split(table, "."); len(parts) == 2 {
		schema = parts[0]
		tableName = parts[1]
	}

	// Query column information
	query := `
		SELECT 
			column_name,
			data_type,
			is_nullable,
			ordinal_position
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := a.DB.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []adapter.Column
	for rows.Next() {
		var col adapter.Column
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
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName) //nolint:gosec // Table names are validated by caller
	var rowCount int64
	if err := a.DB.QueryRowContext(ctx, countQuery).Scan(&rowCount); err != nil {
		rowCount = 0
	}

	return &adapter.Metadata{
		Schema:   schema,
		Name:     tableName,
		Columns:  columns,
		RowCount: rowCount,
	}, nil
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
		// Sanitize column name
		safeName := sanitizeIdentifier(col)
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

// sanitizeIdentifier makes a column name safe for SQL.
func sanitizeIdentifier(name string) string {
	// Replace problematic characters
	safe := strings.ReplaceAll(name, " ", "_")
	safe = strings.ReplaceAll(safe, "-", "_")
	// Quote if it contains special chars or is a reserved word
	if strings.ContainsAny(safe, "()[]{}") || isReservedWord(safe) {
		return fmt.Sprintf(`"%s"`, safe)
	}
	return safe
}

// isReservedWord checks if a name is a PostgreSQL reserved word.
func isReservedWord(name string) bool {
	reserved := map[string]bool{
		"user": true, "order": true, "group": true, "table": true,
		"select": true, "from": true, "where": true, "index": true,
	}
	return reserved[strings.ToLower(name)]
}

// Ensure Adapter implements adapter.Adapter interface
var _ adapter.Adapter = (*Adapter)(nil)
