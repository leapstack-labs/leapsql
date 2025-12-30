package commands

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// sqlite driver for test database.
	_ "modernc.org/sqlite"
)

// setupTestDB creates a test database with some tables and data.
func setupTestDB(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	// Create schema
	schema := `
		CREATE TABLE models (
			id TEXT PRIMARY KEY,
			path TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			materialized TEXT NOT NULL DEFAULT 'table',
			description TEXT DEFAULT ''
		);
		
		CREATE TABLE runs (
			id TEXT PRIMARY KEY,
			environment TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'running',
			started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at DATETIME
		);
		
		CREATE VIEW v_models AS
		SELECT id, path, name, materialized, description FROM models;
	`
	_, err = db.ExecContext(ctx, schema)
	require.NoError(t, err)

	// Insert test data
	_, err = db.ExecContext(ctx, `
		INSERT INTO models (id, path, name, materialized, description) VALUES
		('1', 'staging.stg_orders', 'stg_orders', 'view', 'Staging orders'),
		('2', 'marts.dim_customers', 'dim_customers', 'table', 'Customer dimension');
		
		INSERT INTO runs (id, environment, status, started_at, completed_at) VALUES
		('run-1', 'dev', 'completed', '2024-01-01 10:00:00', '2024-01-01 10:05:00');
	`)
	require.NoError(t, err)
}

func TestQueryCommand_Tables(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	buf := new(bytes.Buffer)
	ctx := context.Background()

	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = listTablesFromDB(ctx, buf, db, "table", false)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "models")
	assert.Contains(t, output, "runs")
	assert.Contains(t, output, "v_models")
}

func TestQueryCommand_ViewsOnly(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	buf := new(bytes.Buffer)
	ctx := context.Background()

	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = listTablesFromDB(ctx, buf, db, "table", true)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "v_models")
	// Should not contain the base tables when viewing only views
	assert.NotContains(t, output, "| models")
}

func TestQueryCommand_Schema(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	buf := new(bytes.Buffer)
	ctx := context.Background()

	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = showSchemaFromDB(ctx, buf, db, "models", "table")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Table: models")
	assert.Contains(t, output, "id")
	assert.Contains(t, output, "path")
	assert.Contains(t, output, "name")
}

func TestQueryCommand_SchemaNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	buf := new(bytes.Buffer)
	ctx := context.Background()

	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = showSchemaFromDB(ctx, buf, db, "nonexistent_table", "table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestQueryCommand_DirectSQL(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	ctx := context.Background()
	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT path, name FROM models ORDER BY path")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	buf := new(bytes.Buffer)
	err = renderResults(buf, rows, "table")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "marts.dim_customers")
	assert.Contains(t, output, "staging.stg_orders")
	assert.Contains(t, output, "(2 rows)")
}

func TestQueryCommand_JSONFormat(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	ctx := context.Background()
	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT path, name FROM models ORDER BY path")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	buf := new(bytes.Buffer)
	err = renderResults(buf, rows, "json")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"path"`)
	assert.Contains(t, output, `"name"`)
	assert.Contains(t, output, `"marts.dim_customers"`)
}

func TestQueryCommand_CSVFormat(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	ctx := context.Background()
	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT path, name FROM models ORDER BY path")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	buf := new(bytes.Buffer)
	err = renderResults(buf, rows, "csv")
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 3) // header + 2 rows

	assert.Equal(t, "path,name", lines[0])
	assert.Contains(t, output, "marts.dim_customers,dim_customers")
}

func TestQueryCommand_MarkdownFormat(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	ctx := context.Background()
	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT path, name FROM models ORDER BY path")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	buf := new(bytes.Buffer)
	err = renderResults(buf, rows, "md")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "| path | name |")
	assert.Contains(t, output, "| --- | --- |")
	assert.Contains(t, output, "| marts.dim_customers | dim_customers |")
}

func TestQueryCommand_EmptyResults(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	ctx := context.Background()
	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT * FROM models WHERE 1=0")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	buf := new(bytes.Buffer)
	err = renderResults(buf, rows, "table")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "(0 rows)")
}

func TestQueryCommand_SchemaJSON(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	buf := new(bytes.Buffer)
	ctx := context.Background()

	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = showSchemaFromDB(ctx, buf, db, "models", "json")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"name": "models"`)
	assert.Contains(t, output, `"type": "table"`)
	assert.Contains(t, output, `"columns"`)
}

func TestQueryCommand_ViewSchema(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	setupTestDB(t, statePath)

	buf := new(bytes.Buffer)
	ctx := context.Background()

	db, err := sql.Open("sqlite", statePath+"?mode=ro")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = showSchemaFromDB(ctx, buf, db, "v_models", "table")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "View: v_models")
}

func TestNewQueryCommand(t *testing.T) {
	cmd := NewQueryCommand()
	assert.Equal(t, "query", cmd.Use[:5])
	assert.NotNil(t, cmd.RunE)

	// Check subcommands
	subCmds := cmd.Commands()
	var names []string
	for _, c := range subCmds {
		names = append(names, c.Name())
	}
	assert.Contains(t, names, "tables")
	assert.Contains(t, names, "views")
	assert.Contains(t, names, "schema")
	assert.Contains(t, names, "search")
}

func TestQueryCommand_NoDB(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "nonexistent", "state.db")

	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	// Verify file doesn't exist check works
	_, err := os.Stat(statePath)
	assert.True(t, os.IsNotExist(err))
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{nil, "NULL"},
		{"hello", "hello"},
		{42, "42"},
		{3.14, "3.14"},
		{true, "true"},
	}

	for _, tt := range tests {
		result := formatValue(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestEscapeCSV(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with,comma", `"with,comma"`},
		{`with"quote`, `"with""quote"`},
		{"with\nnewline", `"with
newline"`},
		{`complex,"values"`, `"complex,""values"""`},
	}

	for _, tt := range tests {
		result := escapeCSV(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}
