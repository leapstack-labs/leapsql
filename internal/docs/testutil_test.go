package docs

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite" // SQLite driver (pure Go)

	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Database Infrastructure
// =============================================================================
//
// In-memory SQLite databases are a testing best practice:
// - Isolation: Each test gets a fresh database, no state leakage
// - Speed: No disk I/O, tests run much faster
// - Parallelism: Unique named DBs allow parallel test execution
// - No cleanup: Database disappears when connection closes
// =============================================================================

// testDBCounter generates unique names for in-memory test databases.
var testDBCounter atomic.Uint64

// TestMetadataDB provides an in-memory SQLite database for testing.
// This is test-only infrastructure that mirrors the schema from state.db.
type TestMetadataDB struct {
	db *sql.DB
}

// openTestMemoryDB creates an isolated in-memory SQLite database for testing.
func openTestMemoryDB() (*TestMetadataDB, error) {
	id := testDBCounter.Add(1)
	dsn := fmt.Sprintf("file:testdb%d?mode=memory&cache=shared", id)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open test database: %w", err)
	}
	return &TestMetadataDB{db: db}, nil
}

// Close closes the database connection.
func (m *TestMetadataDB) Close() error {
	return m.db.Close()
}

// DB returns the underlying database connection.
func (m *TestMetadataDB) DB() *sql.DB {
	return m.db
}

// testMetadataSchema mirrors the docs views from state.db for testing.
// This allows tests to run without the full state infrastructure.
const testMetadataSchema = `
-- Core tables
CREATE TABLE models (
    rowid INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    folder TEXT NOT NULL,
    materialized TEXT NOT NULL,
    unique_key TEXT,
    sql_content TEXT NOT NULL,
    file_path TEXT,
    description TEXT,
    updated_at DATETIME
);
CREATE INDEX idx_models_folder ON models(folder);
CREATE INDEX idx_models_name ON models(name);

CREATE TABLE sources (
    name TEXT PRIMARY KEY
);

CREATE TABLE model_sources (
    model_path TEXT NOT NULL,
    source_name TEXT NOT NULL,
    PRIMARY KEY (model_path, source_name),
    FOREIGN KEY (model_path) REFERENCES models(path),
    FOREIGN KEY (source_name) REFERENCES sources(name)
);

CREATE TABLE dependencies (
    model_path TEXT NOT NULL,
    parent_path TEXT NOT NULL,
    PRIMARY KEY (model_path, parent_path)
);
CREATE INDEX idx_dependencies_parent ON dependencies(parent_path);

CREATE TABLE dependents (
    model_path TEXT NOT NULL,
    dependent_path TEXT NOT NULL,
    PRIMARY KEY (model_path, dependent_path)
);
CREATE INDEX idx_dependents_model ON dependents(model_path);

CREATE TABLE columns (
    model_path TEXT NOT NULL,
    name TEXT NOT NULL,
    idx INTEGER NOT NULL,
    transform_type TEXT,
    function_name TEXT,
    PRIMARY KEY (model_path, name)
);
CREATE INDEX idx_columns_model ON columns(model_path);

CREATE TABLE column_sources (
    model_path TEXT NOT NULL,
    column_name TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    PRIMARY KEY (model_path, column_name, source_table, source_column)
);
CREATE INDEX idx_column_sources_lookup ON column_sources(model_path, column_name);
CREATE INDEX idx_column_sources_reverse ON column_sources(source_table, source_column);

CREATE TABLE source_refs (
    source_name TEXT NOT NULL,
    model_path TEXT NOT NULL,
    PRIMARY KEY (source_name, model_path)
);

CREATE TABLE lineage_edges (
    source_node TEXT NOT NULL,
    target_node TEXT NOT NULL,
    PRIMARY KEY (source_node, target_node)
);
CREATE INDEX idx_lineage_source ON lineage_edges(source_node);
CREATE INDEX idx_lineage_target ON lineage_edges(target_node);

CREATE TABLE column_lineage_nodes (
    id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    column_name TEXT NOT NULL
);

CREATE TABLE column_lineage_edges (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (source_id, target_id)
);
CREATE INDEX idx_col_lineage_source ON column_lineage_edges(source_id);
CREATE INDEX idx_col_lineage_target ON column_lineage_edges(target_id);

CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Full-text search (FTS5)
CREATE VIRTUAL TABLE models_fts USING fts5(
    name, path, description, sql_content,
    content='models',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER models_ai AFTER INSERT ON models BEGIN
    INSERT INTO models_fts(rowid, name, path, description, sql_content)
    VALUES (new.rowid, new.name, new.path, new.description, new.sql_content);
END;

CREATE TRIGGER models_ad AFTER DELETE ON models BEGIN
    INSERT INTO models_fts(models_fts, rowid, name, path, description, sql_content)
    VALUES('delete', old.rowid, old.name, old.path, old.description, old.sql_content);
END;

CREATE TRIGGER models_au AFTER UPDATE ON models BEGIN
    INSERT INTO models_fts(models_fts, rowid, name, path, description, sql_content)
    VALUES('delete', old.rowid, old.name, old.path, old.description, old.sql_content);
    INSERT INTO models_fts(rowid, name, path, description, sql_content)
    VALUES (new.rowid, new.name, new.path, new.description, new.sql_content);
END;
`

// InitSchema creates the test database schema.
func (m *TestMetadataDB) InitSchema() error {
	ctx := context.Background()
	if _, err := m.db.ExecContext(ctx, testMetadataSchema); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

// PopulateFromCatalog populates the test database from a Catalog.
func (m *TestMetadataDB) PopulateFromCatalog(catalog *Catalog) error {
	ctx := context.Background()
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Insert catalog metadata
	metaStmt, err := tx.PrepareContext(ctx, "INSERT INTO catalog_meta (key, value) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare catalog_meta statement: %w", err)
	}
	defer func() { _ = metaStmt.Close() }()

	if _, err := metaStmt.ExecContext(ctx, "project_name", catalog.ProjectName); err != nil {
		return err
	}
	if _, err := metaStmt.ExecContext(ctx, "generated_at", catalog.GeneratedAt.Format("2006-01-02T15:04:05Z07:00")); err != nil {
		return err
	}

	// Insert models
	modelStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO models (path, name, folder, materialized, unique_key, sql_content, file_path, description, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare models statement: %w", err)
	}
	defer func() { _ = modelStmt.Close() }()

	for _, model := range catalog.Models {
		folder := extractFolder(model.Path)
		if _, err := modelStmt.ExecContext(ctx,
			model.Path,
			model.Name,
			folder,
			model.Materialized,
			nullString(model.UniqueKey),
			model.SQL,
			model.FilePath,
			nullString(model.Description),
			model.UpdatedAt,
		); err != nil {
			return fmt.Errorf("failed to insert model %s: %w", model.Path, err)
		}
	}

	// Insert dependencies
	depStmt, err := tx.PrepareContext(ctx, "INSERT INTO dependencies (model_path, parent_path) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare dependencies statement: %w", err)
	}
	defer func() { _ = depStmt.Close() }()

	// Insert dependents
	dependentStmt, err := tx.PrepareContext(ctx, "INSERT INTO dependents (model_path, dependent_path) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare dependents statement: %w", err)
	}
	defer func() { _ = dependentStmt.Close() }()

	for _, model := range catalog.Models {
		for _, dep := range model.Dependencies {
			if _, err := depStmt.ExecContext(ctx, model.Path, dep); err != nil {
				return fmt.Errorf("failed to insert dependency: %w", err)
			}
		}
		for _, dependent := range model.Dependents {
			if _, err := dependentStmt.ExecContext(ctx, model.Path, dependent); err != nil {
				return fmt.Errorf("failed to insert dependent: %w", err)
			}
		}
	}

	// Insert sources
	sourceStmt, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO sources (name) VALUES (?)")
	if err != nil {
		return fmt.Errorf("failed to prepare sources statement: %w", err)
	}
	defer func() { _ = sourceStmt.Close() }()

	sourceRefStmt, err := tx.PrepareContext(ctx, "INSERT INTO source_refs (source_name, model_path) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare source_refs statement: %w", err)
	}
	defer func() { _ = sourceRefStmt.Close() }()

	for _, source := range catalog.Sources {
		if _, err := sourceStmt.ExecContext(ctx, source.Name); err != nil {
			return fmt.Errorf("failed to insert source %s: %w", source.Name, err)
		}
		for _, modelPath := range source.ReferencedBy {
			if _, err := sourceRefStmt.ExecContext(ctx, source.Name, modelPath); err != nil {
				return fmt.Errorf("failed to insert source ref: %w", err)
			}
		}
	}

	// Insert model_sources
	modelSourceStmt, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO model_sources (model_path, source_name) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare model_sources statement: %w", err)
	}
	defer func() { _ = modelSourceStmt.Close() }()

	for _, model := range catalog.Models {
		for _, src := range model.Sources {
			if _, err := modelSourceStmt.ExecContext(ctx, model.Path, src); err != nil {
				return fmt.Errorf("failed to insert model_source: %w", err)
			}
		}
	}

	// Insert columns
	colStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO columns (model_path, name, idx, transform_type, function_name)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare columns statement: %w", err)
	}
	defer func() { _ = colStmt.Close() }()

	colSourceStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO column_sources (model_path, column_name, source_table, source_column)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare column_sources statement: %w", err)
	}
	defer func() { _ = colSourceStmt.Close() }()

	for _, model := range catalog.Models {
		for _, col := range model.Columns {
			if _, err := colStmt.ExecContext(ctx,
				model.Path,
				col.Name,
				col.Index,
				nullString(col.TransformType),
				nullString(col.Function),
			); err != nil {
				return fmt.Errorf("failed to insert column %s.%s: %w", model.Path, col.Name, err)
			}
			for _, src := range col.Sources {
				if src.Table == "" || src.Column == "" {
					continue
				}
				if _, err := colSourceStmt.ExecContext(ctx, model.Path, col.Name, src.Table, src.Column); err != nil {
					return fmt.Errorf("failed to insert column source: %w", err)
				}
			}
		}
	}

	// Insert lineage edges
	lineageStmt, err := tx.PrepareContext(ctx, "INSERT INTO lineage_edges (source_node, target_node) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare lineage_edges statement: %w", err)
	}
	defer func() { _ = lineageStmt.Close() }()

	for _, edge := range catalog.Lineage.Edges {
		if _, err := lineageStmt.ExecContext(ctx, edge.Source, edge.Target); err != nil {
			return fmt.Errorf("failed to insert lineage edge: %w", err)
		}
	}

	// Note: Column lineage is now queried directly from state.db views,
	// not from Catalog. Skipping column_lineage_nodes/edges insertion.

	return tx.Commit()
}

// =============================================================================
// Test Helpers
// =============================================================================

// setupTestDB creates an in-memory database with schema initialized.
func setupTestDB(t *testing.T) *TestMetadataDB {
	t.Helper()

	db, err := openTestMemoryDB()
	require.NoError(t, err)

	err = db.InitSchema()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

// newTestCatalog creates an empty test catalog.
func newTestCatalog() *Catalog {
	return &Catalog{
		GeneratedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		ProjectName: "test_project",
		Models:      []*ModelDoc{},
		Sources:     []SourceDoc{},
		Lineage:     LineageDoc{Nodes: []string{}, Edges: []LineageEdge{}},
	}
}

// newTestModel creates a test model with the given parameters.
func newTestModel(path, name, materialized string) *ModelDoc {
	return &ModelDoc{
		ID:           path,
		Name:         name,
		Path:         path,
		Materialized: materialized,
		SQL:          "SELECT 1",
		FilePath:     "models/" + name + ".sql",
		Sources:      []string{},
		Dependencies: []string{},
		Dependents:   []string{},
		Columns:      []ColumnDoc{},
		UpdatedAt:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}
}

// newTestModelWithColumns creates a test model with columns.
func newTestModelWithColumns(path, name, materialized string, columns []ColumnDoc) *ModelDoc {
	m := newTestModel(path, name, materialized)
	m.Columns = columns
	return m
}

// newTestCatalogWithModels creates a catalog with the given models.
func newTestCatalogWithModels(models ...*ModelDoc) *Catalog {
	catalog := newTestCatalog()
	catalog.Models = models
	return catalog
}

// newTestColumn creates a test column.
func newTestColumn(name string, index int) ColumnDoc {
	return ColumnDoc{
		Name:    name,
		Index:   index,
		Sources: []SourceRef{},
	}
}

// newTestColumnWithSources creates a test column with source references.
func newTestColumnWithSources(name string, index int, sources ...SourceRef) ColumnDoc {
	return ColumnDoc{
		Name:    name,
		Index:   index,
		Sources: sources,
	}
}

// newTestSourceRef creates a test source reference.
func newTestSourceRef(table, column string) SourceRef {
	return SourceRef{
		Table:  table,
		Column: column,
	}
}
