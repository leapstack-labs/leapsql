// Package docs provides SQLite database generation for documentation.
package docs

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	_ "modernc.org/sqlite" // SQLite driver (pure Go)
)

// memoryDBCounter is used to generate unique names for in-memory databases.
var memoryDBCounter atomic.Uint64

// MetadataDB handles SQLite database generation for docs.
type MetadataDB struct {
	db *sql.DB
}

// Schema optimized for UI consumption with FTS5 for search.
// FTS5 is a hard requirement - if not available, the build will fail.
const metadataSchema = `
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

-- Full-text search (FTS5 is required)
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

// OpenMetadataDB opens or creates a SQLite database for metadata.
func OpenMetadataDB(path string) (*MetadataDB, error) {
	// Use page_size=4096 for optimal range request performance
	dsn := fmt.Sprintf("file:%s?_page_size=4096&_journal_mode=WAL", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set pragmas for performance
	ctx := context.Background()
	pragmas := []string{
		"PRAGMA synchronous = OFF",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA cache_size = -64000", // 64MB cache
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set pragma: %w", err)
		}
	}

	return &MetadataDB{db: db}, nil
}

// OpenMemoryDB opens an in-memory SQLite database (for dev server).
// Each call creates a new isolated database with shared cache enabled
// to allow concurrent access from multiple goroutines within that database.
func OpenMemoryDB() (*MetadataDB, error) {
	// Use a unique name for each in-memory database so they're isolated.
	// Shared cache mode allows concurrent access from multiple connections
	// within the same named database.
	// See: https://www.sqlite.org/inmemorydb.html
	id := memoryDBCounter.Add(1)
	dsn := fmt.Sprintf("file:memdb%d?mode=memory&cache=shared", id)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory database: %w", err)
	}

	return &MetadataDB{db: db}, nil
}

// Close closes the database connection.
func (m *MetadataDB) Close() error {
	return m.db.Close()
}

// DB returns the underlying database connection (for query endpoint).
func (m *MetadataDB) DB() *sql.DB {
	return m.db
}

// InitSchema creates the database schema including FTS5.
// FTS5 is a hard requirement. If not available, this will return an error.
func (m *MetadataDB) InitSchema() error {
	ctx := context.Background()
	if _, err := m.db.ExecContext(ctx, metadataSchema); err != nil {
		return fmt.Errorf("failed to create schema (ensure FTS5 is enabled): %w", err)
	}
	return nil
}

// PopulateFromCatalog populates the database from a Catalog.
func (m *MetadataDB) PopulateFromCatalog(catalog *Catalog) error {
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

	// Insert column lineage
	colLineageNodeStmt, err := tx.PrepareContext(ctx, "INSERT INTO column_lineage_nodes (id, model, column_name) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare column_lineage_nodes statement: %w", err)
	}
	defer func() { _ = colLineageNodeStmt.Close() }()

	colLineageEdgeStmt, err := tx.PrepareContext(ctx, "INSERT INTO column_lineage_edges (source_id, target_id) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare column_lineage_edges statement: %w", err)
	}
	defer func() { _ = colLineageEdgeStmt.Close() }()

	for _, node := range catalog.ColumnLineage.Nodes {
		if _, err := colLineageNodeStmt.ExecContext(ctx, node.ID, node.Model, node.Column); err != nil {
			return fmt.Errorf("failed to insert column lineage node: %w", err)
		}
	}

	for _, edge := range catalog.ColumnLineage.Edges {
		if _, err := colLineageEdgeStmt.ExecContext(ctx, edge.Source, edge.Target); err != nil {
			return fmt.Errorf("failed to insert column lineage edge: %w", err)
		}
	}

	return tx.Commit()
}

// Vacuum optimizes the database for range requests.
func (m *MetadataDB) Vacuum() error {
	ctx := context.Background()
	_, err := m.db.ExecContext(ctx, "VACUUM")
	return err
}

// GenerateMetadataDB creates an optimized SQLite database from a Catalog.
func GenerateMetadataDB(catalog *Catalog, outputPath string) error {
	mdb, err := OpenMetadataDB(outputPath)
	if err != nil {
		return err
	}
	defer func() { _ = mdb.Close() }()

	if err := mdb.InitSchema(); err != nil {
		return err
	}

	if err := mdb.PopulateFromCatalog(catalog); err != nil {
		return err
	}

	// Optimize for range requests
	return mdb.Vacuum()
}

// extractFolder extracts the folder from a model path (e.g., "staging.customers" -> "staging").
func extractFolder(path string) string {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			return path[:i]
		}
	}
	return "default"
}

// nullString returns a sql.NullString for optional string fields.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// CopyToPath copies the database to a new location.
// This is used to copy from in-memory to a file for serving.
func (m *MetadataDB) CopyToPath(destPath string) error {
	// Use SQLite VACUUM INTO to copy the database
	ctx := context.Background()
	_, err := m.db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", destPath))
	if err != nil {
		return fmt.Errorf("failed to copy database: %w", err)
	}

	return nil
}
