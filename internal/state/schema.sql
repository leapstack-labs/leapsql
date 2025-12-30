-- LeapSQL State Management Schema
-- This schema tracks pipeline runs, models, execution history, and dependencies.
-- Note: This file is used by SQLC for code generation. Actual schema is managed via Goose migrations.

-- runs: execution sessions
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    error TEXT,
    
    CHECK (status IN ('running', 'completed', 'failed', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_runs_environment ON runs(environment);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);

-- models: registered model metadata
CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    materialized TEXT NOT NULL DEFAULT 'table',
    unique_key TEXT,
    content_hash TEXT NOT NULL,
    file_path TEXT,           -- Absolute path to .sql file (for incremental discovery)
    -- New fields from frontmatter
    owner TEXT,
    schema_name TEXT,
    tags TEXT,           -- JSON array: ["finance", "revenue"]
    tests TEXT,          -- JSON array of test configs
    meta TEXT,           -- JSON object for extensions
    uses_select_star INTEGER DEFAULT 0,  -- 0=false, 1=true (for schema drift detection)
    -- New fields for docs consolidation
    sql_content TEXT DEFAULT '',    -- Rendered SQL (macros expanded)
    raw_content TEXT DEFAULT '',    -- Original file content
    description TEXT DEFAULT '',    -- Model description
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CHECK (materialized IN ('table', 'view', 'incremental'))
);

CREATE INDEX IF NOT EXISTS idx_models_path ON models(path);
CREATE INDEX IF NOT EXISTS idx_models_name ON models(name);
CREATE INDEX IF NOT EXISTS idx_models_file_path ON models(file_path);

-- model_runs: execution history per model
CREATE TABLE IF NOT EXISTS model_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    rows_affected INTEGER DEFAULT 0,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    error TEXT,
    execution_ms INTEGER DEFAULT 0,
    
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE CASCADE,
    
    CHECK (status IN ('pending', 'running', 'success', 'failed', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_model_runs_run_id ON model_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_model_runs_model_id ON model_runs(model_id);
CREATE INDEX IF NOT EXISTS idx_model_runs_status ON model_runs(status);

-- dependencies: DAG edges (model -> parent relationships)
CREATE TABLE IF NOT EXISTS dependencies (
    model_id TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    
    PRIMARY KEY (model_id, parent_id),
    FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES models(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dependencies_model_id ON dependencies(model_id);
CREATE INDEX IF NOT EXISTS idx_dependencies_parent_id ON dependencies(parent_id);

-- environments: virtual environment pointers
CREATE TABLE IF NOT EXISTS environments (
    name TEXT PRIMARY KEY,
    commit_ref TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Trigger to update updated_at on models table
CREATE TRIGGER IF NOT EXISTS models_updated_at
    AFTER UPDATE ON models
    FOR EACH ROW
BEGIN
    UPDATE models SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Trigger to update updated_at on environments table
CREATE TRIGGER IF NOT EXISTS environments_updated_at
    AFTER UPDATE ON environments
    FOR EACH ROW
BEGIN
    UPDATE environments SET updated_at = CURRENT_TIMESTAMP WHERE name = NEW.name;
END;

-- model_columns: output columns for each model
CREATE TABLE IF NOT EXISTS model_columns (
    model_path     TEXT NOT NULL,           -- e.g., "staging.stg_customers"
    column_name    TEXT NOT NULL,
    column_index   INTEGER NOT NULL,
    transform_type TEXT DEFAULT '',         -- '' (direct) or 'EXPR'
    function_name  TEXT DEFAULT '',         -- 'sum', 'count', etc.
    PRIMARY KEY (model_path, column_name),
    FOREIGN KEY (model_path) REFERENCES models(path) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_model_columns_path ON model_columns(model_path);

-- column_lineage: column-to-column lineage (source columns for each output column)
CREATE TABLE IF NOT EXISTS column_lineage (
    model_path    TEXT NOT NULL,            -- model that defines this column
    column_name   TEXT NOT NULL,            -- output column name
    source_table  TEXT NOT NULL,            -- source table (model name or raw table)
    source_column TEXT NOT NULL,            -- source column name
    PRIMARY KEY (model_path, column_name, source_table, source_column),
    FOREIGN KEY (model_path, column_name) REFERENCES model_columns(model_path, column_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_column_lineage_source ON column_lineage(source_table, source_column);
CREATE INDEX IF NOT EXISTS idx_column_lineage_model ON column_lineage(model_path);

-- macro_namespaces: one per .star file
CREATE TABLE IF NOT EXISTS macro_namespaces (
    name       TEXT PRIMARY KEY,              -- "utils", "datetime"
    file_path  TEXT NOT NULL,                 -- Absolute path to .star file
    package    TEXT DEFAULT '',               -- "" for local, package name for vendor
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_macro_namespaces_package ON macro_namespaces(package);

-- macro_functions: many per namespace
CREATE TABLE IF NOT EXISTS macro_functions (
    namespace  TEXT NOT NULL,
    name       TEXT NOT NULL,
    args       TEXT NOT NULL DEFAULT '[]',    -- JSON: ["column", "default=None"]
    docstring  TEXT DEFAULT '',               -- Extracted from function
    line       INTEGER DEFAULT 0,             -- Line number for go-to-definition
    PRIMARY KEY (namespace, name),
    FOREIGN KEY (namespace) REFERENCES macro_namespaces(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_macro_functions_namespace ON macro_functions(namespace);

-- Trigger to update updated_at on macro_namespaces table
CREATE TRIGGER IF NOT EXISTS macro_namespaces_updated_at
    AFTER UPDATE ON macro_namespaces
    FOR EACH ROW
BEGIN
    UPDATE macro_namespaces SET updated_at = CURRENT_TIMESTAMP WHERE name = NEW.name;
END;

-- file_hashes: track content hashes for incremental discovery
CREATE TABLE IF NOT EXISTS file_hashes (
    file_path TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    file_type TEXT NOT NULL,  -- 'model' or 'macro'
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_file_hashes_type ON file_hashes(file_type);

-- column_snapshots: store known-good column state after successful runs
-- Used by schema drift detection to compare current vs. last-known state
CREATE TABLE IF NOT EXISTS column_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_path TEXT NOT NULL,
    source_table TEXT NOT NULL,
    column_name TEXT NOT NULL,
    column_index INTEGER NOT NULL,
    snapshot_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    run_id TEXT NOT NULL,  -- Links to successful run
    UNIQUE(model_path, source_table, column_name, run_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_model ON column_snapshots(model_path);
CREATE INDEX IF NOT EXISTS idx_snapshots_source ON column_snapshots(source_table);
CREATE INDEX IF NOT EXISTS idx_snapshots_run_id ON column_snapshots(run_id);

-- project_meta: key-value store for project-level metadata
CREATE TABLE IF NOT EXISTS project_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- ============================================================================
-- Views for Docs (defined in migration 00004)
-- These views are used by the docs system for frontend consumption
-- ============================================================================

-- Models with derived folder
CREATE VIEW IF NOT EXISTS v_models AS
SELECT
    id,
    path,
    name,
    CASE WHEN instr(path, '.') > 0
         THEN substr(path, 1, instr(path, '.') - 1)
         ELSE 'default' END AS folder,
    materialized,
    unique_key,
    sql_content,
    raw_content,
    file_path,
    description,
    owner,
    schema_name,
    tags,
    tests,
    meta,
    uses_select_star,
    created_at,
    updated_at
FROM models;

-- Dependencies by path (resolves UUIDs to paths)
CREATE VIEW IF NOT EXISTS v_dependencies AS
SELECT m.path AS model_path, p.path AS parent_path
FROM dependencies d
JOIN models m ON d.model_id = m.id
JOIN models p ON d.parent_id = p.id;

-- Dependents (reverse of dependencies)
CREATE VIEW IF NOT EXISTS v_dependents AS
SELECT p.path AS model_path, m.path AS dependent_path
FROM dependencies d
JOIN models m ON d.model_id = m.id
JOIN models p ON d.parent_id = p.id;

-- External sources (tables referenced but not in models)
CREATE VIEW IF NOT EXISTS v_sources AS
SELECT DISTINCT cl.source_table AS name
FROM column_lineage cl
WHERE NOT EXISTS (
    SELECT 1 FROM models m
    WHERE m.name = cl.source_table OR m.path = cl.source_table
)
AND cl.source_table != '';

-- Source references (which models reference which external sources)
CREATE VIEW IF NOT EXISTS v_source_refs AS
SELECT DISTINCT cl.source_table AS source_name, cl.model_path
FROM column_lineage cl
WHERE NOT EXISTS (
    SELECT 1 FROM models m
    WHERE m.name = cl.source_table OR m.path = cl.source_table
)
AND cl.source_table != '';

-- Model-level lineage edges for DAG visualization
CREATE VIEW IF NOT EXISTS v_lineage_edges AS
SELECT parent_path AS source_node, model_path AS target_node
FROM v_dependencies
UNION
SELECT 'source:' || source_name AS source_node, model_path AS target_node
FROM v_source_refs;

-- Column lineage nodes for graph visualization
CREATE VIEW IF NOT EXISTS v_column_lineage_nodes AS
SELECT DISTINCT
    model_path || '.' || column_name AS id,
    model_path AS model,
    column_name
FROM model_columns
UNION
SELECT DISTINCT
    source_table || '.' || source_column AS id,
    source_table AS model,
    source_column AS column_name
FROM column_lineage
WHERE source_table != '' AND source_column != '';

-- Column lineage edges for graph visualization
CREATE VIEW IF NOT EXISTS v_column_lineage_edges AS
SELECT DISTINCT
    source_table || '.' || source_column AS source_id,
    model_path || '.' || column_name AS target_id
FROM column_lineage
WHERE source_table != '' AND source_column != '';

-- Columns view (consistent naming)
CREATE VIEW IF NOT EXISTS v_columns AS
SELECT model_path, column_name AS name, column_index AS idx,
       transform_type, function_name
FROM model_columns;

-- Column sources view (consistent naming)
CREATE VIEW IF NOT EXISTS v_column_sources AS
SELECT model_path, column_name, source_table, source_column
FROM column_lineage;

-- Source columns view: shows which columns are referenced from external sources
CREATE VIEW IF NOT EXISTS v_source_columns AS
SELECT DISTINCT 
    cl.source_table AS source_name, 
    cl.source_column AS column_name
FROM column_lineage cl
WHERE cl.source_table != ''
  AND NOT EXISTS (
      SELECT 1 FROM models m
      WHERE m.name = cl.source_table OR m.path = cl.source_table
  )
ORDER BY cl.source_table, cl.source_column;

-- Macros view for catalog
CREATE VIEW IF NOT EXISTS v_macros AS
SELECT
    n.name AS namespace,
    n.file_path,
    n.package,
    f.name AS function_name,
    f.args,
    f.docstring,
    f.line
FROM macro_namespaces n
LEFT JOIN macro_functions f ON n.name = f.namespace;

-- ============================================================================
-- FTS5 Search (defined in migration 00005)
-- ============================================================================

-- Full-text search on models (virtual table)
CREATE VIRTUAL TABLE IF NOT EXISTS models_fts USING fts5(
    name,
    path,
    description,
    sql_content,
    content='models',
    content_rowid='rowid'
);
