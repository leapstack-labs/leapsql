-- LeapSQL State Management Schema
-- This schema tracks pipeline runs, models, execution history, and dependencies.

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
