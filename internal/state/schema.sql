-- DBGo State Management Schema
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
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CHECK (materialized IN ('table', 'view', 'incremental'))
);

CREATE INDEX IF NOT EXISTS idx_models_path ON models(path);
CREATE INDEX IF NOT EXISTS idx_models_name ON models(name);

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
