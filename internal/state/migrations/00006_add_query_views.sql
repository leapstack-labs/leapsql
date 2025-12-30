-- +goose Up
-- Add views for the query command to inspect pipeline state

-- v_runs: Recent runs overview with calculated duration
CREATE VIEW v_runs AS
SELECT 
    id,
    environment,
    status,
    started_at,
    completed_at,
    ROUND((julianday(COALESCE(completed_at, CURRENT_TIMESTAMP)) - julianday(started_at)) * 86400, 1) AS duration_secs,
    error
FROM runs
ORDER BY started_at DESC;

-- v_model_runs: Model execution history with model info
CREATE VIEW v_model_runs AS
SELECT 
    mr.id,
    mr.run_id,
    m.path AS model_path,
    m.name AS model_name,
    mr.status,
    mr.rows_affected,
    mr.execution_ms,
    mr.started_at,
    mr.completed_at,
    mr.error
FROM model_runs mr
JOIN models m ON mr.model_id = m.id
ORDER BY mr.started_at DESC;

-- v_failed_runs: Failed model runs with context
CREATE VIEW v_failed_runs AS
SELECT 
    mr.run_id,
    r.environment,
    m.path AS model_path,
    m.name AS model_name,
    mr.error,
    mr.started_at,
    mr.completed_at
FROM model_runs mr
JOIN models m ON mr.model_id = m.id
JOIN runs r ON mr.run_id = r.id
WHERE mr.status = 'failed'
ORDER BY mr.started_at DESC;

-- v_stale_models: Models not included in the last successful run
CREATE VIEW v_stale_models AS
SELECT 
    m.path, 
    m.name, 
    m.materialized, 
    m.updated_at,
    (
        SELECT MAX(mr.completed_at) 
        FROM model_runs mr 
        WHERE mr.model_id = m.id AND mr.status = 'success'
    ) AS last_success_at
FROM models m
WHERE m.id NOT IN (
    SELECT DISTINCT mr.model_id 
    FROM model_runs mr
    WHERE mr.run_id = (
        SELECT id FROM runs 
        WHERE status = 'completed' 
        ORDER BY started_at DESC 
        LIMIT 1
    )
)
ORDER BY last_success_at ASC NULLS FIRST;

-- +goose Down
DROP VIEW IF EXISTS v_stale_models;
DROP VIEW IF EXISTS v_failed_runs;
DROP VIEW IF EXISTS v_model_runs;
DROP VIEW IF EXISTS v_runs;
