-- name: RecordModelRun :exec
INSERT INTO model_runs (id, run_id, model_id, status, rows_affected, started_at, error, render_ms, execution_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpdateModelRun :exec
UPDATE model_runs
SET status = ?, rows_affected = ?, completed_at = ?, error = ?, render_ms = ?, execution_ms = ?
WHERE id = ?;

-- name: GetModelRunStartedAt :one
SELECT started_at FROM model_runs WHERE id = ?;

-- name: GetModelRunsForRun :many
SELECT id, run_id, model_id, status, rows_affected, started_at, completed_at, error, render_ms, execution_ms
FROM model_runs
WHERE run_id = ?
ORDER BY started_at;

-- name: GetLatestModelRun :one
SELECT id, run_id, model_id, status, rows_affected, started_at, completed_at, error, render_ms, execution_ms
FROM model_runs
WHERE model_id = ?
ORDER BY started_at DESC
LIMIT 1;

-- name: GetModelRunsWithModelInfo :many
SELECT
    mr.id, mr.run_id, mr.model_id, mr.status,
    mr.rows_affected, mr.started_at, mr.completed_at,
    mr.error, mr.render_ms, mr.execution_ms,
    m.path as model_path, m.name as model_name
FROM model_runs mr
JOIN models m ON mr.model_id = m.id
WHERE mr.run_id = ?
ORDER BY mr.started_at;
