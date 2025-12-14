-- name: RecordModelRun :exec
INSERT INTO model_runs (id, run_id, model_id, status, rows_affected, started_at, error, execution_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpdateModelRun :exec
UPDATE model_runs
SET status = ?, rows_affected = ?, completed_at = ?, error = ?, execution_ms = ?
WHERE id = ?;

-- name: GetModelRunStartedAt :one
SELECT started_at FROM model_runs WHERE id = ?;

-- name: GetModelRunsForRun :many
SELECT id, run_id, model_id, status, rows_affected, started_at, completed_at, error, execution_ms
FROM model_runs
WHERE run_id = ?
ORDER BY started_at;

-- name: GetLatestModelRun :one
SELECT id, run_id, model_id, status, rows_affected, started_at, completed_at, error, execution_ms
FROM model_runs
WHERE model_id = ?
ORDER BY started_at DESC
LIMIT 1;
