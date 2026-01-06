-- name: CreateRun :one
INSERT INTO runs (id, environment, status, started_at)
VALUES (?, ?, ?, ?)
RETURNING id, environment, status, started_at, completed_at, error;

-- name: GetRun :one
SELECT id, environment, status, started_at, completed_at, error
FROM runs
WHERE id = ?;

-- name: CompleteRun :exec
UPDATE runs
SET status = ?, completed_at = ?, error = ?
WHERE id = ?;

-- name: GetLatestRun :one
SELECT id, environment, status, started_at, completed_at, error
FROM runs
WHERE environment = ?
ORDER BY started_at DESC
LIMIT 1;

-- name: ListRuns :many
SELECT id, environment, status, started_at, completed_at, error
FROM runs
ORDER BY started_at DESC
LIMIT ?;
