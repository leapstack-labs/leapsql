-- name: CreateEnvironment :one
INSERT INTO environments (name, created_at, updated_at)
VALUES (?, ?, ?)
RETURNING name, commit_ref, created_at, updated_at;

-- name: GetEnvironment :one
SELECT name, commit_ref, created_at, updated_at
FROM environments
WHERE name = ?;

-- name: UpdateEnvironmentRef :exec
UPDATE environments
SET commit_ref = ?, updated_at = ?
WHERE name = ?;
