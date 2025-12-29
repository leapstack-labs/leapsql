-- Project metadata queries

-- name: GetProjectMeta :one
SELECT value FROM project_meta WHERE key = ?;

-- name: SetProjectMeta :exec
INSERT OR REPLACE INTO project_meta (key, value) VALUES (?, ?);

-- name: DeleteProjectMeta :exec
DELETE FROM project_meta WHERE key = ?;

-- name: ListProjectMeta :many
SELECT key, value FROM project_meta ORDER BY key;
