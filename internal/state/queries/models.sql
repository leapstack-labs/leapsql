-- name: InsertModel :exec
INSERT INTO models (id, path, name, materialized, unique_key, content_hash, file_path,
    owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpdateModel :exec
UPDATE models
SET name = ?, materialized = ?, unique_key = ?, content_hash = ?, file_path = ?,
    owner = ?, schema_name = ?, tags = ?, tests = ?, meta = ?, uses_select_star = ?, updated_at = ?
WHERE id = ?;

-- name: GetModelByID :one
SELECT id, path, name, materialized, unique_key, content_hash, file_path,
    owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at
FROM models
WHERE id = ?;

-- name: GetModelByPath :one
SELECT id, path, name, materialized, unique_key, content_hash, file_path,
    owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at
FROM models
WHERE path = ?;

-- name: GetModelByFilePath :one
SELECT id, path, name, materialized, unique_key, content_hash, file_path,
    owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at
FROM models
WHERE file_path = ?;

-- name: UpdateModelHash :exec
UPDATE models
SET content_hash = ?, updated_at = ?
WHERE id = ?;

-- name: ListModels :many
SELECT id, path, name, materialized, unique_key, content_hash, file_path,
    owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at
FROM models
ORDER BY path;

-- name: DeleteModelByFilePath :exec
DELETE FROM models WHERE file_path = ?;

-- name: ListModelFilePaths :many
SELECT file_path FROM models WHERE file_path IS NOT NULL AND file_path != '';
