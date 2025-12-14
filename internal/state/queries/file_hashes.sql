-- name: GetContentHash :one
SELECT content_hash FROM file_hashes WHERE file_path = ?;

-- name: SetContentHash :exec
INSERT INTO file_hashes (file_path, content_hash, file_type, updated_at)
VALUES (?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(file_path) DO UPDATE SET
    content_hash = excluded.content_hash,
    file_type = excluded.file_type,
    updated_at = CURRENT_TIMESTAMP;

-- name: DeleteContentHash :exec
DELETE FROM file_hashes WHERE file_path = ?;
