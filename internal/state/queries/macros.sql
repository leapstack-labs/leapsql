-- name: UpsertMacroNamespace :exec
INSERT INTO macro_namespaces (name, file_path, package, updated_at)
VALUES (?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(name) DO UPDATE SET
    file_path = excluded.file_path,
    package = excluded.package,
    updated_at = CURRENT_TIMESTAMP;

-- name: DeleteMacroFunctionsByNamespace :exec
DELETE FROM macro_functions WHERE namespace = ?;

-- name: InsertMacroFunction :exec
INSERT INTO macro_functions (namespace, name, args, docstring, line)
VALUES (?, ?, ?, ?, ?);

-- name: GetMacroNamespaces :many
SELECT name, file_path, package, updated_at
FROM macro_namespaces
ORDER BY name;

-- name: GetMacroNamespace :one
SELECT name, file_path, package, updated_at
FROM macro_namespaces
WHERE name = ?;

-- name: GetMacroFunctions :many
SELECT namespace, name, args, docstring, line
FROM macro_functions
WHERE namespace = ?
ORDER BY name;

-- name: GetMacroFunction :one
SELECT namespace, name, args, docstring, line
FROM macro_functions
WHERE namespace = ? AND name = ?;

-- name: MacroFunctionExists :one
SELECT COUNT(*) FROM macro_functions
WHERE namespace = ? AND name = ?;

-- name: SearchMacroNamespaces :many
SELECT name, file_path, package, updated_at
FROM macro_namespaces
WHERE name LIKE ? || '%'
ORDER BY name;

-- name: SearchMacroFunctions :many
SELECT namespace, name, args, docstring, line
FROM macro_functions
WHERE namespace = ? AND name LIKE ? || '%'
ORDER BY name;

-- name: DeleteMacroNamespace :exec
DELETE FROM macro_namespaces WHERE name = ?;

-- name: DeleteMacroNamespaceByFilePath :exec
DELETE FROM macro_namespaces WHERE file_path = ?;

-- name: ListMacroFilePaths :many
SELECT file_path FROM macro_namespaces WHERE file_path IS NOT NULL AND file_path != '';
