-- name: DeleteDependenciesByModelID :exec
DELETE FROM dependencies WHERE model_id = ?;

-- name: InsertDependency :exec
INSERT INTO dependencies (model_id, parent_id) VALUES (?, ?);

-- name: GetDependencies :many
SELECT parent_id FROM dependencies WHERE model_id = ?;

-- name: GetDependents :many
SELECT model_id FROM dependencies WHERE parent_id = ?;

-- name: DeleteDependenciesByModelOrParent :exec
DELETE FROM dependencies
WHERE model_id IN (SELECT m.id FROM models m WHERE m.file_path = ?)
   OR parent_id IN (SELECT m2.id FROM models m2 WHERE m2.file_path = ?);
