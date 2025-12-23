-- name: BatchGetAllColumns :many
-- Returns all columns for all models in one query
SELECT 
    mc.model_path,
    mc.column_name,
    mc.column_index,
    mc.transform_type,
    mc.function_name
FROM model_columns mc
ORDER BY mc.model_path, mc.column_index;

-- name: BatchGetAllColumnLineage :many
-- Returns all column lineage for all models in one query
SELECT 
    cl.model_path,
    cl.column_name,
    cl.source_table,
    cl.source_column
FROM column_lineage cl
ORDER BY cl.model_path, cl.column_name;

-- name: BatchGetAllDependencies :many
-- Returns all dependencies in one query
SELECT 
    d.model_id,
    d.parent_id
FROM dependencies d;

-- name: BatchGetAllDependents :many
-- Returns all dependents (reverse lookup) in one query
SELECT 
    d.parent_id,
    d.model_id
FROM dependencies d;
