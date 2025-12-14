-- name: DeleteColumnLineageByModelPath :exec
DELETE FROM column_lineage WHERE model_path = ?;

-- name: DeleteModelColumnsByModelPath :exec
DELETE FROM model_columns WHERE model_path = ?;

-- name: InsertModelColumn :exec
INSERT INTO model_columns (model_path, column_name, column_index, transform_type, function_name)
VALUES (?, ?, ?, ?, ?);

-- name: InsertColumnLineage :exec
INSERT INTO column_lineage (model_path, column_name, source_table, source_column)
VALUES (?, ?, ?, ?);

-- name: GetModelColumns :many
SELECT column_name, column_index, transform_type, function_name
FROM model_columns
WHERE model_path = ?
ORDER BY column_index;

-- name: GetColumnLineage :many
SELECT column_name, source_table, source_column
FROM column_lineage
WHERE model_path = ?;

-- name: TraceColumnBackward :many
WITH RECURSIVE trace AS (
    -- Start: get direct sources of the target column
    SELECT
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        1 as depth
    FROM column_lineage cl
    WHERE cl.model_path = ? AND cl.column_name = ?

    UNION ALL

    -- Recurse: follow source_table -> model -> its sources
    SELECT
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        t.depth + 1
    FROM trace t
    JOIN models m ON (m.name = t.source_table OR m.path = t.source_table)
    JOIN column_lineage cl ON cl.model_path = m.path AND cl.column_name = t.source_column
    WHERE t.depth < 20
)
SELECT DISTINCT
    source_table as model_path,
    source_column as column_name,
    depth,
    CASE WHEN m.path IS NULL THEN 1 ELSE 0 END as is_external
FROM trace t
LEFT JOIN models m ON (m.name = t.source_table OR m.path = t.source_table)
ORDER BY depth, source_table, source_column;

-- name: TraceColumnForward :many
WITH RECURSIVE trace AS (
    -- Start: find columns that reference this model/column as a source
    SELECT
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        1 as depth
    FROM column_lineage cl
    JOIN models m ON (m.name = cl.source_table OR m.path = cl.source_table)
    WHERE m.path = ? AND cl.source_column = ?

    UNION ALL

    -- Recurse: find what references the columns we found
    SELECT
        cl.model_path,
        cl.column_name,
        cl.source_table,
        cl.source_column,
        t.depth + 1
    FROM trace t
    JOIN models m ON m.path = t.model_path
    JOIN column_lineage cl ON (cl.source_table = m.name OR cl.source_table = m.path)
                          AND cl.source_column = t.column_name
    WHERE t.depth < 20
)
SELECT DISTINCT model_path, column_name, depth
FROM trace
ORDER BY depth, model_path, column_name;
