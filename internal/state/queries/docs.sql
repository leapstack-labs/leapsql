-- Docs-specific queries for frontend consumption
-- These queries use the views defined in migration 00004

-- Models
-- name: GetModelsForDocs :many
SELECT id, path, name, folder, materialized, unique_key, sql_content, raw_content, file_path, description, owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at
FROM v_models ORDER BY folder, name;

-- name: GetModelForDocs :one
SELECT id, path, name, folder, materialized, unique_key, sql_content, raw_content, file_path, description, owner, schema_name, tags, tests, meta, uses_select_star, created_at, updated_at
FROM v_models WHERE path = ?;

-- Dependencies
-- name: GetModelDependenciesByPath :many
SELECT parent_path FROM v_dependencies WHERE model_path = ?;

-- name: GetModelDependentsByPath :many
SELECT dependent_path FROM v_dependents WHERE model_path = ?;

-- Sources
-- name: GetExternalSources :many
SELECT name FROM v_sources ORDER BY name;

-- name: GetSourceReferencedBy :many
SELECT model_path FROM v_source_refs WHERE source_name = ?;

-- name: GetSourceColumns :many
SELECT column_name FROM v_source_columns WHERE source_name = ? ORDER BY column_name;

-- Lineage
-- name: GetLineageEdges :many
SELECT source_node, target_node FROM v_lineage_edges;

-- Column Lineage
-- name: GetColumnLineageNodes :many
SELECT id, model, column_name FROM v_column_lineage_nodes;

-- name: GetColumnLineageNodesForModel :many
SELECT DISTINCT cln.id, cln.model, cln.column_name
FROM v_column_lineage_nodes cln
WHERE cln.model = ?1
UNION
SELECT DISTINCT cln2.id, cln2.model, cln2.column_name
FROM v_column_lineage_nodes cln
JOIN v_column_lineage_edges cle ON cln.id = cle.target_id
JOIN v_column_lineage_nodes cln2 ON cle.source_id = cln2.id
WHERE cln.model = ?1;

-- name: GetColumnLineageEdges :many
SELECT source_id, target_id FROM v_column_lineage_edges;

-- name: GetColumnLineageEdgesForModel :many
SELECT cle.source_id, cle.target_id
FROM v_column_lineage_edges cle
JOIN v_column_lineage_nodes cln ON cle.target_id = cln.id
WHERE cln.model = ?;

-- Columns
-- name: GetColumnsForModel :many
SELECT name, idx, transform_type, function_name
FROM v_columns WHERE model_path = ? ORDER BY idx;

-- name: GetColumnSourcesForColumn :many
SELECT source_table, source_column
FROM v_column_sources WHERE model_path = ? AND column_name = ?;

-- name: GetAllColumnSourcesForModel :many
SELECT column_name, source_table, source_column
FROM v_column_sources WHERE model_path = ?;

-- Search (FTS5 fallback with LIKE) - Used by SQLC. For proper FTS5, use SearchModels() method directly.
-- name: SearchModelsLike :many
SELECT path, name, 
    CASE WHEN instr(path, '.') > 0
         THEN substr(path, 1, instr(path, '.') - 1)
         ELSE 'default' END AS folder,
    materialized, description
FROM models
WHERE name LIKE '%' || ? || '%' OR path LIKE '%' || ? || '%' OR description LIKE '%' || ? || '%'
ORDER BY name
LIMIT 20;

-- Stats
-- name: GetModelCount :one
SELECT COUNT(*) FROM models;

-- name: GetSourceCount :one
SELECT COUNT(*) FROM v_sources;

-- name: GetColumnCount :one
SELECT COUNT(*) FROM model_columns;

-- name: GetFolderCount :one
SELECT COUNT(DISTINCT folder) FROM v_models;

-- name: GetMaterializationCounts :many
SELECT materialized, COUNT(*) as count FROM models GROUP BY materialized;

-- Macros (for future catalog)
-- name: GetMacrosForDocs :many
SELECT namespace, file_path, package, function_name, args, docstring, line
FROM v_macros ORDER BY namespace, function_name;

-- name: GetMacroNamespacesForDocs :many
SELECT DISTINCT namespace, file_path, package FROM v_macros ORDER BY namespace;
