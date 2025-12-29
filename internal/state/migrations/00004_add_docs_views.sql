-- +goose Up
-- Add views for docs frontend consumption

-- Models with derived folder (v_ prefix per SQLite conventions)
CREATE VIEW v_models AS
SELECT
    id,
    path,
    name,
    CASE WHEN instr(path, '.') > 0
         THEN substr(path, 1, instr(path, '.') - 1)
         ELSE 'default' END AS folder,
    materialized,
    unique_key,
    sql_content,
    raw_content,
    file_path,
    description,
    owner,
    schema_name,
    tags,
    tests,
    meta,
    uses_select_star,
    created_at,
    updated_at
FROM models;

-- Dependencies by path (resolves UUIDs to paths)
CREATE VIEW v_dependencies AS
SELECT m.path AS model_path, p.path AS parent_path
FROM dependencies d
JOIN models m ON d.model_id = m.id
JOIN models p ON d.parent_id = p.id;

-- Dependents (reverse of dependencies)
CREATE VIEW v_dependents AS
SELECT p.path AS model_path, m.path AS dependent_path
FROM dependencies d
JOIN models m ON d.model_id = m.id
JOIN models p ON d.parent_id = p.id;

-- External sources (tables referenced but not in models)
CREATE VIEW v_sources AS
SELECT DISTINCT cl.source_table AS name
FROM column_lineage cl
WHERE NOT EXISTS (
    SELECT 1 FROM models m
    WHERE m.name = cl.source_table OR m.path = cl.source_table
)
AND cl.source_table != '';

-- Source references (which models reference which external sources)
CREATE VIEW v_source_refs AS
SELECT DISTINCT cl.source_table AS source_name, cl.model_path
FROM column_lineage cl
WHERE NOT EXISTS (
    SELECT 1 FROM models m
    WHERE m.name = cl.source_table OR m.path = cl.source_table
)
AND cl.source_table != '';

-- Model-level lineage edges for DAG visualization
CREATE VIEW v_lineage_edges AS
SELECT parent_path AS source_node, model_path AS target_node
FROM v_dependencies
UNION
SELECT 'source:' || source_name AS source_node, model_path AS target_node
FROM v_source_refs;

-- Column lineage nodes for graph visualization
CREATE VIEW v_column_lineage_nodes AS
SELECT DISTINCT
    model_path || '.' || column_name AS id,
    model_path AS model,
    column_name
FROM model_columns
UNION
SELECT DISTINCT
    source_table || '.' || source_column AS id,
    source_table AS model,
    source_column AS column_name
FROM column_lineage
WHERE source_table != '' AND source_column != '';

-- Column lineage edges for graph visualization
CREATE VIEW v_column_lineage_edges AS
SELECT DISTINCT
    source_table || '.' || source_column AS source_id,
    model_path || '.' || column_name AS target_id
FROM column_lineage
WHERE source_table != '' AND source_column != '';

-- Columns view (consistent naming)
CREATE VIEW v_columns AS
SELECT model_path, column_name AS name, column_index AS idx,
       transform_type, function_name
FROM model_columns;

-- Column sources view (consistent naming)
CREATE VIEW v_column_sources AS
SELECT model_path, column_name, source_table, source_column
FROM column_lineage;

-- Macros view for catalog
CREATE VIEW v_macros AS
SELECT
    n.name AS namespace,
    n.file_path,
    n.package,
    f.name AS function_name,
    f.args,
    f.docstring,
    f.line
FROM macro_namespaces n
LEFT JOIN macro_functions f ON n.name = f.namespace;

-- +goose Down
DROP VIEW IF EXISTS v_macros;
DROP VIEW IF EXISTS v_column_sources;
DROP VIEW IF EXISTS v_columns;
DROP VIEW IF EXISTS v_column_lineage_edges;
DROP VIEW IF EXISTS v_column_lineage_nodes;
DROP VIEW IF EXISTS v_lineage_edges;
DROP VIEW IF EXISTS v_source_refs;
DROP VIEW IF EXISTS v_sources;
DROP VIEW IF EXISTS v_dependents;
DROP VIEW IF EXISTS v_dependencies;
DROP VIEW IF EXISTS v_models;
