-- +goose Up

-- Source columns view: shows which columns are referenced from external sources
-- This allows the UI to display expected columns for each external source
CREATE VIEW IF NOT EXISTS v_source_columns AS
SELECT DISTINCT 
    cl.source_table AS source_name, 
    cl.source_column AS column_name
FROM column_lineage cl
WHERE cl.source_table != ''
  AND NOT EXISTS (
      SELECT 1 FROM models m
      WHERE m.name = cl.source_table OR m.path = cl.source_table
  )
ORDER BY cl.source_table, cl.source_column;

-- +goose Down
DROP VIEW IF EXISTS v_source_columns;
