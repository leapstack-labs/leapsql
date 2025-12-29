// Safe SQL queries with parameter binding
// All queries use ? placeholders for safe parameter substitution
// Uses v_* views from state.db for data that needs joins/computations

export interface QueryDef {
  sql: string;
  params: any[];
}

// Model queries
export const queries = {
  // Get all models for listing (uses v_models view for computed folder)
  getModels: (): QueryDef => ({
    sql: `SELECT path, name, folder, materialized, unique_key, description, file_path, updated_at 
          FROM v_models ORDER BY folder, name`,
    params: []
  }),

  // Get a single model by path
  getModel: (path: string): QueryDef => ({
    sql: `SELECT path, name, folder, materialized, unique_key, sql_content, description, file_path, updated_at 
          FROM v_models WHERE path = ?`,
    params: [path]
  }),

  // Get model dependencies (what this model depends on)
  getModelDependencies: (path: string): QueryDef => ({
    sql: `SELECT parent_path FROM v_dependencies WHERE model_path = ?`,
    params: [path]
  }),

  // Get model dependents (what depends on this model)
  getModelDependents: (path: string): QueryDef => ({
    sql: `SELECT dependent_path FROM v_dependents WHERE model_path = ?`,
    params: [path]
  }),

  // Get model sources (external tables referenced)
  getModelSources: (path: string): QueryDef => ({
    sql: `SELECT source_name FROM v_source_refs WHERE model_path = ?`,
    params: [path]
  }),

  // Get columns for a model
  getModelColumns: (path: string): QueryDef => ({
    sql: `SELECT name, idx, transform_type, function_name 
          FROM v_columns WHERE model_path = ? ORDER BY idx`,
    params: [path]
  }),

  // Get column sources for a specific column
  getColumnSources: (modelPath: string, columnName: string): QueryDef => ({
    sql: `SELECT source_table, source_column 
          FROM v_column_sources 
          WHERE model_path = ? AND column_name = ?`,
    params: [modelPath, columnName]
  }),

  // Get all column sources for a model
  getAllColumnSources: (modelPath: string): QueryDef => ({
    sql: `SELECT column_name, source_table, source_column 
          FROM v_column_sources 
          WHERE model_path = ?`,
    params: [modelPath]
  }),

  // Source queries
  getSources: (): QueryDef => ({
    sql: `SELECT name FROM v_sources ORDER BY name`,
    params: []
  }),

  getSource: (name: string): QueryDef => ({
    sql: `SELECT name FROM v_sources WHERE name = ?`,
    params: [name]
  }),

  getSourceReferencedBy: (name: string): QueryDef => ({
    sql: `SELECT model_path FROM v_source_refs WHERE source_name = ?`,
    params: [name]
  }),

  // Lineage queries
  getLineageEdges: (): QueryDef => ({
    sql: `SELECT source_node, target_node FROM v_lineage_edges`,
    params: []
  }),

  getLineageNodes: (): QueryDef => ({
    sql: `SELECT path, name, folder, materialized FROM v_models
          UNION ALL
          SELECT 'source:' || name, name, 'sources', 'source' FROM v_sources`,
    params: []
  }),

  // Column lineage queries
  getColumnLineageNodes: (): QueryDef => ({
    sql: `SELECT id, model, column_name FROM v_column_lineage_nodes`,
    params: []
  }),

  getColumnLineageEdges: (): QueryDef => ({
    sql: `SELECT source_id, target_id FROM v_column_lineage_edges`,
    params: []
  }),

  // Get column lineage for a specific model
  getColumnLineageForModel: (modelPath: string): QueryDef => ({
    sql: `SELECT DISTINCT cln.id, cln.model, cln.column_name
          FROM v_column_lineage_nodes cln
          WHERE cln.model = ?
          UNION
          SELECT DISTINCT cln2.id, cln2.model, cln2.column_name
          FROM v_column_lineage_nodes cln
          JOIN v_column_lineage_edges cle ON cln.id = cle.target_id
          JOIN v_column_lineage_nodes cln2 ON cle.source_id = cln2.id
          WHERE cln.model = ?`,
    params: [modelPath, modelPath]
  }),

  getColumnLineageEdgesForModel: (modelPath: string): QueryDef => ({
    sql: `SELECT cle.source_id, cle.target_id
          FROM v_column_lineage_edges cle
          JOIN v_column_lineage_nodes cln ON cle.target_id = cln.id
          WHERE cln.model = ?`,
    params: [modelPath]
  }),

  // Trace column lineage with depth limit to prevent infinite loops
  traceColumnLineage: (modelPath: string, columnName: string): QueryDef => ({
    sql: `
      WITH RECURSIVE lineage AS (
        SELECT source_table, source_column, 1 as depth
        FROM v_column_sources
        WHERE model_path = ? AND column_name = ?
        
        UNION ALL
        
        SELECT cs.source_table, cs.source_column, l.depth + 1
        FROM v_column_sources cs
        JOIN lineage l ON cs.model_path = l.source_table 
                      AND cs.column_name = l.source_column
        WHERE l.depth < 10
      )
      SELECT DISTINCT source_table, source_column, depth 
      FROM lineage
      ORDER BY depth
      LIMIT 100
    `,
    params: [modelPath, columnName]
  }),

  // Full-text search (join through models table for rowid)
  searchModels: (term: string): QueryDef => ({
    sql: `
      SELECT v.path, v.name, v.folder, v.materialized, v.description
      FROM models m
      JOIN models_fts fts ON m.rowid = fts.rowid
      JOIN v_models v ON m.path = v.path
      WHERE models_fts MATCH ?
      ORDER BY rank
      LIMIT 20
    `,
    params: [term + '*']  // Prefix search
  }),

  // Project metadata (uses project_meta table in state.db)
  getProjectName: (): QueryDef => ({
    sql: `SELECT value FROM project_meta WHERE key = 'project_name'`,
    params: []
  }),

  getGeneratedAt: (): QueryDef => ({
    sql: `SELECT value FROM project_meta WHERE key = 'generated_at'`,
    params: []
  }),

  // Stats queries
  getModelCount: (): QueryDef => ({
    sql: `SELECT COUNT(*) as count FROM models`,
    params: []
  }),

  getSourceCount: (): QueryDef => ({
    sql: `SELECT COUNT(*) as count FROM v_sources`,
    params: []
  }),

  getColumnCount: (): QueryDef => ({
    sql: `SELECT COUNT(*) as count FROM model_columns`,
    params: []
  }),

  getMaterializationCounts: (): QueryDef => ({
    sql: `SELECT materialized, COUNT(*) as count FROM models GROUP BY materialized`,
    params: []
  }),

  getFolderCount: (): QueryDef => ({
    sql: `SELECT COUNT(DISTINCT folder) as count FROM v_models`,
    params: []
  }),
};

// Type definitions for query results
export interface ModelRow {
  path: string;
  name: string;
  folder: string;
  materialized: string;
  unique_key: string | null;
  sql_content?: string;
  description: string | null;
  file_path: string | null;
  updated_at: string;
}

export interface ColumnRow {
  name: string;
  idx: number;
  transform_type: string | null;
  function_name: string | null;
}

export interface ColumnSourceRow {
  column_name: string;
  source_table: string;
  source_column: string;
}

export interface LineageEdgeRow {
  source_node: string;
  target_node: string;
}

export interface ColumnLineageNodeRow {
  id: string;
  model: string;
  column_name: string;
}

export interface ColumnLineageEdgeRow {
  source_id: string;
  target_id: string;
}

export interface SearchResultRow {
  path: string;
  name: string;
  folder: string;
  materialized: string;
  description: string | null;
}
