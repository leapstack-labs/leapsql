// Preact contexts for manifest and database
import { createContext } from 'preact';
import { useContext, useMemo, useState, useEffect } from 'preact/hooks';
import type { DatabaseAdapter } from './db/types';
import { queries, rowsToObjects, firstRow } from './db';
import type { 
  ModelRow, 
  ColumnRow, 
  ColumnSourceRow,
  LineageEdgeRow,
  ColumnLineageNodeRow,
  ColumnLineageEdgeRow,
  SearchResultRow,
} from './db';
import type { 
  Manifest, 
  NavGroup, 
  ModelDoc, 
  SourceDoc, 
  ColumnDoc,
  SourceRef,
} from './types';

// ============================================
// Manifest Context (for instant shell render)
// ============================================

export const ManifestContext = createContext<Manifest | null>(null);

export function useManifest(): Manifest {
  const manifest = useContext(ManifestContext);
  if (!manifest) {
    throw new Error('useManifest must be used within a ManifestContext.Provider');
  }
  return manifest;
}

// Hook to group models by folder from manifest (instant render)
export function useNavTree(): NavGroup[] {
  const manifest = useManifest();
  return manifest.nav_tree;
}

// Hook to get stats from manifest (instant render)
export function useManifestStats() {
  const manifest = useManifest();
  return manifest.stats;
}

// ============================================
// Database Context
// ============================================

export const DatabaseContext = createContext<DatabaseAdapter | null>(null);

export function useDB(): DatabaseAdapter | null {
  return useContext(DatabaseContext);
}

export function useDBReady(): boolean {
  const db = useDB();
  return db !== null && db.isReady();
}

// ============================================
// Data Fetching Hooks (use database queries)
// ============================================

interface AsyncState<T> {
  data: T | null;
  loading: boolean;
  error: Error | null;
}

// Generic hook for async data fetching
function useAsyncQuery<T>(
  queryFn: () => Promise<T>,
  deps: any[]
): AsyncState<T> {
  const [state, setState] = useState<AsyncState<T>>({
    data: null,
    loading: true,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;
    setState(s => ({ ...s, loading: true, error: null }));

    queryFn()
      .then(data => {
        if (!cancelled) {
          setState({ data, loading: false, error: null });
        }
      })
      .catch(error => {
        if (!cancelled) {
          setState({ data: null, loading: false, error });
        }
      });

    return () => { cancelled = true; };
  }, deps);

  return state;
}

// Hook to get a model by path
export function useModel(path: string): AsyncState<ModelDoc | null> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) return null;

    // Fetch model basic info
    const { sql, params } = queries.getModel(path);
    const result = await db.query(sql, params);
    const model = firstRow<ModelRow>(result);
    if (!model) return null;

    // Fetch dependencies
    const depsResult = await db.query(...Object.values(queries.getModelDependencies(path)));
    const dependencies = depsResult.values.map(row => row[0] as string);

    // Fetch dependents
    const deptsResult = await db.query(...Object.values(queries.getModelDependents(path)));
    const dependents = deptsResult.values.map(row => row[0] as string);

    // Fetch sources
    const sourcesResult = await db.query(...Object.values(queries.getModelSources(path)));
    const sources = sourcesResult.values.map(row => row[0] as string);

    // Fetch columns
    const colsResult = await db.query(...Object.values(queries.getModelColumns(path)));
    const columns = rowsToObjects<ColumnRow>(colsResult);

    // Fetch column sources
    const colSourcesResult = await db.query(...Object.values(queries.getAllColumnSources(path)));
    const columnSourcesRaw = rowsToObjects<ColumnSourceRow>(colSourcesResult);

    // Group column sources by column name
    const columnSourcesMap = new Map<string, SourceRef[]>();
    for (const cs of columnSourcesRaw) {
      if (!columnSourcesMap.has(cs.column_name)) {
        columnSourcesMap.set(cs.column_name, []);
      }
      columnSourcesMap.get(cs.column_name)!.push({
        table: cs.source_table,
        column: cs.source_column,
      });
    }

    // Convert to ModelDoc format
    const modelDoc: ModelDoc = {
      id: model.path,
      name: model.name,
      path: model.path,
      materialized: model.materialized,
      unique_key: model.unique_key || undefined,
      sql: model.sql_content || '',
      file_path: model.file_path || '',
      sources,
      dependencies,
      dependents,
      description: model.description || undefined,
      updated_at: model.updated_at,
      columns: columns.map(col => ({
        name: col.name,
        index: col.idx,
        transform_type: col.transform_type || undefined,
        function: col.function_name || undefined,
        sources: columnSourcesMap.get(col.name) || [],
      })),
    };

    return modelDoc;
  }, [db, path]);
}

// Hook to get a source by name
export function useSource(name: string): AsyncState<SourceDoc | null> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) return null;

    // Check source exists
    const { sql, params } = queries.getSource(name);
    const result = await db.query(sql, params);
    if (result.values.length === 0) return null;

    // Get referenced by
    const refsResult = await db.query(...Object.values(queries.getSourceReferencedBy(name)));
    const referencedBy = refsResult.values.map(row => row[0] as string);

    return {
      name,
      referenced_by: referencedBy,
    };
  }, [db, name]);
}

// Hook to get all sources
export function useSources(): AsyncState<SourceDoc[]> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) return [];

    const { sql, params } = queries.getSources();
    const result = await db.query(sql, params);
    const sourceNames = result.values.map(row => row[0] as string);

    // Fetch referenced_by for each source
    const sources: SourceDoc[] = [];
    for (const name of sourceNames) {
      const refsResult = await db.query(...Object.values(queries.getSourceReferencedBy(name)));
      sources.push({
        name,
        referenced_by: refsResult.values.map(row => row[0] as string),
      });
    }

    return sources;
  }, [db]);
}

// Hook to get lineage data
export function useLineage(): AsyncState<{ nodes: string[]; edges: { source: string; target: string }[] }> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) return { nodes: [], edges: [] };

    // Get nodes
    const nodesResult = await db.query(...Object.values(queries.getLineageNodes()));
    const nodes = nodesResult.values.map(row => row[0] as string);

    // Get edges
    const edgesResult = await db.query(...Object.values(queries.getLineageEdges()));
    const edges = rowsToObjects<LineageEdgeRow>(edgesResult).map(e => ({
      source: e.source_node,
      target: e.target_node,
    }));

    return { nodes, edges };
  }, [db]);
}

// Hook for column lineage for a specific model
export function useColumnLineage(modelPath: string): AsyncState<{
  nodes: { id: string; model: string; column: string }[];
  edges: { source: string; target: string }[];
}> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) return { nodes: [], edges: [] };

    // Get nodes
    const nodesResult = await db.query(...Object.values(queries.getColumnLineageForModel(modelPath)));
    const nodes = rowsToObjects<ColumnLineageNodeRow>(nodesResult).map(n => ({
      id: n.id,
      model: n.model,
      column: n.column_name,
    }));

    // Get edges
    const edgesResult = await db.query(...Object.values(queries.getColumnLineageEdgesForModel(modelPath)));
    const edges = rowsToObjects<ColumnLineageEdgeRow>(edgesResult).map(e => ({
      source: e.source_id,
      target: e.target_id,
    }));

    return { nodes, edges };
  }, [db, modelPath]);
}

// Hook for search
export function useSearch(term: string): AsyncState<SearchResultRow[]> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady() || !term.trim()) return [];

    const { sql, params } = queries.searchModels(term);
    const result = await db.query(sql, params);
    return rowsToObjects<SearchResultRow>(result);
  }, [db, term]);
}

// Hook for catalog stats (from database, not manifest)
export function useCatalogStats(): AsyncState<{
  totalModels: number;
  totalSources: number;
  totalColumns: number;
  folderCount: number;
  tableCount: number;
  viewCount: number;
  materializations: Record<string, number>;
}> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) {
      return {
        totalModels: 0,
        totalSources: 0,
        totalColumns: 0,
        folderCount: 0,
        tableCount: 0,
        viewCount: 0,
        materializations: {},
      };
    }

    const [modelCount, sourceCount, columnCount, folderCount, matCounts] = await Promise.all([
      db.query(...Object.values(queries.getModelCount())),
      db.query(...Object.values(queries.getSourceCount())),
      db.query(...Object.values(queries.getColumnCount())),
      db.query(...Object.values(queries.getFolderCount())),
      db.query(...Object.values(queries.getMaterializationCounts())),
    ]);

    const materializations: Record<string, number> = {};
    matCounts.values.forEach(row => {
      materializations[row[0] as string] = row[1] as number;
    });

    return {
      totalModels: modelCount.values[0]?.[0] as number || 0,
      totalSources: sourceCount.values[0]?.[0] as number || 0,
      totalColumns: columnCount.values[0]?.[0] as number || 0,
      folderCount: folderCount.values[0]?.[0] as number || 0,
      tableCount: materializations['table'] || 0,
      viewCount: materializations['view'] || 0,
      materializations,
    };
  }, [db]);
}

// Hook to get recent models for overview
export function useRecentModels(limit: number = 10): AsyncState<ModelRow[]> {
  const db = useDB();

  return useAsyncQuery(async () => {
    if (!db || !db.isReady()) return [];

    const { sql, params } = queries.getModels();
    const result = await db.query(sql, params);
    const models = rowsToObjects<ModelRow>(result);
    return models.slice(0, limit);
  }, [db, limit]);
}

// ============================================
// Legacy compatibility (for gradual migration)
// ============================================

// This provides the old CatalogContext interface for components
// that haven't been migrated yet
export interface CatalogContextValue {
  catalog: {
    project_name: string;
    generated_at: string;
    models: ModelDoc[];
    sources: SourceDoc[];
  };
  modelsByPath: Map<string, ModelDoc>;
  sourcesByName: Map<string, SourceDoc>;
  getModel: (path: string) => ModelDoc | undefined;
  getSource: (name: string) => SourceDoc | undefined;
}

export const CatalogContext = createContext<CatalogContextValue | null>(null);

export function useCatalog(): CatalogContextValue {
  const ctx = useContext(CatalogContext);
  if (!ctx) {
    throw new Error('useCatalog must be used within a CatalogProvider');
  }
  return ctx;
}

// Helper to create CatalogContextValue from loaded data
export function createCatalogContext(
  projectName: string,
  generatedAt: string,
  models: ModelDoc[],
  sources: SourceDoc[]
): CatalogContextValue {
  const modelsByPath = new Map<string, ModelDoc>();
  models.forEach(model => {
    modelsByPath.set(model.path, model);
  });

  const sourcesByName = new Map<string, SourceDoc>();
  sources.forEach(source => {
    sourcesByName.set(source.name, source);
  });

  return {
    catalog: {
      project_name: projectName,
      generated_at: generatedAt,
      models,
      sources,
    },
    modelsByPath,
    sourcesByName,
    getModel: (path: string) => modelsByPath.get(path),
    getSource: (name: string) => sourcesByName.get(name),
  };
}

// Hook to group models by folder (uses manifest for instant render)
export function useModelsByFolder(): Record<string, NavGroup['models']> {
  const navTree = useNavTree();

  return useMemo(() => {
    const groups: Record<string, NavGroup['models']> = {};
    navTree.forEach(group => {
      groups[group.folder] = group.models;
    });
    return groups;
  }, [navTree]);
}
