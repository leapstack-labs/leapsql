// Preact context for catalog data
import { createContext } from 'preact';
import { useContext, useMemo } from 'preact/hooks';
import type { Catalog, ModelDoc, SourceDoc } from './types';

// Context type
export interface CatalogContextValue {
  catalog: Catalog;
  modelsByPath: Map<string, ModelDoc>;
  sourcesByName: Map<string, SourceDoc>;
  getModel: (path: string) => ModelDoc | undefined;
  getSource: (name: string) => SourceDoc | undefined;
}

// Create the context
export const CatalogContext = createContext<CatalogContextValue | null>(null);

// Hook to use the catalog context
export function useCatalog(): CatalogContextValue {
  const ctx = useContext(CatalogContext);
  if (!ctx) {
    throw new Error('useCatalog must be used within a CatalogProvider');
  }
  return ctx;
}

// Create context value from catalog data
export function createCatalogContext(catalog: Catalog): CatalogContextValue {
  const modelsByPath = new Map<string, ModelDoc>();
  catalog.models.forEach(model => {
    modelsByPath.set(model.path, model);
  });

  const sourcesByName = new Map<string, SourceDoc>();
  (catalog.sources || []).forEach(source => {
    sourcesByName.set(source.name, source);
  });

  return {
    catalog,
    modelsByPath,
    sourcesByName,
    getModel: (path: string) => modelsByPath.get(path),
    getSource: (name: string) => sourcesByName.get(name),
  };
}

// Hook to compute derived data from catalog
export function useCatalogStats() {
  const { catalog } = useCatalog();
  
  return useMemo(() => {
    const materializations: Record<string, number> = {};
    const folders = new Set<string>();
    
    catalog.models.forEach(model => {
      materializations[model.materialized] = (materializations[model.materialized] || 0) + 1;
      const folder = model.path.split('.')[0];
      folders.add(folder);
    });
    
    return {
      totalModels: catalog.models.length,
      totalSources: (catalog.sources || []).length,
      folderCount: folders.size,
      tableCount: materializations.table || 0,
      viewCount: materializations.view || 0,
      materializations,
      folders: Array.from(folders).sort(),
    };
  }, [catalog]);
}

// Hook to group models by folder
export function useModelsByFolder() {
  const { catalog } = useCatalog();
  
  return useMemo(() => {
    const groups: Record<string, ModelDoc[]> = {};
    
    catalog.models.forEach(model => {
      const parts = model.path.split('.');
      const folder = parts.length > 1 ? parts[0] : 'default';
      if (!groups[folder]) {
        groups[folder] = [];
      }
      groups[folder].push(model);
    });
    
    // Sort models within each group
    Object.values(groups).forEach(models => {
      models.sort((a, b) => a.name.localeCompare(b.name));
    });
    
    return groups;
  }, [catalog]);
}
