// MiniSearch configuration for fuzzy search
import MiniSearch from 'minisearch';
import type { ModelDoc, SourceDoc } from './types';

// Searchable item type (union of models and sources)
export interface SearchItem {
  id: string;
  type: 'model' | 'source';
  name: string;
  path?: string;
  description?: string;
  folder?: string;
}

// Create and configure a MiniSearch instance for models and sources
export function createSearchIndex(models: ModelDoc[], sources: SourceDoc[]): MiniSearch<SearchItem> {
  const search = new MiniSearch<SearchItem>({
    fields: ['name', 'path', 'description', 'folder'],
    storeFields: ['id', 'type', 'name', 'path', 'description', 'folder'],
    searchOptions: {
      prefix: true,
      fuzzy: 0.2,
      boost: {
        name: 2,
        path: 1.5,
      },
    },
  });

  // Add models to the index
  const items: SearchItem[] = models.map(model => ({
    id: `model:${model.path}`,
    type: 'model' as const,
    name: model.name,
    path: model.path,
    description: model.description,
    folder: model.path.split('.')[0],
  }));

  // Add sources to the index
  sources.forEach(source => {
    items.push({
      id: `source:${source.name}`,
      type: 'source' as const,
      name: source.name,
    });
  });

  search.addAll(items);
  return search;
}

// Search result type
export interface SearchResult {
  id: string;
  type: 'model' | 'source';
  name: string;
  path?: string;
  description?: string;
  folder?: string;
  score: number;
}

// Perform a search and return results
export function performSearch(
  search: MiniSearch<SearchItem>,
  query: string,
  limit = 20
): SearchResult[] {
  if (!query.trim()) {
    return [];
  }

  const results = search.search(query, { limit });
  
  return results.map(result => ({
    id: result.id,
    type: result.type as 'model' | 'source',
    name: result.name as string,
    path: result.path as string | undefined,
    description: result.description as string | undefined,
    folder: result.folder as string | undefined,
    score: result.score,
  }));
}
