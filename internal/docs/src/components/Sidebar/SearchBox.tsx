// Search box component with MiniSearch
import type { FunctionComponent } from 'preact';
import { useState, useMemo, useCallback } from 'preact/hooks';
import { useCatalog } from '../../lib/context';
import { createSearchIndex, performSearch, type SearchResult } from '../../lib/search';
import { navigateToModel, navigateToSource } from '../../lib/router';

export const SearchBox: FunctionComponent = () => {
  const { catalog } = useCatalog();
  const [query, setQuery] = useState('');
  const [isFocused, setIsFocused] = useState(false);

  const searchIndex = useMemo(
    () => createSearchIndex(catalog.models, catalog.sources || []),
    [catalog]
  );

  const results = useMemo(
    () => performSearch(searchIndex, query, 10),
    [searchIndex, query]
  );

  const handleSelect = useCallback((result: SearchResult) => {
    if (result.type === 'model' && result.path) {
      navigateToModel(result.path);
    } else if (result.type === 'source') {
      navigateToSource(result.name);
    }
    setQuery('');
    setIsFocused(false);
  }, []);

  const showResults = isFocused && query.length > 0 && results.length > 0;

  return (
    <div class="search-container">
      <input
        type="text"
        class="search-input"
        placeholder="Search..."
        value={query}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        onFocus={() => setIsFocused(true)}
        onBlur={() => setTimeout(() => setIsFocused(false), 200)}
      />
      {showResults && (
        <div class="search-results">
          {results.map((result) => (
            <div
              key={result.id}
              class={`search-result ${result.type}`}
              onClick={() => handleSelect(result)}
            >
              <span class="search-result-name">{result.name}</span>
              {result.path && result.path !== result.name && (
                <span class="search-result-path">{result.path}</span>
              )}
              <span class={`search-result-type ${result.type}`}>
                {result.type}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};
