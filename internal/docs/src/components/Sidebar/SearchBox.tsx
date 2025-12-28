// Search box component with database FTS5 search
import type { FunctionComponent } from 'preact';
import { useState, useCallback, useMemo } from 'preact/hooks';
import { useSearch } from '../../lib/context';
import { navigateToModel, navigateToSource } from '../../lib/router';
import type { SearchResultRow } from '../../lib/db/queries';

interface SearchBoxProps {
  dbReady: boolean;
}

export const SearchBox: FunctionComponent<SearchBoxProps> = ({ dbReady }) => {
  const [query, setQuery] = useState('');
  const [isFocused, setIsFocused] = useState(false);

  // Debounce search query
  const searchTerm = useMemo(() => {
    return query.trim().length >= 2 ? query.trim() : '';
  }, [query]);

  const { data: results, loading } = useSearch(searchTerm);

  const handleSelect = useCallback((result: SearchResultRow) => {
    navigateToModel(result.path);
    setQuery('');
    setIsFocused(false);
  }, []);

  const showResults = isFocused && query.length >= 2 && results && results.length > 0;

  return (
    <div class="search-container">
      <input
        type="text"
        class="search-input"
        placeholder={dbReady ? "Search..." : "Loading..."}
        value={query}
        disabled={!dbReady}
        onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        onFocus={() => setIsFocused(true)}
        onBlur={() => setTimeout(() => setIsFocused(false), 200)}
      />
      {showResults && (
        <div class="search-results">
          {results.map((result) => (
            <div
              key={result.path}
              class="search-result model"
              onClick={() => handleSelect(result)}
            >
              <span class="search-result-name">{result.name}</span>
              {result.path !== result.name && (
                <span class="search-result-path">{result.path}</span>
              )}
              <span class={`search-result-type model`}>
                {result.materialized}
              </span>
            </div>
          ))}
        </div>
      )}
      {isFocused && query.length >= 2 && loading && (
        <div class="search-results">
          <div class="search-result" style={{ opacity: 0.5, cursor: 'default' }}>
            Searching...
          </div>
        </div>
      )}
      {isFocused && query.length >= 2 && !loading && results && results.length === 0 && (
        <div class="search-results">
          <div class="search-result" style={{ opacity: 0.5, cursor: 'default' }}>
            No results found
          </div>
        </div>
      )}
    </div>
  );
};
