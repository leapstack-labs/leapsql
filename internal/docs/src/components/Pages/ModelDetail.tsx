// Model detail page component
import type { FunctionComponent } from 'preact';
import { useEffect, useRef } from 'preact/hooks';
import { useCatalog } from '../../lib/context';
import { navigateToModel, navigateToSource } from '../../lib/router';
import { NotFound } from './NotFound';
import { ColumnLineageGraph } from '../Graph/ColumnLineage';
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';

// Register SQL language
hljs.registerLanguage('sql', sql);

interface ModelDetailProps {
  path: string;
}

// Escape HTML special characters
function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

export const ModelDetail: FunctionComponent<ModelDetailProps> = ({ path }) => {
  const { getModel, modelsByPath, catalog } = useCatalog();
  const model = getModel(path);
  const codeRef = useRef<HTMLElement>(null);

  // Highlight code on mount
  useEffect(() => {
    if (codeRef.current && model) {
      hljs.highlightElement(codeRef.current);
    }
  }, [model, path]);

  if (!model) {
    return <NotFound message={`Model "${path}" not found`} />;
  }

  // Find external sources (sources that aren't models)
  const externalSources = model.sources.filter(
    (src) => !modelsByPath.has(src) && !catalog.models.some((m) => m.name === src)
  );

  return (
    <>
      <div class="model-header">
        <div>
          <h1 class="model-title">{model.name}</h1>
          <div class="model-path">{model.path}</div>
          {model.description && (
            <p style={{ marginTop: '1rem', color: 'var(--text-secondary)' }}>
              {model.description}
            </p>
          )}
          <div class="model-meta">
            <div class="meta-item">
              <span class="label">Type:</span>
              <span class={`model-badge ${model.materialized}`}>
                {model.materialized}
              </span>
            </div>
            {model.unique_key && (
              <div class="meta-item">
                <span class="label">Unique Key:</span>
                <code>{model.unique_key}</code>
              </div>
            )}
          </div>
        </div>
      </div>

      {model.dependencies.length > 0 && (
        <div class="section">
          <h2 class="section-title">Dependencies ({model.dependencies.length})</h2>
          <div class="dep-list">
            {model.dependencies.map((dep) => (
              <a
                key={dep}
                class="dep-tag"
                onClick={() => navigateToModel(dep)}
              >
                {dep}
              </a>
            ))}
          </div>
        </div>
      )}

      {model.dependents.length > 0 && (
        <div class="section">
          <h2 class="section-title">Dependents ({model.dependents.length})</h2>
          <div class="dep-list">
            {model.dependents.map((dep) => (
              <a
                key={dep}
                class="dep-tag"
                onClick={() => navigateToModel(dep)}
              >
                {dep}
              </a>
            ))}
          </div>
        </div>
      )}

      {externalSources.length > 0 && (
        <div class="section">
          <h2 class="section-title">
            External Sources ({externalSources.length})
          </h2>
          <div class="dep-list">
            {externalSources.map((src) => (
              <span
                key={src}
                class="dep-tag external"
                onClick={() => navigateToSource(src)}
              >
                {src}
              </span>
            ))}
          </div>
        </div>
      )}

      {model.columns && model.columns.length > 0 && (
        <div class="section">
          <h2 class="section-title">Columns ({model.columns.length})</h2>
          <table class="data-table columns-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Transform</th>
                <th>Sources</th>
              </tr>
            </thead>
            <tbody>
              {model.columns.map((col) => (
                <tr key={col.name}>
                  <td>
                    <code class="column-name">{escapeHtml(col.name)}</code>
                  </td>
                  <td>
                    {col.transform_type === 'EXPR' ? (
                      <span class="transform-badge expr">
                        {col.function || 'expression'}
                      </span>
                    ) : (
                      <span class="transform-badge direct">direct</span>
                    )}
                  </td>
                  <td>
                    {col.sources && col.sources.length > 0 ? (
                      <div class="source-list">
                        {col.sources.map((src, idx) => (
                          <span
                            key={idx}
                            class="source-ref"
                            title={`${src.table}.${src.column}`}
                          >
                            {src.table && (
                              <>
                                <span class="source-table">{src.table}</span>.
                              </>
                            )}
                            <span class="source-column">{src.column}</span>
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span class="no-sources">-</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div class="section">
        <h2 class="section-title">Column Lineage</h2>
        <ColumnLineageGraph modelPath={path} />
        <div class="column-lineage-legend">
          <div class="legend-item">
            <div
              class="legend-color"
              style={{ background: 'var(--accent-green)' }}
            />
            <span>Current model</span>
          </div>
          <div class="legend-item">
            <div
              class="legend-color"
              style={{ background: 'var(--accent-blue)' }}
            />
            <span>Source models</span>
          </div>
          <div class="legend-item">
            <div
              class="legend-color"
              style={{ background: 'var(--accent-orange)' }}
            />
            <span>External sources</span>
          </div>
        </div>
      </div>

      <div class="section">
        <h2 class="section-title">SQL</h2>
        <div class="code-block">
          <div class="code-header">
            <span class="code-title">
              {model.file_path || model.path + '.sql'}
            </span>
          </div>
          <div class="code-content">
            <pre>
              <code ref={codeRef} class="language-sql">
                {model.sql}
              </code>
            </pre>
          </div>
        </div>
      </div>
    </>
  );
};
