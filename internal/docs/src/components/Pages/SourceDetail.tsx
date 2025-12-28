// Source detail page component
import type { FunctionComponent } from 'preact';
import { useCatalog } from '../../lib/context';
import { navigateToModel } from '../../lib/router';
import { NotFound } from './NotFound';

interface SourceDetailProps {
  name: string;
}

export const SourceDetail: FunctionComponent<SourceDetailProps> = ({ name }) => {
  const { getSource } = useCatalog();
  const source = getSource(name);

  if (!source) {
    return <NotFound message={`Source "${name}" not found`} />;
  }

  return (
    <>
      <div class="model-header">
        <div>
          <div class="source-badge-header">SOURCE</div>
          <h1 class="model-title">{source.name}</h1>
          <p style={{ marginTop: '1rem', color: 'var(--text-secondary)' }}>
            External data source referenced by {source.referenced_by.length} model
            {source.referenced_by.length !== 1 ? 's' : ''}.
          </p>
        </div>
      </div>

      <div class="section">
        <h2 class="section-title">
          Referenced By ({source.referenced_by.length})
        </h2>
        <div class="dep-list">
          {source.referenced_by.map((modelPath) => (
            <a
              key={modelPath}
              class="dep-tag"
              onClick={() => navigateToModel(modelPath)}
            >
              {modelPath}
            </a>
          ))}
        </div>
      </div>

      <div class="section">
        <h2 class="section-title">About External Sources</h2>
        <div class="info-box">
          <p>
            External sources are tables or datasets that exist outside of this
            project's managed models. They typically represent raw data from
            databases, data warehouses, or other systems that serve as inputs to
            your data transformations.
          </p>
          <p style={{ marginTop: '0.75rem' }}>
            This source (<code>{source.name}</code>) is referenced in the SQL of
            the models listed above.
          </p>
        </div>
      </div>
    </>
  );
};
