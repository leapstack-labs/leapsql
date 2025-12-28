// Overview page component
import type { FunctionComponent } from 'preact';
import { useCatalog, useCatalogStats } from '../../lib/context';
import { navigateToModel } from '../../lib/router';

export const Overview: FunctionComponent = () => {
  const { catalog } = useCatalog();
  const stats = useCatalogStats();

  return (
    <>
      <div class="page-header">
        <h1>Overview</h1>
        <p class="description">
          Documentation for {catalog.project_name || 'your data models'}
        </p>
      </div>

      <div class="stats-grid">
        <div class="stat-card">
          <div class="stat-value">{stats.totalModels}</div>
          <div class="stat-label">Total Models</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{stats.folderCount}</div>
          <div class="stat-label">Folders</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{stats.tableCount}</div>
          <div class="stat-label">Tables</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{stats.viewCount}</div>
          <div class="stat-label">Views</div>
        </div>
      </div>

      <div class="section">
        <h2 class="section-title">Recent Models</h2>
        <table class="data-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Path</th>
              <th>Type</th>
              <th>Dependencies</th>
            </tr>
          </thead>
          <tbody>
            {catalog.models.slice(0, 10).map((model) => (
              <tr
                key={model.path}
                onClick={() => navigateToModel(model.path)}
                style={{ cursor: 'pointer' }}
              >
                <td>
                  <strong>{model.name}</strong>
                </td>
                <td>
                  <code>{model.path}</code>
                </td>
                <td>
                  <span class={`model-badge ${model.materialized}`}>
                    {model.materialized}
                  </span>
                </td>
                <td>{(model.dependencies || []).length}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div class="section">
        <h2 class="section-title">Generated</h2>
        <p style={{ color: 'var(--text-secondary)' }}>
          {new Date(catalog.generated_at).toLocaleString()}
        </p>
      </div>
    </>
  );
};
