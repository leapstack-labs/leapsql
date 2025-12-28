// Overview page component
import type { FunctionComponent } from 'preact';
import { useManifest, useManifestStats, useRecentModels, useCatalogStats } from '../../lib/context';
import { navigateToModel } from '../../lib/router';

interface OverviewProps {
  dbReady: boolean;
}

// Skeleton component for loading states
const Skeleton: FunctionComponent<{ width?: string; height?: string }> = ({ 
  width = '100%', 
  height = '1em' 
}) => (
  <div 
    class="skeleton" 
    style={{ 
      width, 
      height, 
      backgroundColor: 'var(--bg-tertiary)', 
      borderRadius: '4px',
      animation: 'pulse 1.5s ease-in-out infinite'
    }} 
  />
);

export const Overview: FunctionComponent<OverviewProps> = ({ dbReady }) => {
  const manifest = useManifest();
  const manifestStats = useManifestStats();
  const recentModels = useRecentModels(10);
  const dbStats = useCatalogStats();

  // Use manifest stats immediately, then update with DB stats when ready
  const stats = dbStats.data || {
    totalModels: manifestStats.model_count,
    totalSources: manifestStats.source_count,
    folderCount: manifestStats.folder_count,
    tableCount: manifestStats.table_count,
    viewCount: manifestStats.view_count,
  };

  return (
    <>
      <div class="page-header">
        <h1>Overview</h1>
        <p class="description">
          Documentation for {manifest.project_name || 'your data models'}
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
        {!dbReady || recentModels.loading ? (
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
              {[...Array(5)].map((_, i) => (
                <tr key={i}>
                  <td><Skeleton width="120px" /></td>
                  <td><Skeleton width="180px" /></td>
                  <td><Skeleton width="60px" /></td>
                  <td><Skeleton width="30px" /></td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : recentModels.error ? (
          <div class="error-message">Failed to load models: {recentModels.error.message}</div>
        ) : (
          <table class="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Path</th>
                <th>Type</th>
              </tr>
            </thead>
            <tbody>
              {recentModels.data?.map((model) => (
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
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div class="section">
        <h2 class="section-title">Generated</h2>
        <p style={{ color: 'var(--text-secondary)' }}>
          {new Date(manifest.generated_at).toLocaleString()}
        </p>
      </div>
    </>
  );
};
