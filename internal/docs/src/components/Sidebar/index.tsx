// Sidebar component with navigation and search
import type { FunctionComponent } from 'preact';
import { useCatalog, useModelsByFolder } from '../../lib/context';
import { SearchBox } from './SearchBox';
import { NavGroup } from './NavGroup';

export const Sidebar: FunctionComponent = () => {
  const { catalog } = useCatalog();
  const modelsByFolder = useModelsByFolder();

  return (
    <aside class="sidebar">
      <div class="sidebar-header">
        <h1>{catalog.project_name || 'LeapSQL'}</h1>
        <div class="subtitle">Documentation</div>
      </div>

      <SearchBox />

      <nav class="sidebar-nav">
        <div class="nav-section">
          <a href="#/" class="nav-link">
            <span class="icon">&#9776;</span>
            Overview
          </a>
          <a href="#/lineage" class="nav-link">
            <span class="icon">&#9670;</span>
            Lineage
          </a>
        </div>

        <div class="nav-section" id="models-nav">
          <div class="nav-group-list">
            {/* Sources group */}
            {catalog.sources && catalog.sources.length > 0 && (
              <NavGroup
                title="Sources"
                groupId="sources"
                items={catalog.sources.map(src => ({
                  id: src.name,
                  name: src.name,
                  type: 'source' as const,
                }))}
              />
            )}

            {/* Model groups by folder */}
            {Object.entries(modelsByFolder)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([folder, models]) => (
                <NavGroup
                  key={folder}
                  title={folder}
                  groupId={folder}
                  items={models.map(m => ({
                    id: m.path,
                    name: m.name,
                    type: 'model' as const,
                    badge: m.materialized,
                  }))}
                />
              ))}
          </div>
        </div>
      </nav>
    </aside>
  );
};
