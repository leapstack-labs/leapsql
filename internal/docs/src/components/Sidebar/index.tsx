// Sidebar component with navigation and search
import type { FunctionComponent } from 'preact';
import { useManifest, useNavTree, useSources, useMacros } from '../../lib/context';
import { SearchBox } from './SearchBox';
import { NavGroup } from './NavGroup';

interface SidebarProps {
  dbReady: boolean;
}

export const Sidebar: FunctionComponent<SidebarProps> = ({ dbReady }) => {
  const manifest = useManifest();
  const navTree = useNavTree();
  const sources = useSources();
  const macros = useMacros();

  return (
    <aside class="sidebar">
      <div class="sidebar-header">
        <h1>{manifest.project_name || 'LeapSQL'}</h1>
        <div class="subtitle">Documentation</div>
      </div>

      <SearchBox dbReady={dbReady} />

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
            {/* Sources group - loads from database when ready */}
            {sources.data && sources.data.length > 0 && (
              <NavGroup
                title="Sources"
                groupId="sources"
                items={sources.data.map(src => ({
                  id: src.name,
                  name: src.name,
                  type: 'source' as const,
                }))}
              />
            )}

            {/* Macros group - loads from database when ready */}
            {macros.data && macros.data.length > 0 && (
              <NavGroup
                title="Macros"
                groupId="macros"
                items={macros.data.map(m => ({
                  id: m.namespace,
                  name: m.namespace,
                  type: 'macro' as const,
                  badge: m.functions.length > 0 ? `${m.functions.length}` : undefined,
                }))}
              />
            )}

            {/* Model groups by folder - instant from manifest */}
            {navTree
              .sort((a, b) => a.folder.localeCompare(b.folder))
              .map((group) => (
                <NavGroup
                  key={group.folder}
                  title={group.folder}
                  groupId={group.folder}
                  items={group.models.map(m => ({
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
