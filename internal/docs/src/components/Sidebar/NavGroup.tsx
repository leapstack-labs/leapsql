// Collapsible navigation group component
import type { FunctionComponent } from 'preact';
import { useState, useEffect } from 'preact/hooks';
import { navigateToModel, navigateToSource, navigateToMacro } from '../../lib/router';

interface NavItem {
  id: string;
  name: string;
  type: 'model' | 'source' | 'macro';
  badge?: string;
}

interface NavGroupProps {
  title: string;
  groupId: string;
  items: NavItem[];
}

// Get collapsed state from localStorage
function getCollapsedState(): Record<string, boolean> {
  try {
    const stored = localStorage.getItem('leapsql-nav-collapsed');
    return stored ? JSON.parse(stored) : {};
  } catch {
    return {};
  }
}

// Save collapsed state to localStorage
function saveCollapsedState(state: Record<string, boolean>): void {
  try {
    localStorage.setItem('leapsql-nav-collapsed', JSON.stringify(state));
  } catch {
    // Ignore storage errors
  }
}

export const NavGroup: FunctionComponent<NavGroupProps> = ({ title, groupId, items }) => {
  const [isCollapsed, setIsCollapsed] = useState(() => {
    const state = getCollapsedState();
    return state[groupId] ?? false;
  });

  // Check if current route matches any item in this group
  const [activeId, setActiveId] = useState<string | null>(null);

  useEffect(() => {
    const updateActive = () => {
      const hash = window.location.hash;
      const modelMatch = hash.match(/^#\/models\/(.+)$/);
      const sourceMatch = hash.match(/^#\/sources\/(.+)$/);
      const macroMatch = hash.match(/^#\/macros\/(.+)$/);
      
      if (modelMatch) {
        setActiveId(decodeURIComponent(modelMatch[1]));
      } else if (sourceMatch) {
        setActiveId(decodeURIComponent(sourceMatch[1]));
      } else if (macroMatch) {
        setActiveId(decodeURIComponent(macroMatch[1]));
      } else {
        setActiveId(null);
      }
    };

    updateActive();
    window.addEventListener('hashchange', updateActive);
    return () => window.removeEventListener('hashchange', updateActive);
  }, []);

  const toggle = () => {
    const newState = !isCollapsed;
    setIsCollapsed(newState);
    
    const state = getCollapsedState();
    state[groupId] = newState;
    saveCollapsedState(state);
  };

  const handleItemClick = (item: NavItem) => {
    if (item.type === 'model') {
      navigateToModel(item.id);
    } else if (item.type === 'source') {
      navigateToSource(item.id);
    } else {
      navigateToMacro(item.id);
    }
  };

  return (
    <div class="nav-group">
      <div class="nav-group-header" onClick={toggle}>
        <span class="nav-group-toggle">{isCollapsed ? '+' : '-'}</span>
        <span class="nav-group-title">{title}</span>
        <span class="nav-group-count">{items.length}</span>
      </div>
      
      {!isCollapsed && (
        <div class="nav-group-items">
          {items.map((item) => (
            <a
              key={item.id}
              class={`${item.type}-item${activeId === item.id ? ' active' : ''}`}
              onClick={() => handleItemClick(item)}
            >
              {item.name}
              {item.badge && (
                <span class={`model-badge ${item.badge}`}>{item.badge}</span>
              )}
            </a>
          ))}
        </div>
      )}
    </div>
  );
};
