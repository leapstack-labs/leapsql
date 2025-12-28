// Layout component with sidebar and main content area
import type { FunctionComponent, ComponentChildren } from 'preact';
import { Sidebar } from './Sidebar';
import { ThemeToggle } from './ThemeToggle';

interface LayoutProps {
  children: ComponentChildren;
  dbReady: boolean;
}

export const Layout: FunctionComponent<LayoutProps> = ({ children, dbReady }) => {
  return (
    <div class="layout">
      <Sidebar dbReady={dbReady} />
      <main class="main">
        <div class="main-header">
          <ThemeToggle />
        </div>
        {children}
      </main>
    </div>
  );
};
