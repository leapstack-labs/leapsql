// Layout component with sidebar and main content area
import type { FunctionComponent, ComponentChildren } from 'preact';
import { Sidebar } from './Sidebar';
import { ThemeToggle } from './ThemeToggle';
import { ThemeSwitcher } from './ThemeSwitcher';

declare global {
  interface Window {
    __DEV_MODE__?: boolean;
  }
}

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
          {window.__DEV_MODE__ && <ThemeSwitcher />}
          <ThemeToggle />
        </div>
        {children}
      </main>
    </div>
  );
};
