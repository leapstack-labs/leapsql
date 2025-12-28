// Layout component with sidebar and main content area
import type { FunctionComponent, ComponentChildren } from 'preact';
import { Sidebar } from './Sidebar';

interface LayoutProps {
  children: ComponentChildren;
  dbReady: boolean;
}

export const Layout: FunctionComponent<LayoutProps> = ({ children, dbReady }) => {
  return (
    <div class="layout">
      <Sidebar dbReady={dbReady} />
      <main class="main">{children}</main>
    </div>
  );
};
