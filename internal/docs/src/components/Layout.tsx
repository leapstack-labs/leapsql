// Layout component with sidebar and main content area
import type { FunctionComponent, ComponentChildren } from 'preact';
import { Sidebar } from './Sidebar';

interface LayoutProps {
  children: ComponentChildren;
}

export const Layout: FunctionComponent<LayoutProps> = ({ children }) => {
  return (
    <div class="layout">
      <Sidebar />
      <main class="main">{children}</main>
    </div>
  );
};
