// Hash-based router for the documentation site
import { useState, useEffect, useCallback } from 'preact/hooks';
import type { Route } from './types';

// Parse the current hash into a Route object
export function parseHash(hash: string): Route {
  const path = hash.slice(1) || '/';
  
  if (path === '/' || path === '') {
    return { type: 'overview' };
  }
  
  if (path === '/lineage') {
    return { type: 'lineage' };
  }
  
  if (path.startsWith('/models/')) {
    const modelPath = decodeURIComponent(path.slice(8));
    return { type: 'model', path: modelPath };
  }
  
  if (path.startsWith('/sources/')) {
    const sourceName = decodeURIComponent(path.slice(9));
    return { type: 'source', name: sourceName };
  }
  
  return { type: 'not-found' };
}

// Hook to get and update the current route
export function useRoute(): Route {
  const [route, setRoute] = useState<Route>(() => parseHash(window.location.hash));
  
  useEffect(() => {
    const handleHashChange = () => {
      setRoute(parseHash(window.location.hash));
    };
    
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);
  
  return route;
}

// Navigation helpers
export function navigateTo(path: string): void {
  window.location.hash = path;
}

export function navigateToModel(modelPath: string): void {
  navigateTo(`/models/${encodeURIComponent(modelPath)}`);
}

export function navigateToSource(sourceName: string): void {
  navigateTo(`/sources/${encodeURIComponent(sourceName)}`);
}

export function navigateToLineage(): void {
  navigateTo('/lineage');
}

export function navigateToOverview(): void {
  navigateTo('/');
}

// Hook that returns a navigation function
export function useNavigate() {
  return useCallback((path: string) => {
    navigateTo(path);
  }, []);
}
