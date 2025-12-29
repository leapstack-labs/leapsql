// Main entry point for the documentation site with SQLite-over-HTTP
import { render } from 'preact';
import { useState, useEffect } from 'preact/hooks';
import { App } from './components/App';
import { ManifestContext, DatabaseContext } from './lib/context';
import { HttpAdapter } from './lib/db/http-adapter';
import { WasmAdapter } from './lib/db/wasm-adapter';
import type { DatabaseAdapter } from './lib/db/types';
import type { Manifest } from './lib/types';
import './css/main.css';

// Get manifest from embedded data (instant load)
function getManifest(): Manifest {
  if (window.__MANIFEST__) {
    return window.__MANIFEST__;
  }
  throw new Error('Manifest not found - please ensure __MANIFEST__ is embedded in the page');
}

// Check if running in dev mode
function isDevMode(): boolean {
  return window.__DEV_MODE__ === true || window.location.hostname === 'localhost';
}

// Root component with hybrid boot
function Root() {
  const [manifest] = useState<Manifest>(() => getManifest());
  const [db, setDb] = useState<DatabaseAdapter | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    // Initialize the appropriate database adapter
    const adapter = isDevMode() ? new HttpAdapter() : new WasmAdapter();

    adapter.init()
      .then(() => setDb(adapter))
      .catch(setError);
  }, []);

  if (error) {
    return (
      <div class="loading">
        <div>
          <h3>Failed to load database</h3>
          <p style={{ color: 'var(--muted-foreground)', marginTop: '0.5rem' }}>
            {error.message}
          </p>
        </div>
      </div>
    );
  }

  return (
    <ManifestContext.Provider value={manifest}>
      <DatabaseContext.Provider value={db}>
        <App dbReady={db !== null} />
      </DatabaseContext.Provider>
    </ManifestContext.Provider>
  );
}

// Initialize the app
function init() {
  const appRoot = document.getElementById('app');
  if (!appRoot) {
    console.error('App root element not found');
    return;
  }

  try {
    render(<Root />, appRoot);
  } catch (error) {
    appRoot.innerHTML = `
      <div class="loading">
        <div>
          <h3>Failed to load documentation</h3>
          <p style="color: var(--muted-foreground); margin-top: 0.5rem;">${error instanceof Error ? error.message : 'Unknown error'}</p>
        </div>
      </div>
    `;
  }
}

// Start the app when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
