// Main entry point for the documentation site
import { render } from 'preact';
import { App } from './components/App';
import './css/main.css';

// Load catalog data from embedded JSON
function loadCatalog() {
  const dataElement = document.getElementById('catalog-data');
  if (!dataElement) {
    throw new Error('Catalog data not found');
  }
  
  try {
    return JSON.parse(dataElement.textContent || '{}');
  } catch (e) {
    throw new Error('Failed to parse catalog data');
  }
}

// Initialize the app
function init() {
  const appRoot = document.getElementById('app');
  if (!appRoot) {
    console.error('App root element not found');
    return;
  }

  try {
    const catalog = loadCatalog();
    render(<App catalog={catalog} />, appRoot);
  } catch (error) {
    appRoot.innerHTML = `
      <div class="loading">
        <div>
          <h3>Failed to load documentation</h3>
          <p style="color: var(--text-secondary); margin-top: 0.5rem;">${error instanceof Error ? error.message : 'Unknown error'}</p>
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
