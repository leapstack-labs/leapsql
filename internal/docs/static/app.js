// DBGo Docs - Static Documentation Site
// Vanilla JS with hash-based routing

// Global state
let CATALOG = null;
let MODELS_BY_PATH = {};

// Initialize the app
async function init() {
  try {
    // Load catalog data
    const response = await fetch('./data/catalog.json');
    if (!response.ok) {
      throw new Error(`Failed to load catalog: ${response.status}`);
    }
    CATALOG = await response.json();
    
    // Build lookup map
    CATALOG.models.forEach(model => {
      MODELS_BY_PATH[model.path] = model;
    });
    
    // Initial render
    router();
    
    // Listen for hash changes
    window.addEventListener('hashchange', router);
  } catch (error) {
    console.error('Failed to initialize:', error);
    document.getElementById('app').innerHTML = `
      <div class="loading">
        <div>
          <h3>Failed to load documentation</h3>
          <p style="color: var(--text-secondary); margin-top: 0.5rem;">${error.message}</p>
        </div>
      </div>
    `;
  }
}

// Router
function router() {
  const hash = window.location.hash.slice(1) || '/';
  const app = document.getElementById('app');
  
  // Parse route
  if (hash === '/' || hash === '') {
    app.innerHTML = renderLayout(renderHomePage());
  } else if (hash === '/lineage') {
    app.innerHTML = renderLayout(renderLineagePage());
    // Initialize DAG after DOM is ready
    setTimeout(() => initDAG(), 0);
  } else if (hash.startsWith('/models/')) {
    const modelPath = decodeURIComponent(hash.slice(8));
    app.innerHTML = renderLayout(renderModelPage(modelPath));
    // Highlight SQL and init column lineage after DOM is ready
    setTimeout(() => {
      highlightCode();
      initColumnLineage(modelPath);
    }, 0);
  } else {
    app.innerHTML = renderLayout(renderNotFound());
  }
  
  // Update active nav link
  updateActiveNav();
}

// Update active navigation link
function updateActiveNav() {
  document.querySelectorAll('.nav-link, .model-item').forEach(link => {
    const href = link.getAttribute('href');
    if (href === window.location.hash || (href === '#/' && !window.location.hash)) {
      link.classList.add('active');
    } else {
      link.classList.remove('active');
    }
  });
}

// Layout wrapper
function renderLayout(content) {
  return `
    <div class="layout">
      ${renderSidebar()}
      <main class="main">
        ${content}
      </main>
    </div>
  `;
}

// Sidebar
function renderSidebar() {
  const modelsByFolder = groupModelsByFolder(CATALOG.models);
  
  return `
    <aside class="sidebar">
      <div class="sidebar-header">
        <h1>${CATALOG.project_name || 'DBGo'}</h1>
        <div class="subtitle">Documentation</div>
      </div>
      
      <div class="search-container">
        <input 
          type="text" 
          class="search-input" 
          placeholder="Search models..." 
          onkeyup="handleSearch(this.value)"
        >
      </div>
      
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
          <div class="nav-section-title">Models</div>
          <ul class="model-list" id="model-list">
            ${renderModelList(modelsByFolder)}
          </ul>
        </div>
      </nav>
    </aside>
  `;
}

// Group models by folder
function groupModelsByFolder(models) {
  const groups = {};
  models.forEach(model => {
    const parts = model.path.split('.');
    const folder = parts.length > 1 ? parts[0] : 'default';
    if (!groups[folder]) {
      groups[folder] = [];
    }
    groups[folder].push(model);
  });
  return groups;
}

// Render model list
function renderModelList(modelsByFolder) {
  let html = '';
  Object.entries(modelsByFolder).sort().forEach(([folder, models]) => {
    html += `<li class="nav-section-title" style="padding: 0.5rem 1.5rem; margin-top: 0.5rem;">${folder}</li>`;
    models.sort((a, b) => a.name.localeCompare(b.name)).forEach(model => {
      html += `
        <li>
          <a href="#/models/${encodeURIComponent(model.path)}" class="model-item" data-model="${model.path}">
            ${model.name}
            <span class="model-badge ${model.materialized}">${model.materialized}</span>
          </a>
        </li>
      `;
    });
  });
  return html;
}

// Search handler
function handleSearch(query) {
  const modelList = document.getElementById('model-list');
  if (!modelList) return;
  
  const normalizedQuery = query.toLowerCase().trim();
  
  if (!normalizedQuery) {
    // Reset to full list
    const modelsByFolder = groupModelsByFolder(CATALOG.models);
    modelList.innerHTML = renderModelList(modelsByFolder);
    return;
  }
  
  // Filter models
  const filtered = CATALOG.models.filter(model => 
    model.name.toLowerCase().includes(normalizedQuery) ||
    model.path.toLowerCase().includes(normalizedQuery) ||
    (model.description && model.description.toLowerCase().includes(normalizedQuery))
  );
  
  const modelsByFolder = groupModelsByFolder(filtered);
  modelList.innerHTML = renderModelList(modelsByFolder);
}

// Home page
function renderHomePage() {
  const totalModels = CATALOG.models.length;
  const materializations = {};
  const folders = new Set();
  
  CATALOG.models.forEach(model => {
    materializations[model.materialized] = (materializations[model.materialized] || 0) + 1;
    const folder = model.path.split('.')[0];
    folders.add(folder);
  });
  
  return `
    <div class="page-header">
      <h1>Overview</h1>
      <p class="description">Documentation for ${CATALOG.project_name || 'your data models'}</p>
    </div>
    
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-value">${totalModels}</div>
        <div class="stat-label">Total Models</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${folders.size}</div>
        <div class="stat-label">Folders</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${materializations.table || 0}</div>
        <div class="stat-label">Tables</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${materializations.view || 0}</div>
        <div class="stat-label">Views</div>
      </div>
    </div>
    
    <div class="section">
      <h2 class="section-title">Recent Models</h2>
      <table class="data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Path</th>
            <th>Type</th>
            <th>Dependencies</th>
          </tr>
        </thead>
        <tbody>
          ${CATALOG.models.slice(0, 10).map(model => `
            <tr onclick="navigateTo('/models/${encodeURIComponent(model.path)}')" style="cursor: pointer;">
              <td><strong>${model.name}</strong></td>
              <td><code>${model.path}</code></td>
              <td><span class="model-badge ${model.materialized}">${model.materialized}</span></td>
              <td>${(model.dependencies || []).length}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    
    <div class="section">
      <h2 class="section-title">Generated</h2>
      <p style="color: var(--text-secondary);">
        ${new Date(CATALOG.generated_at).toLocaleString()}
      </p>
    </div>
  `;
}

// Model detail page
function renderModelPage(modelPath) {
  const model = MODELS_BY_PATH[modelPath];
  
  if (!model) {
    return renderNotFound(`Model "${modelPath}" not found`);
  }
  
  // Find external sources (sources that aren't models)
  const externalSources = model.sources.filter(src => !MODELS_BY_PATH[src] && !CATALOG.models.some(m => m.name === src));
  
  return `
    <div class="model-header">
      <div>
        <h1 class="model-title">${model.name}</h1>
        <div class="model-path">${model.path}</div>
        ${model.description ? `<p style="margin-top: 1rem; color: var(--text-secondary);">${model.description}</p>` : ''}
        <div class="model-meta">
          <div class="meta-item">
            <span class="label">Type:</span>
            <span class="model-badge ${model.materialized}">${model.materialized}</span>
          </div>
          ${model.unique_key ? `
            <div class="meta-item">
              <span class="label">Unique Key:</span>
              <code>${model.unique_key}</code>
            </div>
          ` : ''}
        </div>
      </div>
    </div>
    
    ${model.dependencies.length > 0 ? `
      <div class="section">
        <h2 class="section-title">Dependencies (${model.dependencies.length})</h2>
        <div class="dep-list">
          ${model.dependencies.map(dep => `
            <a href="#/models/${encodeURIComponent(dep)}" class="dep-tag">${dep}</a>
          `).join('')}
        </div>
      </div>
    ` : ''}
    
    ${model.dependents.length > 0 ? `
      <div class="section">
        <h2 class="section-title">Dependents (${model.dependents.length})</h2>
        <div class="dep-list">
          ${model.dependents.map(dep => `
            <a href="#/models/${encodeURIComponent(dep)}" class="dep-tag">${dep}</a>
          `).join('')}
        </div>
      </div>
    ` : ''}
    
    ${externalSources.length > 0 ? `
      <div class="section">
        <h2 class="section-title">External Sources (${externalSources.length})</h2>
        <div class="dep-list">
          ${externalSources.map(src => `
            <span class="dep-tag external">${src}</span>
          `).join('')}
        </div>
      </div>
    ` : ''}
    
    ${model.columns && model.columns.length > 0 ? `
      <div class="section">
        <h2 class="section-title">Columns (${model.columns.length})</h2>
        <table class="data-table columns-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Transform</th>
              <th>Sources</th>
            </tr>
          </thead>
          <tbody>
            ${model.columns.map(col => `
              <tr>
                <td><code class="column-name">${escapeHtml(col.name)}</code></td>
                <td>
                  ${col.transform_type === 'EXPR' ? `
                    <span class="transform-badge expr">${col.function || 'expression'}</span>
                  ` : `
                    <span class="transform-badge direct">direct</span>
                  `}
                </td>
                <td>
                  ${col.sources && col.sources.length > 0 ? `
                    <div class="source-list">
                      ${col.sources.map(src => `
                        <span class="source-ref" title="${src.table}.${src.column}">
                          ${src.table ? `<span class="source-table">${src.table}</span>.` : ''}<span class="source-column">${src.column}</span>
                        </span>
                      `).join('')}
                    </div>
                  ` : '<span class="no-sources">-</span>'}
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    ` : ''}
    
    <div class="section">
      <h2 class="section-title">Column Lineage</h2>
      <div class="column-lineage-container" id="column-lineage-container">
        <svg id="column-lineage-svg"></svg>
      </div>
      <div class="column-lineage-legend">
        <div class="legend-item">
          <div class="legend-color" style="background: var(--accent-green);"></div>
          <span>Current model</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background: var(--accent-blue);"></div>
          <span>Source models</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background: var(--accent-orange);"></div>
          <span>External sources</span>
        </div>
      </div>
    </div>
    
    <div class="section">
      <h2 class="section-title">SQL</h2>
      <div class="code-block">
        <div class="code-header">
          <span class="code-title">${model.file_path || model.path + '.sql'}</span>
        </div>
        <div class="code-content">
          <pre><code class="language-sql">${escapeHtml(model.sql)}</code></pre>
        </div>
      </div>
    </div>
  `;
}

// Lineage page
function renderLineagePage() {
  return `
    <div class="page-header">
      <h1>Lineage</h1>
      <p class="description">Data flow and dependencies between models</p>
    </div>
    
    <div class="section">
      <div class="dag-container" id="dag-container">
        <svg id="dag-svg"></svg>
      </div>
    </div>
    
    <div class="section">
      <h2 class="section-title">Legend</h2>
      <div style="display: flex; gap: 2rem; color: var(--text-secondary); font-size: 0.875rem;">
        <div style="display: flex; align-items: center; gap: 0.5rem;">
          <div style="width: 12px; height: 12px; border-radius: 50%; background: var(--node-staging);"></div>
          staging
        </div>
        <div style="display: flex; align-items: center; gap: 0.5rem;">
          <div style="width: 12px; height: 12px; border-radius: 50%; background: var(--node-marts);"></div>
          marts
        </div>
        <div style="display: flex; align-items: center; gap: 0.5rem;">
          <div style="width: 12px; height: 12px; border-radius: 50%; background: var(--node-default);"></div>
          other
        </div>
      </div>
    </div>
  `;
}

// Initialize DAG visualization
function initDAG() {
  const container = document.getElementById('dag-container');
  const svg = d3.select('#dag-svg');
  
  if (!container || !svg.node()) return;
  
  const width = container.clientWidth;
  const height = container.clientHeight;
  
  svg.attr('width', width).attr('height', height);
  
  // Clear existing
  svg.selectAll('*').remove();
  
  // Create nodes and links data
  const nodes = CATALOG.lineage.nodes.map(path => {
    const model = MODELS_BY_PATH[path];
    return {
      id: path,
      name: model ? model.name : path,
      folder: path.split('.')[0]
    };
  });
  
  const links = CATALOG.lineage.edges.map(edge => ({
    source: edge.source,
    target: edge.target
  }));
  
  // Create a group for zoom/pan
  const g = svg.append('g');
  
  // Add zoom behavior
  const zoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on('zoom', (event) => {
      g.attr('transform', event.transform);
    });
  
  svg.call(zoom);
  
  // Create arrow marker
  svg.append('defs').append('marker')
    .attr('id', 'arrowhead')
    .attr('viewBox', '-0 -5 10 10')
    .attr('refX', 20)
    .attr('refY', 0)
    .attr('orient', 'auto')
    .attr('markerWidth', 6)
    .attr('markerHeight', 6)
    .append('path')
    .attr('d', 'M 0,-5 L 10,0 L 0,5')
    .attr('fill', '#30363d');
  
  // Create force simulation
  const simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(120))
    .force('charge', d3.forceManyBody().strength(-400))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(50));
  
  // Create links
  const link = g.append('g')
    .selectAll('line')
    .data(links)
    .join('line')
    .attr('class', 'dag-link')
    .attr('marker-end', 'url(#arrowhead)');
  
  // Create nodes
  const node = g.append('g')
    .selectAll('g')
    .data(nodes)
    .join('g')
    .attr('class', 'dag-node')
    .call(d3.drag()
      .on('start', dragstarted)
      .on('drag', dragged)
      .on('end', dragended))
    .on('click', (event, d) => {
      navigateTo(`/models/${encodeURIComponent(d.id)}`);
    });
  
  // Node circles
  node.append('circle')
    .attr('r', 12)
    .attr('fill', d => getNodeColor(d.folder))
    .attr('stroke', d => getNodeColor(d.folder));
  
  // Node labels
  node.append('text')
    .text(d => d.name)
    .attr('x', 18)
    .attr('y', 4)
    .attr('fill', '#e6edf3');
  
  // Update positions on tick
  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);
    
    node.attr('transform', d => `translate(${d.x},${d.y})`);
  });
  
  // Drag functions
  function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }
  
  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }
  
  function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }
  
  // Fit to view after simulation settles
  simulation.on('end', () => {
    fitToView();
  });
  
  function fitToView() {
    const bounds = g.node().getBBox();
    const fullWidth = width;
    const fullHeight = height;
    const midX = bounds.x + bounds.width / 2;
    const midY = bounds.y + bounds.height / 2;
    const scale = 0.8 / Math.max(bounds.width / fullWidth, bounds.height / fullHeight);
    const translate = [fullWidth / 2 - scale * midX, fullHeight / 2 - scale * midY];
    
    svg.transition()
      .duration(750)
      .call(zoom.transform, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale));
  }
}

// Get node color based on folder
function getNodeColor(folder) {
  const colors = {
    'staging': '#3fb950',
    'marts': '#58a6ff',
    'intermediate': '#a371f7',
    'seeds': '#d29922'
  };
  return colors[folder] || '#8b949e';
}

// Initialize column lineage visualization for a specific model
function initColumnLineage(modelPath) {
  const container = document.getElementById('column-lineage-container');
  const svg = d3.select('#column-lineage-svg');
  
  if (!container || !svg.node() || !CATALOG.column_lineage) return;
  
  const model = MODELS_BY_PATH[modelPath];
  if (!model || !model.columns || model.columns.length === 0) {
    container.innerHTML = '<div class="empty-state"><p>No column lineage data available for this model.</p></div>';
    return;
  }
  
  const width = container.clientWidth;
  const height = 400;
  
  svg.attr('width', width).attr('height', height);
  
  // Clear existing
  svg.selectAll('*').remove();
  
  // Filter nodes and edges relevant to this model
  const relevantNodeIds = new Set();
  const relevantEdges = [];
  
  // Add this model's columns
  model.columns.forEach(col => {
    const nodeId = modelPath + '.' + col.name;
    relevantNodeIds.add(nodeId);
    
    // Find edges pointing to this column
    CATALOG.column_lineage.edges.forEach(edge => {
      if (edge.target === nodeId) {
        relevantEdges.push(edge);
        relevantNodeIds.add(edge.source);
      }
    });
  });
  
  // If no lineage data, show message
  if (relevantEdges.length === 0 && model.columns.length > 0) {
    // Still show columns even without lineage edges
    model.columns.forEach(col => {
      col.sources.forEach(src => {
        const sourceNodeId = src.table + '.' + src.column;
        relevantNodeIds.add(sourceNodeId);
        relevantEdges.push({
          source: sourceNodeId,
          target: modelPath + '.' + col.name
        });
      });
    });
  }
  
  if (relevantNodeIds.size === 0) {
    container.innerHTML = '<div class="empty-state"><p>No column lineage data available for this model.</p></div>';
    return;
  }
  
  // Build nodes data
  const nodes = Array.from(relevantNodeIds).map(nodeId => {
    const parts = nodeId.split('.');
    const columnName = parts.pop();
    const modelName = parts.join('.');
    const isCurrentModel = modelName === modelPath;
    const isModelSource = MODELS_BY_PATH[modelName] !== undefined;
    
    return {
      id: nodeId,
      model: modelName,
      column: columnName,
      isCurrentModel,
      isModelSource
    };
  });
  
  // Build edges data
  const edges = relevantEdges.map(edge => ({
    source: edge.source,
    target: edge.target
  }));
  
  // Create a group for zoom/pan
  const g = svg.append('g');
  
  // Add zoom behavior
  const zoom = d3.zoom()
    .scaleExtent([0.3, 3])
    .on('zoom', (event) => {
      g.attr('transform', event.transform);
    });
  
  svg.call(zoom);
  
  // Create arrow marker
  svg.append('defs').append('marker')
    .attr('id', 'col-arrowhead')
    .attr('viewBox', '-0 -5 10 10')
    .attr('refX', 8)
    .attr('refY', 0)
    .attr('orient', 'auto')
    .attr('markerWidth', 6)
    .attr('markerHeight', 6)
    .append('path')
    .attr('d', 'M 0,-5 L 10,0 L 0,5')
    .attr('fill', '#30363d');
  
  // Create force simulation
  const simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(edges).id(d => d.id).distance(100))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(60))
    .force('x', d3.forceX(d => d.isCurrentModel ? width * 0.7 : width * 0.3).strength(0.1))
    .force('y', d3.forceY(height / 2).strength(0.05));
  
  // Create links
  const link = g.append('g')
    .selectAll('line')
    .data(edges)
    .join('line')
    .attr('class', 'column-lineage-link')
    .attr('marker-end', 'url(#col-arrowhead)');
  
  // Create nodes
  const node = g.append('g')
    .selectAll('g')
    .data(nodes)
    .join('g')
    .attr('class', 'column-lineage-node')
    .call(d3.drag()
      .on('start', dragstarted)
      .on('drag', dragged)
      .on('end', dragended));
  
  // Node rectangles (column boxes)
  node.append('rect')
    .attr('rx', 4)
    .attr('ry', 4)
    .attr('width', d => Math.max(80, d.column.length * 8 + 16))
    .attr('height', 32)
    .attr('x', d => -Math.max(80, d.column.length * 8 + 16) / 2)
    .attr('y', -16)
    .attr('fill', d => {
      if (d.isCurrentModel) return 'rgba(63, 185, 80, 0.2)';
      if (d.isModelSource) return 'rgba(88, 166, 255, 0.2)';
      return 'rgba(210, 153, 34, 0.2)';
    })
    .attr('stroke', d => {
      if (d.isCurrentModel) return '#3fb950';
      if (d.isModelSource) return '#58a6ff';
      return '#d29922';
    })
    .attr('stroke-width', 1.5);
  
  // Node column labels
  node.append('text')
    .text(d => d.column)
    .attr('text-anchor', 'middle')
    .attr('y', 4)
    .attr('fill', '#e6edf3')
    .attr('font-size', '11px')
    .attr('font-family', 'SF Mono, Consolas, monospace');
  
  // Node model labels (on hover tooltip)
  node.append('title')
    .text(d => `${d.model}.${d.column}`);
  
  // Model name labels above nodes (for source nodes)
  node.filter(d => !d.isCurrentModel)
    .append('text')
    .text(d => d.model)
    .attr('text-anchor', 'middle')
    .attr('y', -24)
    .attr('fill', '#8b949e')
    .attr('font-size', '9px');
  
  // Update positions on tick
  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);
    
    node.attr('transform', d => `translate(${d.x},${d.y})`);
  });
  
  // Drag functions
  function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }
  
  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }
  
  function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }
  
  // Fit to view after simulation settles
  simulation.on('end', () => {
    fitColumnLineageToView();
  });
  
  function fitColumnLineageToView() {
    const bounds = g.node().getBBox();
    if (bounds.width === 0 || bounds.height === 0) return;
    
    const fullWidth = width;
    const fullHeight = height;
    const midX = bounds.x + bounds.width / 2;
    const midY = bounds.y + bounds.height / 2;
    const scale = 0.8 / Math.max(bounds.width / fullWidth, bounds.height / fullHeight);
    const clampedScale = Math.min(Math.max(scale, 0.5), 1.5);
    const translate = [fullWidth / 2 - clampedScale * midX, fullHeight / 2 - clampedScale * midY];
    
    svg.transition()
      .duration(750)
      .call(zoom.transform, d3.zoomIdentity.translate(translate[0], translate[1]).scale(clampedScale));
  }
}

// Not found page
function renderNotFound(message) {
  return `
    <div class="empty-state">
      <h3>Not Found</h3>
      <p>${message || 'The page you are looking for does not exist.'}</p>
      <a href="#/" style="color: var(--link-color); margin-top: 1rem; display: inline-block;">Go to Overview</a>
    </div>
  `;
}

// Navigation helper
function navigateTo(path) {
  window.location.hash = path;
}

// Escape HTML
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Highlight code
function highlightCode() {
  if (typeof hljs !== 'undefined') {
    document.querySelectorAll('pre code').forEach(block => {
      hljs.highlightElement(block);
    });
  }
}

// Start the app
document.addEventListener('DOMContentLoaded', init);
