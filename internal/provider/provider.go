// Package provider manages shared context for all lint consumers.
// It caches parsed documents, project context, and the dependency graph
// to avoid redundant parsing and database queries.
package provider

import (
	"log/slog"
	"sync"
	"time"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

// Provider manages shared context for all lint consumers.
// It caches parsed documents, project context, and the dependency graph.
type Provider struct {
	// Document cache (keyed by URI)
	documents   map[string]*ParsedDocument
	documentsMu sync.RWMutex

	// Project context cache
	projectCtx     *project.Context
	projectCtxHash string
	projectCtxMu   sync.RWMutex

	// Dependencies
	store   core.Store
	config  lint.ProjectHealthConfig
	dialect *core.Dialect
	logger  *slog.Logger
}

// New creates a new Provider with the given dependencies.
func New(store core.Store, d *core.Dialect, config lint.ProjectHealthConfig, logger *slog.Logger) *Provider {
	if logger == nil {
		logger = slog.Default()
	}
	return &Provider{
		documents: make(map[string]*ParsedDocument),
		store:     store,
		dialect:   d,
		config:    config,
		logger:    logger,
	}
}

// GetOrParse returns a cached ParsedDocument or parses the content if needed.
// Thread-safe for concurrent access.
func (p *Provider) GetOrParse(uri string, content string, version int) *ParsedDocument {
	p.documentsMu.RLock()
	doc, exists := p.documents[uri]
	if exists && doc.Version >= version {
		p.documentsMu.RUnlock()
		return doc
	}
	p.documentsMu.RUnlock()

	// Need to parse
	p.documentsMu.Lock()
	defer p.documentsMu.Unlock()

	// Double-check after acquiring write lock
	doc, exists = p.documents[uri]
	if exists && doc.Version >= version {
		return doc
	}

	// Parse the document
	doc = Parse(content, uri, version, p.dialect)
	p.documents[uri] = doc

	return doc
}

// Get returns a cached ParsedDocument without parsing.
// Returns nil if not cached.
func (p *Provider) Get(uri string) *ParsedDocument {
	p.documentsMu.RLock()
	defer p.documentsMu.RUnlock()
	return p.documents[uri]
}

// Invalidate removes a document from the cache.
func (p *Provider) Invalidate(uri string) {
	p.documentsMu.Lock()
	defer p.documentsMu.Unlock()
	delete(p.documents, uri)
}

// InvalidateAll clears the entire document cache.
func (p *Provider) InvalidateAll() {
	p.documentsMu.Lock()
	defer p.documentsMu.Unlock()
	p.documents = make(map[string]*ParsedDocument)
}

// GetProjectContext returns the cached project context or builds a new one.
// The context is rebuilt if the underlying data has changed (detected via hash).
func (p *Provider) GetProjectContext() *project.Context {
	if p.store == nil {
		return nil
	}

	p.projectCtxMu.RLock()
	if p.projectCtx != nil {
		p.projectCtxMu.RUnlock()
		return p.projectCtx
	}
	p.projectCtxMu.RUnlock()

	// Need to build context
	p.projectCtxMu.Lock()
	defer p.projectCtxMu.Unlock()

	// Double-check after acquiring write lock
	if p.projectCtx != nil {
		return p.projectCtx
	}

	p.projectCtx = p.buildProjectContext()
	return p.projectCtx
}

// InvalidateProjectContext forces a rebuild of the project context on next access.
func (p *Provider) InvalidateProjectContext() {
	p.projectCtxMu.Lock()
	defer p.projectCtxMu.Unlock()
	p.projectCtx = nil
	p.projectCtxHash = ""
}

// buildProjectContext constructs the project context from the store.
// Uses batch queries for performance (~5 queries instead of ~500 for 100 models).
func (p *Provider) buildProjectContext() *project.Context {
	if p.store == nil {
		return nil
	}

	startTime := time.Now()

	// Get all models in one query
	storeModels, err := p.store.ListModels()
	if err != nil || len(storeModels) == 0 {
		return nil
	}

	// Build model ID to path mapping for dependency resolution
	modelIDToPath := make(map[string]string, len(storeModels))
	for _, m := range storeModels {
		modelIDToPath[m.ID] = m.Path
	}

	// Batch fetch all columns (1 query instead of N)
	allColumns, err := p.store.BatchGetAllColumns()
	if err != nil {
		p.logger.Warn("Failed to batch get columns, falling back to per-model queries", "error", err)
		allColumns = nil
	}

	// Batch fetch all dependencies (1 query instead of N)
	allDeps, err := p.store.BatchGetAllDependencies()
	if err != nil {
		p.logger.Warn("Failed to batch get dependencies", "error", err)
		allDeps = make(map[string][]string)
	}

	// Batch fetch all dependents (1 query instead of N)
	allDependents, err := p.store.BatchGetAllDependents()
	if err != nil {
		p.logger.Warn("Failed to batch get dependents", "error", err)
		allDependents = make(map[string][]string)
	}

	// Build the context maps
	models := make(map[string]*project.ModelInfo, len(storeModels))
	parents := make(map[string][]string)
	children := make(map[string][]string)

	// Process each model
	for _, m := range storeModels {
		// Get columns from batch result or fallback to individual query
		var columns []core.ColumnInfo
		if allColumns != nil {
			columns = allColumns[m.Path]
		} else {
			// Fallback to individual query
			columns, _ = p.store.GetModelColumns(m.Path)
		}

		// Get dependencies from batch result (convert IDs to paths)
		deps := allDeps[m.ID]
		var parentPaths []string
		for _, depID := range deps {
			if path, ok := modelIDToPath[depID]; ok {
				parentPaths = append(parentPaths, path)
			}
		}

		// Get dependents from batch result (convert IDs to paths)
		dependents := allDependents[m.ID]
		var childPaths []string
		for _, depID := range dependents {
			if path, ok := modelIDToPath[depID]; ok {
				childPaths = append(childPaths, path)
			}
		}

		models[m.Path] = &project.ModelInfo{
			Path:           m.Path,
			Name:           m.Name,
			FilePath:       m.FilePath,
			Columns:        columns,
			Materialized:   m.Materialized,
			Tags:           m.Tags,
			Meta:           m.Meta,
			UsesSelectStar: m.UsesSelectStar,
		}
		parents[m.Path] = parentPaths
		children[m.Path] = childPaths
	}

	// Infer model types
	project.InferAndSetTypes(models)

	p.logger.Debug("Built project context",
		"models", len(models),
		"duration", time.Since(startTime))

	// Use NewContextWithStore to enable schema drift detection (PL05)
	return project.NewContextWithStore(models, parents, children, p.config, p.store)
}

// Store returns the underlying state store.
func (p *Provider) Store() core.Store {
	return p.store
}

// Dialect returns the SQL dialect.
func (p *Provider) Dialect() *core.Dialect {
	return p.dialect
}

// SetDialect updates the dialect and invalidates all cached documents.
func (p *Provider) SetDialect(d *core.Dialect) {
	p.dialect = d
	p.InvalidateAll()
}

// Config returns the project health configuration.
func (p *Provider) Config() lint.ProjectHealthConfig {
	return p.config
}

// SetConfig updates the project health configuration.
func (p *Provider) SetConfig(config lint.ProjectHealthConfig) {
	p.config = config
	p.InvalidateProjectContext()
}
