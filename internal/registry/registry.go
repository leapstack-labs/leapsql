// Package registry provides model registration and table name resolution.
// It maps table names referenced in SQL to their corresponding model paths,
// enabling automatic dependency detection without explicit @import pragmas.
package registry

import (
	"strings"
	"sync"

	"github.com/leapstack-labs/leapsql/internal/loader"
)

// ModelRegistry maps table names to model paths for dependency resolution.
type ModelRegistry struct {
	mu sync.RWMutex

	// byPath maps model paths to their configs: "staging.stg_customers" → *ModelConfig
	byPath map[string]*loader.ModelConfig

	// byName maps unqualified model names to paths: "stg_customers" → "staging.stg_customers"
	// Note: if multiple models have the same name, the last registered wins
	byName map[string]string

	// byTable maps qualified table names to model paths
	// Supports multiple lookup formats:
	//   "staging.stg_customers" → "staging.stg_customers"
	//   "public.stg_customers" → "staging.stg_customers" (schema mapping)
	byTable map[string]string

	// externalSources tracks known external sources (raw tables)
	externalSources map[string]struct{}
}

// NewModelRegistry creates a new empty registry.
func NewModelRegistry() *ModelRegistry {
	return &ModelRegistry{
		byPath:          make(map[string]*loader.ModelConfig),
		byName:          make(map[string]string),
		byTable:         make(map[string]string),
		externalSources: make(map[string]struct{}),
	}
}

// Register adds a model to the registry.
// It registers the model under its path and various table name variants.
func (r *ModelRegistry) Register(model *loader.ModelConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Register by full path
	r.byPath[model.Path] = model

	// Register by model name (unqualified)
	r.byName[model.Name] = model.Path

	// Register by path as table name
	r.byTable[model.Path] = model.Path

	// If the path contains a dot (e.g., "staging.stg_customers"),
	// also register without the first component to support schema-qualified references
	if parts := strings.SplitN(model.Path, ".", 2); len(parts) == 2 {
		// Allow lookup by just the model name: "stg_customers"
		r.byTable[parts[1]] = model.Path

		// Also allow schema-qualified lookups with different schema prefixes
		// e.g., "public.stg_customers" → "staging.stg_customers"
		r.byTable[model.Name] = model.Path
	}
}

// RegisterExternalSource marks a table name as an external source (not a model).
func (r *ModelRegistry) RegisterExternalSource(tableName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.externalSources[tableName] = struct{}{}
}

// Resolve attempts to resolve a table name to a model path.
// Returns the model path and true if found, or empty string and false if not a model.
func (r *ModelRegistry) Resolve(tableName string) (modelPath string, isModel bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Try exact match on path
	if _, ok := r.byPath[tableName]; ok {
		return tableName, true
	}

	// 2. Try table name mapping
	if path, ok := r.byTable[tableName]; ok {
		return path, true
	}

	// 3. Try by unqualified name
	if path, ok := r.byName[tableName]; ok {
		return path, true
	}

	// 4. Handle qualified names: try extracting just the table name
	if parts := strings.Split(tableName, "."); len(parts) > 1 {
		justName := parts[len(parts)-1]

		// Try by name
		if path, ok := r.byName[justName]; ok {
			return path, true
		}

		// Try in table mapping
		if path, ok := r.byTable[justName]; ok {
			return path, true
		}
	}

	return "", false
}

// IsExternalSource returns true if the table name is known to be an external source.
func (r *ModelRegistry) IsExternalSource(tableName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.externalSources[tableName]
	return ok
}

// GetModel returns the model config for a given path.
func (r *ModelRegistry) GetModel(path string) (*loader.ModelConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	model, ok := r.byPath[path]
	return model, ok
}

// AllModels returns all registered models.
func (r *ModelRegistry) AllModels() []*loader.ModelConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	models := make([]*loader.ModelConfig, 0, len(r.byPath))
	for _, model := range r.byPath {
		models = append(models, model)
	}
	return models
}

// Count returns the number of registered models.
func (r *ModelRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byPath)
}

// ResolveDependencies resolves a list of table names to model dependencies and external sources.
// It separates the table names into:
//   - dependencies: tables that are known models (deduplicated by model path)
//   - externalSources: tables that are not models (raw/source tables, deduplicated)
func (r *ModelRegistry) ResolveDependencies(tableNames []string) (dependencies []string, externalSources []string) {
	seenDeps := make(map[string]struct{})
	seenExternal := make(map[string]struct{})

	for _, tableName := range tableNames {
		if modelPath, isModel := r.Resolve(tableName); isModel {
			// Deduplicate by resolved model path
			if _, ok := seenDeps[modelPath]; !ok {
				seenDeps[modelPath] = struct{}{}
				dependencies = append(dependencies, modelPath)
			}
		} else {
			// Deduplicate external sources by original table name
			if _, ok := seenExternal[tableName]; !ok {
				seenExternal[tableName] = struct{}{}
				externalSources = append(externalSources, tableName)
				// Mark as external source
				r.externalSources[tableName] = struct{}{}
			}
		}
	}

	return dependencies, externalSources
}

// GetExternalSources returns all known external sources (tables not mapped to models).
func (r *ModelRegistry) GetExternalSources() map[string]struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make(map[string]struct{}, len(r.externalSources))
	for k, v := range r.externalSources {
		result[k] = v
	}
	return result
}
