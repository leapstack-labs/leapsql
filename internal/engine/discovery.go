// Package engine provides the SQL model execution engine.
// discovery.go contains the unified discovery system for macros and models.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/macro"
	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/internal/registry"
	"github.com/leapstack-labs/leapsql/internal/state"
)

// DiscoveryOptions configures the discovery process.
type DiscoveryOptions struct {
	ForceFullRefresh bool   // Ignore content hashes, re-parse everything
	ModelsDir        string // Override default models directory
	MacrosDir        string // Override default macros directory
	SeedsDir         string // Override default seeds directory
}

// DiscoveryResult contains statistics about the discovery run.
type DiscoveryResult struct {
	// Models
	ModelsTotal   int
	ModelsChanged int
	ModelsSkipped int
	ModelsDeleted int

	// Macros
	MacrosTotal   int
	MacrosChanged int
	MacrosSkipped int
	MacrosDeleted int

	// Seeds
	SeedsValidated int
	SeedsMissing   []string

	// Errors (non-fatal)
	Errors []DiscoveryError

	// Timing
	Duration time.Duration
}

// DiscoveryError represents a non-fatal error during discovery.
type DiscoveryError struct {
	Path    string
	Type    string // "parse", "validation", "hash", "save"
	Message string
}

// HasErrors returns true if any errors occurred.
func (r *DiscoveryResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// Summary returns a human-readable summary.
func (r *DiscoveryResult) Summary() string {
	return fmt.Sprintf(
		"Models: %d total (%d changed, %d skipped, %d deleted) | "+
			"Macros: %d total (%d changed, %d skipped, %d deleted) | "+
			"Duration: %s",
		r.ModelsTotal, r.ModelsChanged, r.ModelsSkipped, r.ModelsDeleted,
		r.MacrosTotal, r.MacrosChanged, r.MacrosSkipped, r.MacrosDeleted,
		r.Duration.Round(time.Millisecond),
	)
}

// Discover performs unified discovery of macros and models.
// This is the single source of truth for project state.
func (e *Engine) Discover(opts DiscoveryOptions) (*DiscoveryResult, error) {
	start := time.Now()
	result := &DiscoveryResult{}

	e.logger.Info("starting discovery")

	// 1. Discover macros first (models may reference them in templates)
	if err := e.discoverMacros(opts, result); err != nil {
		return result, fmt.Errorf("macro discovery failed: %w", err)
	}

	// 2. Discover models
	if err := e.discoverModels(opts, result); err != nil {
		return result, fmt.Errorf("model discovery failed: %w", err)
	}

	// 3. Validate seed references (check file existence, don't load data)
	e.validateSeeds(opts, result)

	// 4. Build dependency graph from scratch
	if err := e.buildGraph(); err != nil {
		return result, fmt.Errorf("graph construction failed: %w", err)
	}

	// 5. Persist dependencies to SQLite
	if err := e.persistDependencies(); err != nil {
		return result, fmt.Errorf("dependency persistence failed: %w", err)
	}

	result.Duration = time.Since(start)

	e.logger.Info("discovery completed",
		"models_total", result.ModelsTotal,
		"models_changed", result.ModelsChanged,
		"models_skipped", result.ModelsSkipped,
		"macros_total", result.MacrosTotal,
		"duration_ms", result.Duration.Milliseconds())

	return result, nil
}

// shouldParseFile checks if a file needs re-parsing based on content hash.
func (e *Engine) shouldParseFile(filePath string, forceRefresh bool) (needsParse bool, newHash string, content []byte) {
	content, err := os.ReadFile(filePath) //nolint:gosec // G304: filePath is validated by filepath.Walk
	if err != nil {
		return true, "", nil // File error, try to parse anyway
	}
	newHash = computeHash(string(content))

	if forceRefresh {
		return true, newHash, content
	}

	// Check existing hash in SQLite
	existingHash, err := e.store.GetContentHash(filePath)
	if err != nil || existingHash == "" {
		return true, newHash, content // No existing record, must parse
	}

	return existingHash != newHash, newHash, content
}

// discoverMacros scans and indexes macro files incrementally.
func (e *Engine) discoverMacros(opts DiscoveryOptions, result *DiscoveryResult) error {
	macrosDir := e.macrosDir
	if opts.MacrosDir != "" {
		macrosDir = opts.MacrosDir
	}

	if macrosDir == "" {
		return nil
	}

	if _, err := os.Stat(macrosDir); os.IsNotExist(err) {
		return nil // No macros dir is OK
	}

	e.logger.Debug("discovering macros", "macros_dir", macrosDir)

	// Track which files we've seen (for deletion detection)
	seenFiles := make(map[string]bool)

	err := filepath.Walk(macrosDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".star") {
			return nil //nolint:nilerr // Skip directories and non-.star files
		}

		absPath, _ := filepath.Abs(path)
		seenFiles[absPath] = true
		result.MacrosTotal++

		needsParse, newHash, content := e.shouldParseFile(absPath, opts.ForceFullRefresh)
		if !needsParse {
			e.logger.Debug("skipping unchanged macro", "path", absPath)
			result.MacrosSkipped++
			return nil
		}

		// Parse the .star file
		parsed, parseErr := macro.ParseStarlarkFile(absPath, content)
		if parseErr != nil {
			e.logger.Debug("macro parse error", "path", absPath, "error", parseErr.Error())
			result.Errors = append(result.Errors, DiscoveryError{
				Path: absPath, Type: "parse", Message: parseErr.Error(),
			})
			return nil // Continue with other files (graceful degradation)
		}

		e.logger.Debug("parsed macro", "path", absPath, "namespace", parsed.Name)

		// Save to SQLite
		if saveErr := e.saveMacroToStore(parsed, absPath, newHash); saveErr != nil {
			result.Errors = append(result.Errors, DiscoveryError{
				Path: absPath, Type: "save", Message: saveErr.Error(),
			})
			return nil //nolint:nilerr // Continue with other files
		}

		// Update in-memory macro registry (reload from file)
		if e.macroRegistry != nil {
			// Load the module for runtime use
			loader := macro.NewLoader(macrosDir)
			modules, _ := loader.Load()
			for _, mod := range modules {
				if mod.Path == absPath {
					_ = e.macroRegistry.Register(mod)
					break
				}
			}
		}

		result.MacrosChanged++
		return nil
	})

	if err != nil {
		return err
	}

	// Remove deleted macros from SQLite
	result.MacrosDeleted = e.cleanupDeletedMacros(seenFiles)

	return nil
}

// saveMacroToStore saves a parsed macro namespace to the state store.
func (e *Engine) saveMacroToStore(parsed *macro.ParsedNamespace, absPath, hash string) error {
	ns := &state.MacroNamespace{
		Name:     parsed.Name,
		FilePath: parsed.FilePath,
		Package:  parsed.Package,
	}

	var funcs []*state.MacroFunction
	for _, f := range parsed.Functions {
		funcs = append(funcs, &state.MacroFunction{
			Namespace: parsed.Name,
			Name:      f.Name,
			Args:      f.Args,
			Docstring: f.Docstring,
			Line:      f.Line,
		})
	}

	if err := e.store.SaveMacroNamespace(ns, funcs); err != nil {
		return err
	}

	// Update content hash
	return e.store.SetContentHash(absPath, hash, "macro")
}

// discoverModels scans and indexes model files incrementally.
func (e *Engine) discoverModels(opts DiscoveryOptions, result *DiscoveryResult) error {
	modelsDir := e.modelsDir
	if opts.ModelsDir != "" {
		modelsDir = opts.ModelsDir
	}

	if modelsDir == "" {
		return nil
	}

	// Ensure modelsDir is absolute for consistent path resolution
	absModelsDir, absErr := filepath.Abs(modelsDir)
	if absErr != nil {
		return fmt.Errorf("failed to resolve models directory: %w", absErr)
	}

	e.logger.Debug("discovering models", "models_dir", absModelsDir)

	// Clear in-memory state for fresh build
	e.models = make(map[string]*parser.ModelConfig)
	e.registry = registry.NewModelRegistry()

	// Track which files we've seen
	seenFiles := make(map[string]bool)

	scanner := parser.NewScanner(absModelsDir, e.dialect)

	err := filepath.Walk(absModelsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil //nolint:nilerr // Skip directories and non-.sql files
		}

		absPath, _ := filepath.Abs(path)
		seenFiles[absPath] = true
		result.ModelsTotal++

		needsParse, newHash, content := e.shouldParseFile(absPath, opts.ForceFullRefresh)

		var modelConfig *parser.ModelConfig

		if !needsParse {
			// Try to load from SQLite
			storedModel, err := e.store.GetModelByFilePath(absPath)
			if err == nil && storedModel != nil {
				modelConfig = e.reconstructModelConfig(storedModel, absPath, content)
				e.logger.Debug("skipping unchanged model", "path", absPath)
				result.ModelsSkipped++
			}
		}

		if modelConfig == nil {
			// Parse the file
			var parseErr error
			modelConfig, parseErr = scanner.ParseContent(absPath, content)
			if parseErr != nil {
				e.logger.Debug("model parse error", "path", absPath, "error", parseErr.Error())
				result.Errors = append(result.Errors, DiscoveryError{
					Path: absPath, Type: "parse", Message: parseErr.Error(),
				})
				return nil // Continue with other files (graceful degradation)
			}

			e.logger.Debug("parsed model", "path", absPath, "model_name", modelConfig.Name)

			// Save to SQLite
			if err := e.saveModelToStore(modelConfig, absPath, newHash); err != nil {
				result.Errors = append(result.Errors, DiscoveryError{
					Path: absPath, Type: "save", Message: err.Error(),
				})
			}

			result.ModelsChanged++
		}

		// Register in memory
		e.registry.Register(modelConfig)
		e.models[modelConfig.Path] = modelConfig

		return nil
	})

	if err != nil {
		return err
	}

	// Remove deleted models from SQLite
	result.ModelsDeleted = e.cleanupDeletedModels(seenFiles)

	return nil
}

// reconstructModelConfig creates a ModelConfig from stored state and file content.
func (e *Engine) reconstructModelConfig(_ *state.Model, filePath string, content []byte) *parser.ModelConfig {
	// We need to re-parse the file to get the full SQL and sources
	// But we can skip the full parse validation since we know it was valid before
	scanner := parser.NewScanner(e.modelsDir, e.dialect)
	config, err := scanner.ParseContent(filePath, content)
	if err != nil {
		// If parsing fails now, return nil to trigger full re-parse
		return nil
	}
	return config
}

// saveModelToStore saves a parsed model to the state store.
func (e *Engine) saveModelToStore(m *parser.ModelConfig, absPath, hash string) error {
	model := &state.Model{
		Path:           m.Path,
		Name:           m.Name,
		Materialized:   m.Materialized,
		UniqueKey:      m.UniqueKey,
		ContentHash:    computeHash(m.RawContent),
		FilePath:       absPath,
		Owner:          m.Owner,
		Schema:         m.Schema,
		Tags:           m.Tags,
		Meta:           m.Meta,
		UsesSelectStar: m.UsesSelectStar,
	}

	if err := e.store.RegisterModel(model); err != nil {
		return err
	}

	// Store column lineage if available
	if len(m.Columns) > 0 {
		stateColumns := make([]state.ColumnInfo, 0, len(m.Columns))
		for _, col := range m.Columns {
			sources := make([]state.SourceRef, 0, len(col.Sources))
			for _, src := range col.Sources {
				sources = append(sources, state.SourceRef{
					Table:  src.Table,
					Column: src.Column,
				})
			}
			stateColumns = append(stateColumns, state.ColumnInfo{
				Name:          col.Name,
				Index:         col.Index,
				TransformType: col.TransformType,
				Function:      col.Function,
				Sources:       sources,
			})
		}

		_ = e.store.DeleteModelColumns(m.Path)
		if err := e.store.SaveModelColumns(m.Path, stateColumns); err != nil {
			return err
		}
	}

	// Update content hash
	return e.store.SetContentHash(absPath, hash, "model")
}

// validateSeeds checks that seed files exist for external sources.
func (e *Engine) validateSeeds(opts DiscoveryOptions, result *DiscoveryResult) {
	seedsDir := e.seedsDir
	if opts.SeedsDir != "" {
		seedsDir = opts.SeedsDir
	}

	if seedsDir == "" {
		return
	}

	// Get all external sources (tables not mapped to models)
	externalSources := e.registry.GetExternalSources()

	for tableName := range externalSources {
		// Check if seed file exists
		csvPath := filepath.Join(seedsDir, tableName+".csv")
		if _, err := os.Stat(csvPath); os.IsNotExist(err) {
			result.SeedsMissing = append(result.SeedsMissing, tableName)
		} else {
			result.SeedsValidated++
		}
	}
}

// buildGraph constructs the dependency graph from in-memory models.
func (e *Engine) buildGraph() error {
	e.graph.Clear()

	// Phase 1: Add all models as nodes
	for _, m := range e.models {
		e.graph.AddNode(m.Path, m)
	}

	// Phase 2: Resolve dependencies and add edges
	for _, m := range e.models {
		var tableSources []string
		if len(m.Sources) > 0 {
			tableSources = m.Sources
		} else {
			tableSources = m.Imports
		}

		dependencies, _ := e.registry.ResolveDependencies(tableSources)

		for _, dep := range dependencies {
			if dep == m.Path {
				continue // Skip self-references
			}
			if _, exists := e.graph.GetNode(dep); exists {
				if err := e.graph.AddEdge(dep, m.Path); err != nil {
					return fmt.Errorf("failed to add dependency %s -> %s: %w", dep, m.Path, err)
				}
			}
		}
	}

	// Check for cycles
	if hasCycle, cyclePath := e.graph.HasCycle(); hasCycle {
		return fmt.Errorf("circular dependency detected: %v", cyclePath)
	}

	return nil
}

// persistDependencies saves the dependency graph to SQLite.
func (e *Engine) persistDependencies() error {
	for modelPath, m := range e.models {
		model, err := e.store.GetModelByPath(modelPath)
		if err != nil || model == nil {
			continue
		}

		// Get dependencies for this model from the graph (parents = dependencies)
		var parentIDs []string
		incoming := e.graph.GetParents(modelPath)
		for _, parentPath := range incoming {
			parentModel, err := e.store.GetModelByPath(parentPath)
			if err == nil && parentModel != nil {
				parentIDs = append(parentIDs, parentModel.ID)
			}
		}

		// Save dependencies
		if err := e.store.SetDependencies(model.ID, parentIDs); err != nil {
			return fmt.Errorf("failed to set dependencies for %s: %w", m.Path, err)
		}
	}

	return nil
}

// cleanupDeletedMacros removes macro entries for files that no longer exist.
func (e *Engine) cleanupDeletedMacros(seenFiles map[string]bool) int {
	deleted := 0
	existingPaths, _ := e.store.ListMacroFilePaths()

	for _, path := range existingPaths {
		if !seenFiles[path] {
			_ = e.store.DeleteMacroNamespaceByFilePath(path)
			_ = e.store.DeleteContentHash(path)
			deleted++
		}
	}

	return deleted
}

// cleanupDeletedModels removes model entries for files that no longer exist.
func (e *Engine) cleanupDeletedModels(seenFiles map[string]bool) int {
	deleted := 0
	existingPaths, _ := e.store.ListModelFilePaths()

	for _, path := range existingPaths {
		if !seenFiles[path] {
			_ = e.store.DeleteModelByFilePath(path)
			_ = e.store.DeleteContentHash(path)
			deleted++
		}
	}

	return deleted
}

// computeHash generates a SHA256 hash of content.
func computeHash(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:8]) // Use first 8 bytes for brevity
}
