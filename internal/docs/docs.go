// Package docs generates static documentation sites for DBGo projects.
// It exports model metadata, lineage, and run history to JSON and generates
// a self-contained static site that can be hosted on GitHub Pages or similar.
package docs

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/leapstack-labs/leapsql/internal/loader"
	"github.com/leapstack-labs/leapsql/internal/registry"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

//go:embed template.html
var htmlTemplate string

// SourceRef represents a source column reference in lineage.
type SourceRef struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// ColumnDoc represents column lineage information for documentation.
type ColumnDoc struct {
	Name          string      `json:"name"`
	Index         int         `json:"index"`
	TransformType string      `json:"transform_type,omitempty"` // "" (direct) or "EXPR"
	Function      string      `json:"function,omitempty"`       // "sum", "count", etc.
	Sources       []SourceRef `json:"sources"`                  // where this column comes from
}

// ModelDoc represents a model for documentation purposes.
type ModelDoc struct {
	ID           string      `json:"id"`
	Name         string      `json:"name"`
	Path         string      `json:"path"`
	Materialized string      `json:"materialized"`
	UniqueKey    string      `json:"unique_key,omitempty"`
	SQL          string      `json:"sql"`
	FilePath     string      `json:"file_path"`
	Sources      []string    `json:"sources"`
	Dependencies []string    `json:"dependencies"`
	Dependents   []string    `json:"dependents"`
	Columns      []ColumnDoc `json:"columns"`
	Description  string      `json:"description,omitempty"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

// LineageEdge represents an edge in the dependency graph.
type LineageEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// LineageDoc represents the full lineage graph.
type LineageDoc struct {
	Nodes []string      `json:"nodes"`
	Edges []LineageEdge `json:"edges"`
}

// SourceDoc represents an external data source (not a model).
type SourceDoc struct {
	Name         string   `json:"name"`
	ReferencedBy []string `json:"referenced_by"` // models that use this source
}

// Catalog represents the full documentation catalog.
// Column lineage is queried directly from state.db views by the frontend.
type Catalog struct {
	GeneratedAt time.Time   `json:"generated_at"`
	ProjectName string      `json:"project_name"`
	Models      []*ModelDoc `json:"models"`
	Sources     []SourceDoc `json:"sources"`
	Lineage     LineageDoc  `json:"lineage"`
}

// Generator generates documentation from parsed models.
type Generator struct {
	registry    *registry.ModelRegistry
	models      []*core.Model
	projectName string
	theme       string
	statePath   string // Path to state.db (set when loading from state)
}

// NewGenerator creates a new documentation generator.
func NewGenerator(projectName string) *Generator {
	return &Generator{
		registry:    registry.NewModelRegistry(),
		projectName: projectName,
		theme:       "vercel", // Default theme
	}
}

// SetTheme sets the theme for documentation generation.
func (g *Generator) SetTheme(theme string) {
	if theme != "" {
		g.theme = theme
	}
}

// LoadModels loads models from a directory.
func (g *Generator) LoadModels(modelsDir string) error {
	// Pass nil for dialect - lineage extraction will be skipped for docs
	scanner := loader.NewScanner(modelsDir, nil)
	models, err := scanner.ScanDir(modelsDir)
	if err != nil {
		return fmt.Errorf("failed to scan models: %w", err)
	}

	g.models = models

	// Register all models in the registry
	for _, model := range models {
		g.registry.Register(model)
	}

	return nil
}

// LoadFromState loads models and lineage from the state database.
// This is the preferred method as it includes column lineage extracted during discover.
func (g *Generator) LoadFromState(store core.Store) error {
	return g.LoadFromStateWithPath(store, "")
}

// LoadFromStateWithPath loads models and stores the state DB path for later use.
// The statePath is used by Build() to copy state.db to metadata.db.
// Note: Dependencies and column lineage are queried directly from state.db views
// by the frontend, so we only load what's needed for manifest generation.
func (g *Generator) LoadFromStateWithPath(store core.Store, statePath string) error {
	g.statePath = statePath

	// 1. Get all persisted models
	persistedModels, err := store.ListModels()
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}

	// 2. Get columns (needed for column count in manifest stats)
	columnsMap, err := store.BatchGetAllColumns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	// 3. Convert PersistedModel -> core.Model with columns attached
	g.models = make([]*core.Model, 0, len(persistedModels))
	for _, pm := range persistedModels {
		model := pm.Model // embedded core.Model
		if model == nil {
			continue
		}
		// Attach columns (needed for manifest column count)
		if cols, ok := columnsMap[model.Path]; ok {
			model.Columns = cols
		}
		g.models = append(g.models, model)
		g.registry.Register(model)
	}

	return nil
}

// GenerateCatalog generates the documentation catalog.
// Note: Dependencies, sources, and lineage are queried directly from state.db views
// by the frontend. This method generates minimal data for manifest/nav tree.
func (g *Generator) GenerateCatalog() *Catalog {
	catalog := &Catalog{
		GeneratedAt: time.Now().UTC(),
		ProjectName: g.projectName,
		Models:      make([]*ModelDoc, 0, len(g.models)),
		Sources:     []SourceDoc{}, // Populated from views by frontend
	}

	// Build model docs with minimal data for manifest generation
	modelDocs := make(map[string]*ModelDoc)
	for _, model := range g.models {
		doc := &ModelDoc{
			ID:           model.Path, // Use path as ID for simplicity
			Name:         model.Name,
			Path:         model.Path,
			Materialized: model.Materialized,
			UniqueKey:    model.UniqueKey,
			SQL:          model.SQL,
			FilePath:     model.FilePath,
			Sources:      []string{}, // Populated from views by frontend
			Dependencies: []string{}, // Populated from views by frontend
			Dependents:   []string{}, // Populated from views by frontend
			Columns:      convertColumns(model.Columns),
			UpdatedAt:    time.Now().UTC(),
		}

		// Extract description from SQL comments
		doc.Description = extractDescription(model.RawContent)

		modelDocs[model.Path] = doc
		catalog.Models = append(catalog.Models, doc)
	}

	// Build minimal lineage graph (model nodes only, no edges)
	// Full lineage with edges is queried from v_lineage_edges by frontend
	catalog.Lineage = g.buildLineage(modelDocs, catalog.Sources)

	// Note: Column lineage is queried directly from state.db views by the frontend
	// (v_column_lineage_nodes, v_column_lineage_edges)

	return catalog
}

// buildLineage constructs the lineage graph from model docs and sources.
func (g *Generator) buildLineage(modelDocs map[string]*ModelDoc, sources []SourceDoc) LineageDoc {
	lineage := LineageDoc{
		Nodes: make([]string, 0, len(modelDocs)+len(sources)),
		Edges: []LineageEdge{},
	}

	// Add all model nodes
	for path := range modelDocs {
		lineage.Nodes = append(lineage.Nodes, path)
	}

	// Add all source nodes (prefixed with "source:" to distinguish)
	for _, src := range sources {
		lineage.Nodes = append(lineage.Nodes, "source:"+src.Name)
	}

	// Add edges from dependencies (model -> model)
	for _, doc := range modelDocs {
		for _, depPath := range doc.Dependencies {
			lineage.Edges = append(lineage.Edges, LineageEdge{
				Source: depPath,
				Target: doc.Path,
			})
		}
	}

	// Add edges from sources to models (source -> model)
	for _, src := range sources {
		for _, modelPath := range src.ReferencedBy {
			lineage.Edges = append(lineage.Edges, LineageEdge{
				Source: "source:" + src.Name,
				Target: modelPath,
			})
		}
	}

	return lineage
}

// convertColumns converts core.ColumnInfo to ColumnDoc.
func convertColumns(columns []core.ColumnInfo) []ColumnDoc {
	if columns == nil {
		return []ColumnDoc{}
	}

	result := make([]ColumnDoc, 0, len(columns))
	for _, col := range columns {
		sources := make([]SourceRef, 0, len(col.Sources))
		for _, src := range col.Sources {
			sources = append(sources, SourceRef{
				Table:  src.Table,
				Column: src.Column,
			})
		}

		result = append(result, ColumnDoc{
			Name:          col.Name,
			Index:         col.Index,
			TransformType: string(col.TransformType),
			Function:      col.Function,
			Sources:       sources,
		})
	}
	return result
}

// extractDescription extracts description from SQL comments.
func extractDescription(content string) string {
	// Look for -- comments at the start that aren't pragmas
	lines := []byte(content)
	var desc string
	inDesc := false

	for _, line := range splitLines(string(lines)) {
		trimmed := trimPrefix(line, "-- ")
		if trimmed != line { // It's a comment
			// Skip pragma comments
			if hasPrefix(trimmed, "@config") || hasPrefix(trimmed, "@import") || hasPrefix(trimmed, "#if") || hasPrefix(trimmed, "#endif") {
				continue
			}
			if inDesc || desc == "" {
				if desc != "" {
					desc += " "
				}
				desc += trimmed
				inDesc = true
			}
		} else if trimmed != "" && !isEmptyOrWhitespace(trimmed) {
			// Non-comment, non-empty line - stop looking for description
			break
		}
	}

	return desc
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func trimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func isEmptyOrWhitespace(s string) bool {
	for _, c := range s {
		if c != ' ' && c != '\t' && c != '\r' {
			return false
		}
	}
	return true
}

// Build generates documentation site with SQLite database.
// Output structure:
//   - index.html (shell + manifest + JS/CSS)
//   - metadata.db (SQLite database - copied from state.db)
func (g *Generator) Build(outputDir string) error {
	catalog := g.GenerateCatalog()
	manifest := GenerateManifest(catalog)

	// Create output directory
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get the docs package directory for building frontend
	docsDir, err := GetDocsDir()
	if err != nil {
		return fmt.Errorf("failed to get docs directory: %w", err)
	}

	// 1. Copy state.db to metadata.db (state.db has all views and data)
	dbPath := filepath.Join(outputDir, "metadata.db")
	if g.statePath != "" {
		if err := CopyFromState(g.statePath, dbPath); err != nil {
			return fmt.Errorf("failed to copy state database: %w", err)
		}
	} else {
		// Fallback: if no state path, create empty database
		// This shouldn't happen in normal usage but provides safety
		return fmt.Errorf("state database path not set - call LoadFromStateWithPath first")
	}

	// 2. Build frontend (TypeScript -> JS, CSS bundled)
	buildResult, err := BuildFrontend(docsDir, true, g.theme)
	if err != nil {
		return fmt.Errorf("failed to build frontend: %w", err)
	}

	// 3. Marshal manifest to JSON for embedding
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// 4. Parse and execute HTML template
	tmpl, err := template.New("docs").Parse(htmlTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	// G203: CSS/JS are from our own build output, not user input
	data := templateDataV2{
		ProjectName:  g.projectName,
		CSS:          template.CSS(buildResult.CSS), //nolint:gosec // G203: trusted build output
		JS:           template.JS(buildResult.JS),   //nolint:gosec // G203: trusted build output
		ManifestJSON: template.JS(manifestJSON),     //nolint:gosec // G203: trusted build output
		DevMode:      false,
	}

	// Write index.html
	outputPath := filepath.Join(outputDir, "index.html")
	f, err := os.Create(outputPath) //nolint:gosec // G304: path is from trusted source
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := tmpl.Execute(f, data); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	// 5. Copy sql.js WASM assets (for production mode)
	if err := copyWasmAssets(docsDir, outputDir); err != nil {
		return fmt.Errorf("failed to copy WASM assets: %w", err)
	}

	return nil
}

// copyWasmAssets copies sql.js-httpvfs WASM files to the output directory.
func copyWasmAssets(docsDir, outputDir string) error {
	// Look for WASM files in node_modules
	wasmSrc := filepath.Join(docsDir, "node_modules", "sql.js-httpvfs", "dist", "sql-wasm.wasm")
	wasmDst := filepath.Join(outputDir, "sql-wasm.wasm")

	// Check if the source exists
	if _, err := os.Stat(wasmSrc); os.IsNotExist(err) {
		// sql.js-httpvfs not installed yet - this is okay for dev, warn for prod
		return nil
	}

	return CopyFile(wasmSrc, wasmDst)
}

// Serve starts a local HTTP server for the documentation site.
func (g *Generator) Serve(outputDir string, port int) error {
	// Build first
	if err := g.Build(outputDir); err != nil {
		return err
	}

	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Serving docs at http://localhost%s\n", addr)

	server := &http.Server{
		Addr:              addr,
		Handler:           http.FileServer(http.Dir(outputDir)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server.ListenAndServe()
}

// ServeFromFS serves the documentation site from the generated files.
func ServeFromFS(outputDir string, port int) error {
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Serving docs at http://localhost%s\n", addr)

	server := &http.Server{
		Addr:              addr,
		Handler:           http.FileServer(http.Dir(outputDir)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server.ListenAndServe()
}

// WriteJSON writes any data structure to a JSON file.
func WriteJSON(path string, data any) error {
	f, err := os.Create(path) //nolint:gosec // G304: path is from trusted source
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

// CopyFile copies a file from src to dst.
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src) //nolint:gosec // G304: src is from trusted source
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst) //nolint:gosec // G304: dst is from trusted source
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
