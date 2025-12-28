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
	"strings"
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

// ColumnLineageNode represents a node in the column lineage graph.
type ColumnLineageNode struct {
	ID     string `json:"id"`     // "model.column" format
	Model  string `json:"model"`  // model path
	Column string `json:"column"` // column name
}

// ColumnLineageEdge represents an edge in the column lineage graph.
type ColumnLineageEdge struct {
	Source string `json:"source"` // "model.column" format
	Target string `json:"target"` // "model.column" format
}

// ColumnLineageDoc represents the full column-level lineage graph.
type ColumnLineageDoc struct {
	Nodes []ColumnLineageNode `json:"nodes"`
	Edges []ColumnLineageEdge `json:"edges"`
}

// SourceDoc represents an external data source (not a model).
type SourceDoc struct {
	Name         string   `json:"name"`
	ReferencedBy []string `json:"referenced_by"` // models that use this source
}

// Catalog represents the full documentation catalog.
type Catalog struct {
	GeneratedAt   time.Time        `json:"generated_at"`
	ProjectName   string           `json:"project_name"`
	Models        []*ModelDoc      `json:"models"`
	Sources       []SourceDoc      `json:"sources"`
	Lineage       LineageDoc       `json:"lineage"`
	ColumnLineage ColumnLineageDoc `json:"column_lineage"`
}

// Generator generates documentation from parsed models.
type Generator struct {
	registry    *registry.ModelRegistry
	models      []*core.Model
	projectName string
}

// NewGenerator creates a new documentation generator.
func NewGenerator(projectName string) *Generator {
	return &Generator{
		registry:    registry.NewModelRegistry(),
		projectName: projectName,
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

// GenerateCatalog generates the documentation catalog.
func (g *Generator) GenerateCatalog() *Catalog {
	catalog := &Catalog{
		GeneratedAt: time.Now().UTC(),
		ProjectName: g.projectName,
		Models:      make([]*ModelDoc, 0, len(g.models)),
		Sources:     []SourceDoc{},
	}

	// Build model docs
	modelDocs := make(map[string]*ModelDoc)
	for _, model := range g.models {
		deps, _ := g.registry.ResolveDependencies(model.Sources)
		if deps == nil {
			deps = []string{}
		}

		sources := model.Sources
		if sources == nil {
			sources = []string{}
		}

		doc := &ModelDoc{
			ID:           model.Path, // Use path as ID for simplicity
			Name:         model.Name,
			Path:         model.Path,
			Materialized: model.Materialized,
			UniqueKey:    model.UniqueKey,
			SQL:          model.SQL,
			FilePath:     model.FilePath,
			Sources:      sources,
			Dependencies: deps,
			Dependents:   []string{},
			Columns:      convertColumns(model.Columns),
			UpdatedAt:    time.Now().UTC(),
		}

		// Extract description from SQL comments
		doc.Description = extractDescription(model.RawContent)

		modelDocs[model.Path] = doc
		catalog.Models = append(catalog.Models, doc)
	}

	// Build dependents (reverse dependencies)
	for _, doc := range modelDocs {
		for _, depPath := range doc.Dependencies {
			if depDoc, ok := modelDocs[depPath]; ok {
				depDoc.Dependents = append(depDoc.Dependents, doc.Path)
			}
		}
	}

	// Collect external sources (tables that aren't models)
	sourceRefs := make(map[string][]string) // source name -> models that reference it
	for _, doc := range modelDocs {
		for _, src := range doc.Sources {
			// Check if this source is NOT a model
			if _, isModel := modelDocs[src]; !isModel {
				// Also check by name (in case the source uses just the table name)
				isModelByName := false
				for _, m := range g.models {
					if m.Name == src {
						isModelByName = true
						break
					}
				}
				if !isModelByName {
					sourceRefs[src] = append(sourceRefs[src], doc.Path)
				}
			}
		}
	}

	// Build Sources list
	for srcName, refs := range sourceRefs {
		catalog.Sources = append(catalog.Sources, SourceDoc{
			Name:         srcName,
			ReferencedBy: refs,
		})
	}

	// Build lineage graph (now includes sources)
	catalog.Lineage = g.buildLineage(modelDocs, catalog.Sources)

	// Build column lineage graph
	catalog.ColumnLineage = g.buildColumnLineage(g.models, modelDocs)

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

// buildColumnLineage constructs the column-level lineage graph.
func (g *Generator) buildColumnLineage(models []*core.Model, _ map[string]*ModelDoc) ColumnLineageDoc {
	lineage := ColumnLineageDoc{
		Nodes: []ColumnLineageNode{},
		Edges: []ColumnLineageEdge{},
	}

	// Track which nodes we've added
	nodeSet := make(map[string]bool)

	for _, model := range models {
		for _, col := range model.Columns {
			// Add this column as a node
			nodeID := model.Path + "." + col.Name
			if !nodeSet[nodeID] {
				lineage.Nodes = append(lineage.Nodes, ColumnLineageNode{
					ID:     nodeID,
					Model:  model.Path,
					Column: col.Name,
				})
				nodeSet[nodeID] = true
			}

			// Add edges from source columns to this column
			for _, src := range col.Sources {
				if src.Table == "" || src.Column == "" {
					continue
				}

				// Try to find the source model by name or path
				sourceModelPath := ""
				for _, m := range models {
					if m.Name == src.Table || m.Path == src.Table {
						sourceModelPath = m.Path
						break
					}
				}

				sourceNodeID := ""
				if sourceModelPath != "" {
					sourceNodeID = sourceModelPath + "." + src.Column
				} else {
					// External source (not a model)
					sourceNodeID = src.Table + "." + src.Column
				}

				// Add source node if not exists
				if !nodeSet[sourceNodeID] {
					lineage.Nodes = append(lineage.Nodes, ColumnLineageNode{
						ID:     sourceNodeID,
						Model:  src.Table,
						Column: src.Column,
					})
					nodeSet[sourceNodeID] = true
				}

				// Add edge from source to target
				lineage.Edges = append(lineage.Edges, ColumnLineageEdge{
					Source: sourceNodeID,
					Target: nodeID,
				})
			}
		}
	}

	return lineage
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
//   - metadata.db (SQLite database)
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

	// 1. Generate metadata.db
	dbPath := filepath.Join(outputDir, "metadata.db")
	if err := GenerateMetadataDB(catalog, dbPath); err != nil {
		return fmt.Errorf("failed to generate database: %w", err)
	}

	// 2. Build frontend (TypeScript -> JS, CSS bundled)
	buildResult, err := BuildFrontend(docsDir, true)
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

// Ensure the unused import for strings is used
var _ = strings.Contains
