// Package docs generates static documentation sites for DBGo projects.
// It exports model metadata, lineage, and run history to JSON and generates
// a self-contained static site that can be hosted on GitHub Pages or similar.
package docs

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/user/dbgo/internal/parser"
	"github.com/user/dbgo/internal/registry"
)

//go:embed static/*
var staticFiles embed.FS

// ModelDoc represents a model for documentation purposes.
type ModelDoc struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Path         string    `json:"path"`
	Materialized string    `json:"materialized"`
	UniqueKey    string    `json:"unique_key,omitempty"`
	SQL          string    `json:"sql"`
	FilePath     string    `json:"file_path"`
	Sources      []string  `json:"sources"`
	Dependencies []string  `json:"dependencies"`
	Dependents   []string  `json:"dependents"`
	Description  string    `json:"description,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
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

// Catalog represents the full documentation catalog.
type Catalog struct {
	GeneratedAt time.Time   `json:"generated_at"`
	ProjectName string      `json:"project_name"`
	Models      []*ModelDoc `json:"models"`
	Lineage     LineageDoc  `json:"lineage"`
}

// Generator generates documentation from parsed models.
type Generator struct {
	registry    *registry.ModelRegistry
	models      []*parser.ModelConfig
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
	scanner := parser.NewScanner(modelsDir)
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

	// Build lineage graph
	catalog.Lineage = g.buildLineage(modelDocs)

	return catalog
}

// buildLineage constructs the lineage graph from model docs.
func (g *Generator) buildLineage(modelDocs map[string]*ModelDoc) LineageDoc {
	lineage := LineageDoc{
		Nodes: make([]string, 0, len(modelDocs)),
		Edges: []LineageEdge{},
	}

	// Add all model nodes
	for path := range modelDocs {
		lineage.Nodes = append(lineage.Nodes, path)
	}

	// Add edges from dependencies
	for _, doc := range modelDocs {
		for _, depPath := range doc.Dependencies {
			lineage.Edges = append(lineage.Edges, LineageEdge{
				Source: depPath,
				Target: doc.Path,
			})
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

// Build generates the static site to the output directory.
func (g *Generator) Build(outputDir string) error {
	catalog := g.GenerateCatalog()

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create data directory
	dataDir := filepath.Join(outputDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Write catalog.json
	catalogJSON, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "catalog.json"), catalogJSON, 0644); err != nil {
		return fmt.Errorf("failed to write catalog.json: %w", err)
	}

	// Copy static files
	if err := g.copyStaticFiles(outputDir); err != nil {
		return fmt.Errorf("failed to copy static files: %w", err)
	}

	return nil
}

// copyStaticFiles copies embedded static files to the output directory.
func (g *Generator) copyStaticFiles(outputDir string) error {
	return fs.WalkDir(staticFiles, "static", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the root "static" directory
		if path == "static" {
			return nil
		}

		// Get relative path from "static/"
		relPath := path[len("static/"):]
		outPath := filepath.Join(outputDir, relPath)

		if d.IsDir() {
			return os.MkdirAll(outPath, 0755)
		}

		// Copy file
		content, err := staticFiles.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}

		return os.WriteFile(outPath, content, 0644)
	})
}

// Serve starts a local HTTP server for the documentation site.
func (g *Generator) Serve(outputDir string, port int) error {
	// Build first
	if err := g.Build(outputDir); err != nil {
		return err
	}

	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Serving docs at http://localhost%s\n", addr)

	return http.ListenAndServe(addr, http.FileServer(http.Dir(outputDir)))
}

// ServeFromFS serves the documentation site from the generated files.
func ServeFromFS(outputDir string, port int) error {
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Serving docs at http://localhost%s\n", addr)

	return http.ListenAndServe(addr, http.FileServer(http.Dir(outputDir)))
}

// WriteJSON writes any data structure to a JSON file.
func WriteJSON(path string, data interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

// CopyFile copies a file from src to dst.
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
