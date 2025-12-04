// Package parser provides SQL model file parsing with pragma extraction.
// It handles DBGo-specific pragmas like @config, @import, and #if directives.
package parser

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/user/dbgo/pkg/lineage"
)

// ModelConfig holds configuration extracted from SQL model pragmas.
type ModelConfig struct {
	// Path is the model path (e.g., "staging.customers")
	Path string
	// Name is the model name (filename without extension)
	Name string
	// FilePath is the absolute path to the SQL file
	FilePath string
	// Materialized defines how the model is stored: table, view, incremental
	Materialized string
	// UniqueKey for incremental models
	UniqueKey string
	// Imports are explicit model dependencies from @import pragmas (legacy)
	Imports []string
	// Sources are all table names referenced in the SQL (auto-detected via lineage parser)
	// This includes both model references and external/raw tables
	Sources []string
	// Columns contains column-level lineage information
	Columns []ColumnInfo
	// SQL is the raw SQL content (excluding pragmas)
	SQL string
	// RawContent is the full file content including pragmas
	RawContent string
	// Conditionals are #if directives for environment-specific SQL
	Conditionals []Conditional
}

// Conditional represents an #if directive block.
type Conditional struct {
	Condition string
	Content   string
}

// SourceRef represents a source column reference in lineage.
type SourceRef struct {
	Table  string
	Column string
}

// ColumnInfo represents column lineage information.
type ColumnInfo struct {
	Name          string
	Index         int
	TransformType string      // "" (direct) or "EXPR"
	Function      string      // "sum", "count", etc.
	Sources       []SourceRef // where this column comes from
}

// Parser parses SQL model files and extracts pragmas.
type Parser struct {
	// BaseDir is the models directory root
	BaseDir string
}

// NewParser creates a new parser with the given base directory.
func NewParser(baseDir string) *Parser {
	return &Parser{BaseDir: baseDir}
}

// Pragma patterns
var (
	// @config(materialized='table', unique_key='id')
	configPattern = regexp.MustCompile(`--\s*@config\s*\(\s*(.+?)\s*\)`)
	// @import(staging.orders)
	importPattern = regexp.MustCompile(`--\s*@import\s*\(\s*([^)]+)\s*\)`)
	// #if env == 'prod'
	ifPattern = regexp.MustCompile(`--\s*#if\s+(.+)`)
	// #endif
	endifPattern = regexp.MustCompile(`--\s*#endif`)
	// Key-value patterns for config
	kvPattern = regexp.MustCompile(`(\w+)\s*=\s*'([^']*)'`)
)

// ParseFile parses a single SQL model file.
func (p *Parser) ParseFile(filePath string) (*ModelConfig, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseContent(filePath, string(content))
}

// ParseContent parses SQL content from a file.
func (p *Parser) ParseContent(filePath string, content string) (*ModelConfig, error) {
	config := &ModelConfig{
		FilePath:     filePath,
		RawContent:   content,
		Materialized: "table", // default
		Imports:      []string{},
		Conditionals: []Conditional{},
	}

	// Derive name and path from file path
	config.Name = strings.TrimSuffix(filepath.Base(filePath), ".sql")
	config.Path = p.filePathToModelPath(filePath)

	var sqlLines []string
	var inConditional bool
	var currentConditional Conditional

	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()

		// Check for @config pragma
		if matches := configPattern.FindStringSubmatch(line); len(matches) > 1 {
			p.parseConfig(matches[1], config)
			continue
		}

		// Check for @import pragma
		if matches := importPattern.FindStringSubmatch(line); len(matches) > 1 {
			imports := strings.Split(matches[1], ",")
			for _, imp := range imports {
				imp = strings.TrimSpace(imp)
				if imp != "" {
					config.Imports = append(config.Imports, imp)
				}
			}
			continue
		}

		// Check for #if directive
		if matches := ifPattern.FindStringSubmatch(line); len(matches) > 1 {
			inConditional = true
			currentConditional = Conditional{Condition: strings.TrimSpace(matches[1])}
			continue
		}

		// Check for #endif directive
		if endifPattern.MatchString(line) {
			if inConditional {
				config.Conditionals = append(config.Conditionals, currentConditional)
				inConditional = false
				currentConditional = Conditional{}
			}
			continue
		}

		// Accumulate conditional content
		if inConditional {
			currentConditional.Content += line + "\n"
			continue
		}

		// Regular SQL line
		sqlLines = append(sqlLines, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning file: %w", err)
	}

	config.SQL = strings.TrimSpace(strings.Join(sqlLines, "\n"))

	// Auto-detect table sources and column lineage using the lineage parser
	if config.SQL != "" {
		result, err := extractLineage(config.SQL)
		if err == nil {
			config.Sources = result.Sources
			config.Columns = result.Columns
		}
		// If lineage extraction fails, we continue without sources/columns
		// The model may have syntax errors or use unsupported SQL features
	}

	return config, nil
}

// lineageResult holds both table sources and column lineage information.
type lineageResult struct {
	Sources []string
	Columns []ColumnInfo
}

// extractLineage uses the lineage parser to extract all table sources and column lineage from SQL.
func extractLineage(sql string) (*lineageResult, error) {
	modelLineage, err := lineage.ExtractLineage(sql, nil)
	if err != nil {
		return nil, err
	}

	// Convert lineage.ColumnLineage to parser.ColumnInfo
	columns := make([]ColumnInfo, 0, len(modelLineage.Columns))
	for i, col := range modelLineage.Columns {
		// Convert source columns
		sources := make([]SourceRef, 0, len(col.Sources))
		for _, src := range col.Sources {
			sources = append(sources, SourceRef{
				Table:  src.Table,
				Column: src.Column,
			})
		}

		columns = append(columns, ColumnInfo{
			Name:          col.Name,
			Index:         i,
			TransformType: string(col.Transform),
			Function:      col.Function,
			Sources:       sources,
		})
	}

	return &lineageResult{
		Sources: modelLineage.Sources,
		Columns: columns,
	}, nil
}

// parseConfig parses config pragma key-value pairs.
func (p *Parser) parseConfig(configStr string, config *ModelConfig) {
	matches := kvPattern.FindAllStringSubmatch(configStr, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			key := strings.ToLower(match[1])
			value := match[2]

			switch key {
			case "materialized":
				config.Materialized = value
			case "unique_key":
				config.UniqueKey = value
			}
		}
	}
}

// filePathToModelPath converts a file path to a model path.
// e.g., "/base/staging/customers.sql" -> "staging.customers"
func (p *Parser) filePathToModelPath(filePath string) string {
	relPath, err := filepath.Rel(p.BaseDir, filePath)
	if err != nil {
		// Fallback to just the filename
		return strings.TrimSuffix(filepath.Base(filePath), ".sql")
	}

	// Remove .sql extension
	relPath = strings.TrimSuffix(relPath, ".sql")

	// Convert path separators to dots
	parts := strings.Split(relPath, string(filepath.Separator))
	return strings.Join(parts, ".")
}

// ExtractReferences scans SQL for table references using ref() function.
// e.g., SELECT * FROM {{ ref('staging.orders') }}
func ExtractReferences(sql string) []string {
	// Pattern for ref('model_path') or ref("model_path")
	refPattern := regexp.MustCompile(`ref\s*\(\s*['"]([^'"]+)['"]\s*\)`)
	matches := refPattern.FindAllStringSubmatch(sql, -1)

	var refs []string
	seen := make(map[string]bool)
	for _, match := range matches {
		if len(match) > 1 && !seen[match[1]] {
			refs = append(refs, match[1])
			seen[match[1]] = true
		}
	}
	return refs
}

// Scanner scans a directory for SQL model files.
type Scanner struct {
	parser *Parser
}

// NewScanner creates a new directory scanner.
func NewScanner(baseDir string) *Scanner {
	return &Scanner{
		parser: NewParser(baseDir),
	}
}

// ScanDir recursively scans a directory for SQL files and parses them.
func (s *Scanner) ScanDir(dir string) ([]*ModelConfig, error) {
	var models []*ModelConfig

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-SQL files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}

		// Skip hidden files
		if strings.HasPrefix(info.Name(), ".") {
			return nil
		}

		config, err := s.parser.ParseFile(path)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", path, err)
		}

		models = append(models, config)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan directory: %w", err)
	}

	return models, nil
}

// GetParser returns the underlying parser.
func (s *Scanner) GetParser() *Parser {
	return s.parser
}
