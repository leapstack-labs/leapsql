// Package loader provides YAML frontmatter parsing for SQL model files.
package loader

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"gopkg.in/yaml.v3"
)

// FrontmatterConfig represents parsed YAML frontmatter.
// Unknown fields cause parse errors (use Meta for extensions).
type FrontmatterConfig struct {
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Materialized string            `yaml:"materialized"` // table, view, incremental
	UniqueKey    string            `yaml:"unique_key"`
	Owner        string            `yaml:"owner"`
	Schema       string            `yaml:"schema"`
	Tags         []string          `yaml:"tags"`
	Tests        []core.TestConfig `yaml:"tests"`
	Meta         map[string]any    `yaml:"meta"` // Extension point for custom fields
}

// FrontmatterResult holds the result of frontmatter extraction.
type FrontmatterResult struct {
	Config  *FrontmatterConfig
	SQL     string // SQL content after frontmatter
	HasYAML bool   // Whether frontmatter was found
}

// frontmatterPattern matches /*--- ... ---*/ blocks
// The pattern allows optional content between the delimiters
var frontmatterPattern = regexp.MustCompile(`(?s)^\s*/\*---\s*\n(.*?)\s*---\*/`)

// ExtractFrontmatter extracts YAML frontmatter from SQL content.
// Returns the parsed config, remaining SQL, and any error.
func ExtractFrontmatter(content string) (*FrontmatterResult, error) {
	result := &FrontmatterResult{
		Config:  &FrontmatterConfig{},
		SQL:     content,
		HasYAML: false,
	}

	matches := frontmatterPattern.FindStringSubmatch(content)
	if len(matches) < 2 {
		// No frontmatter found, return content as-is
		return result, nil
	}

	result.HasYAML = true
	yamlContent := matches[1]

	// Remove the frontmatter block from SQL
	result.SQL = strings.TrimSpace(frontmatterPattern.ReplaceAllString(content, ""))

	// Parse YAML with strict mode to reject unknown fields
	config, err := parseFrontmatterYAML(yamlContent)
	if err != nil {
		return nil, err
	}

	result.Config = config
	return result, nil
}

// testConfigYAML is an internal type for YAML unmarshaling with correct tags.
type testConfigYAML struct {
	Unique         []string                  `yaml:"unique,omitempty"`
	NotNull        []string                  `yaml:"not_null,omitempty"`
	AcceptedValues *acceptedValuesConfigYAML `yaml:"accepted_values,omitempty"`
}

// acceptedValuesConfigYAML is an internal type for YAML unmarshaling.
type acceptedValuesConfigYAML struct {
	Column string   `yaml:"column"`
	Values []string `yaml:"values"`
}

// frontmatterConfigYAML is an internal type for YAML unmarshaling.
type frontmatterConfigYAML struct {
	Name         string           `yaml:"name"`
	Description  string           `yaml:"description"`
	Materialized string           `yaml:"materialized"`
	UniqueKey    string           `yaml:"unique_key"`
	Owner        string           `yaml:"owner"`
	Schema       string           `yaml:"schema"`
	Tags         []string         `yaml:"tags"`
	Tests        []testConfigYAML `yaml:"tests"`
	Meta         map[string]any   `yaml:"meta"`
}

// parseFrontmatterYAML parses YAML content with strict field validation.
func parseFrontmatterYAML(yamlContent string) (*FrontmatterConfig, error) {
	// First, decode into a map to check for unknown fields
	var rawMap map[string]any
	if err := yaml.Unmarshal([]byte(yamlContent), &rawMap); err != nil {
		return nil, &FrontmatterParseError{
			Message: fmt.Sprintf("invalid YAML: %v", err),
		}
	}

	// Check for unknown fields
	knownFields := map[string]bool{
		"name":         true,
		"description":  true,
		"materialized": true,
		"unique_key":   true,
		"owner":        true,
		"schema":       true,
		"tags":         true,
		"tests":        true,
		"meta":         true,
	}

	for field := range rawMap {
		if !knownFields[field] {
			return nil, &UnknownFieldError{
				Field: field,
			}
		}
	}

	// Now decode into the internal YAML struct
	var yamlConfig frontmatterConfigYAML
	if err := yaml.Unmarshal([]byte(yamlContent), &yamlConfig); err != nil {
		return nil, &FrontmatterParseError{
			Message: fmt.Sprintf("failed to parse frontmatter: %v", err),
		}
	}

	// Validate materialized value if present
	if yamlConfig.Materialized != "" {
		validMaterialized := map[string]bool{
			"table":       true,
			"view":        true,
			"incremental": true,
		}
		if !validMaterialized[yamlConfig.Materialized] {
			return nil, &FrontmatterParseError{
				Message: fmt.Sprintf("invalid materialized value: %q, must be one of: table, view, incremental", yamlConfig.Materialized),
			}
		}
	}

	// Convert to FrontmatterConfig with core types
	config := &FrontmatterConfig{
		Name:         yamlConfig.Name,
		Description:  yamlConfig.Description,
		Materialized: yamlConfig.Materialized,
		UniqueKey:    yamlConfig.UniqueKey,
		Owner:        yamlConfig.Owner,
		Schema:       yamlConfig.Schema,
		Tags:         yamlConfig.Tags,
		Meta:         yamlConfig.Meta,
	}

	// Convert tests
	for _, t := range yamlConfig.Tests {
		test := core.TestConfig{
			Unique:  t.Unique,
			NotNull: t.NotNull,
		}
		if t.AcceptedValues != nil {
			test.AcceptedValues = &core.AcceptedValuesConfig{
				Column: t.AcceptedValues.Column,
				Values: t.AcceptedValues.Values,
			}
		}
		config.Tests = append(config.Tests, test)
	}

	return config, nil
}

// ApplyDefaults applies default values to a FrontmatterConfig based on file context.
func (c *FrontmatterConfig) ApplyDefaults(filename string, dirPath string) {
	// Default name from filename (without .sql extension)
	if c.Name == "" {
		c.Name = strings.TrimSuffix(filename, ".sql")
	}

	// Default materialized to "table"
	if c.Materialized == "" {
		c.Materialized = "table"
	}

	// Default schema from directory path
	if c.Schema == "" && dirPath != "" {
		c.Schema = dirPath
	}
}

// FrontmatterParseError represents a frontmatter parsing error.
type FrontmatterParseError struct {
	File    string
	Line    int
	Message string
}

func (e *FrontmatterParseError) Error() string {
	if e.File != "" {
		if e.Line > 0 {
			return fmt.Sprintf("%s:%d: %s", e.File, e.Line, e.Message)
		}
		return fmt.Sprintf("%s: %s", e.File, e.Message)
	}
	return e.Message
}

// UnknownFieldError represents an error for unknown frontmatter fields.
type UnknownFieldError struct {
	File  string
	Field string
}

func (e *UnknownFieldError) Error() string {
	msg := fmt.Sprintf("unknown field %q in frontmatter, use \"meta\" field for custom fields", e.Field)
	if e.File != "" {
		return fmt.Sprintf("%s: %s", e.File, msg)
	}
	return msg
}
