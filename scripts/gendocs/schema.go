package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// generateSchemaDocs generates schema documentation for frontmatter and config.
func generateSchemaDocs(outDir string) error {
	log.Printf("Generating schema docs to %s", outDir)

	// Create output directory
	if err := os.MkdirAll(outDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate configuration reference
	if err := generateConfigurationDoc(outDir); err != nil {
		return fmt.Errorf("failed to generate configuration.md: %w", err)
	}
	log.Printf("  Generated configuration.md")

	return nil
}

// ConfigField represents a configuration field definition.
type ConfigField struct {
	Name        string
	Type        string
	Required    bool
	Default     string
	Description string
	Category    string // "project", "common", "duckdb", "postgres", "snowflake"
}

// getConfigSchema returns the configuration schema definition.
// This is based on internal/config/types.go TargetConfig and ProjectConfig.
func getConfigSchema() []ConfigField {
	return []ConfigField{
		// Project settings
		{Name: "models_dir", Type: "string", Default: "models", Description: "Path to models directory", Category: "project"},
		{Name: "seeds_dir", Type: "string", Default: "seeds", Description: "Path to seeds directory", Category: "project"},
		{Name: "macros_dir", Type: "string", Default: "macros", Description: "Path to macros directory", Category: "project"},

		// Common target options
		{Name: "type", Type: "string", Required: true, Description: "Database type: duckdb, postgres, snowflake, bigquery", Category: "common"},
		{Name: "schema", Type: "string", Required: false, Description: "Default schema for models", Category: "common"},

		// File-based databases (DuckDB)
		{Name: "database", Type: "string", Required: false, Description: "File path (DuckDB) or database name", Category: "duckdb"},

		// Network databases (PostgreSQL)
		{Name: "host", Type: "string", Required: false, Description: "Database host", Category: "postgres"},
		{Name: "port", Type: "int", Required: false, Default: "5432", Description: "Database port", Category: "postgres"},
		{Name: "user", Type: "string", Required: false, Description: "Database username", Category: "postgres"},
		{Name: "password", Type: "string", Required: false, Description: "Database password", Category: "postgres"},

		// Snowflake-specific
		{Name: "account", Type: "string", Required: false, Description: "Snowflake account identifier", Category: "snowflake"},
		{Name: "warehouse", Type: "string", Required: false, Description: "Snowflake warehouse name", Category: "snowflake"},
		{Name: "role", Type: "string", Required: false, Description: "Snowflake role to use", Category: "snowflake"},

		// Advanced options
		{Name: "options", Type: "map[string]string", Required: false, Description: "Additional driver-specific options", Category: "advanced"},
		{Name: "params", Type: "map[string]any", Required: false, Description: "Adapter-specific configuration (extensions, secrets, settings)", Category: "advanced"},
	}
}

// generateConfigurationDoc generates the configuration reference page.
func generateConfigurationDoc(outDir string) error {
	w := NewMarkdownWriter()

	// Frontmatter
	w.Frontmatter("Configuration", "LeapSQL configuration reference")
	w.GeneratedMarker()

	// Title and intro
	w.Header(1, "Configuration")
	w.Paragraph("LeapSQL is configured via `leapsql.yaml` in your project root.")

	// Project settings section
	w.Header(2, "Project Settings")
	w.Paragraph("Directory paths for project assets:")

	fields := getConfigSchema()
	projectHeaders := []string{"Field", "Type", "Default", "Description"}
	var projectRows [][]string
	for _, f := range fields {
		if f.Category == "project" {
			defVal := f.Default
			if defVal == "" {
				defVal = "-"
			}
			projectRows = append(projectRows, []string{
				InlineCode(f.Name),
				f.Type,
				InlineCode(defVal),
				f.Description,
			})
		}
	}
	w.Table(projectHeaders, projectRows)

	// Target configuration section
	w.Header(2, "Target Configuration")
	w.Paragraph("Database targets are defined under the `targets` key. Each target specifies how to connect to a database.")

	// Common options
	w.Header(3, "Common Options")
	commonHeaders := []string{"Field", "Type", "Required", "Description"}
	var commonRows [][]string
	for _, f := range fields {
		if f.Category == "common" {
			req := "No"
			if f.Required {
				req = "Yes"
			}
			commonRows = append(commonRows, []string{
				InlineCode(f.Name),
				f.Type,
				req,
				f.Description,
			})
		}
	}
	w.Table(commonHeaders, commonRows)

	// DuckDB
	w.Header(3, "DuckDB")
	w.Paragraph("DuckDB is an embedded analytical database. It can run in-memory or persist to a file.")

	duckdbHeaders := []string{"Field", "Type", "Description"}
	var duckdbRows [][]string
	for _, f := range fields {
		if f.Category == "duckdb" {
			duckdbRows = append(duckdbRows, []string{
				InlineCode(f.Name),
				f.Type,
				f.Description,
			})
		}
	}
	w.Table(duckdbHeaders, duckdbRows)

	w.Header(4, "DuckDB Example")
	w.CodeBlock("yaml", `targets:
  dev:
    type: duckdb
    database: ./data/warehouse.duckdb
    schema: main

  # In-memory DuckDB (default)
  memory:
    type: duckdb
    schema: main`)

	// PostgreSQL
	w.Header(3, "PostgreSQL")
	w.Paragraph("PostgreSQL connection options:")

	pgHeaders := []string{"Field", "Type", "Default", "Description"}
	var pgRows [][]string
	for _, f := range fields {
		if f.Category == "postgres" {
			defVal := f.Default
			if defVal == "" {
				defVal = "-"
			}
			pgRows = append(pgRows, []string{
				InlineCode(f.Name),
				f.Type,
				defVal,
				f.Description,
			})
		}
	}
	w.Table(pgHeaders, pgRows)

	w.Header(4, "PostgreSQL Example")
	w.CodeBlock("yaml", `targets:
  prod:
    type: postgres
    host: localhost
    port: 5432
    user: analytics
    password: ${POSTGRES_PASSWORD}
    database: warehouse
    schema: analytics`)

	// Snowflake
	w.Header(3, "Snowflake")
	w.Paragraph("Snowflake-specific connection options:")

	sfHeaders := []string{"Field", "Type", "Description"}
	var sfRows [][]string
	for _, f := range fields {
		if f.Category == "snowflake" {
			sfRows = append(sfRows, []string{
				InlineCode(f.Name),
				f.Type,
				f.Description,
			})
		}
	}
	w.Table(sfHeaders, sfRows)

	w.Header(4, "Snowflake Example")
	w.CodeBlock("yaml", `targets:
  prod:
    type: snowflake
    account: xy12345.us-east-1
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    database: ANALYTICS
    warehouse: COMPUTE_WH
    role: ANALYTICS_ROLE
    schema: PUBLIC`)

	// Full example
	w.Header(2, "Full Configuration Example")
	w.CodeBlock("yaml", `# LeapSQL Configuration
# leapsql.yaml

# Project directories
models_dir: models
seeds_dir: seeds
macros_dir: macros

# Default target to use
default_target: dev

# Database targets
targets:
  dev:
    type: duckdb
    database: ./data/dev.duckdb
    schema: main
    params:
      extensions:
        - httpfs
        - parquet

  staging:
    type: postgres
    host: staging-db.example.com
    port: 5432
    user: leapsql
    password: ${STAGING_DB_PASSWORD}
    database: analytics
    schema: staging

  prod:
    type: postgres
    host: prod-db.example.com
    port: 5432
    user: leapsql
    password: ${PROD_DB_PASSWORD}
    database: analytics
    schema: public`)

	// Environment variables
	w.Header(2, "Environment Variables")
	w.Paragraph("Use `${VAR_NAME}` syntax to reference environment variables in your configuration:")
	w.CodeBlock("yaml", `targets:
  prod:
    type: postgres
    password: ${POSTGRES_PASSWORD}`)

	// Write file
	filename := filepath.Join(outDir, "configuration.md")
	return os.WriteFile(filename, w.Bytes(), 0600)
}
