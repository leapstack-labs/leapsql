package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/adapter"
)

// DefaultSchemaForType returns the default schema for a database type.
func DefaultSchemaForType(dbType string) string {
	switch strings.ToLower(dbType) {
	case "duckdb":
		return "main"
	case "postgres", "postgresql":
		return "public"
	case "snowflake":
		return "PUBLIC"
	case "bigquery":
		return "" // BigQuery uses project.dataset.table
	default:
		return "main"
	}
}

// Validate checks if the target configuration is valid.
// It uses the adapter registry to determine which adapter types are available.
func (t *TargetConfig) Validate() error {
	if t.Type == "" {
		return fmt.Errorf("target type is required")
	}

	// Use adapter registry as single source of truth
	if !adapter.IsRegistered(strings.ToLower(t.Type)) {
		return &adapter.UnknownAdapterError{
			Type:      t.Type,
			Available: adapter.ListAdapters(),
		}
	}

	return nil
}

// ApplyDefaults sets default values based on the database type.
func (t *TargetConfig) ApplyDefaults() {
	if t.Schema == "" {
		t.Schema = DefaultSchemaForType(t.Type)
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.ModelsDir == "" {
		return fmt.Errorf("models_dir is required")
	}

	// Only validate directory existence if we're running a command that needs it
	// This allows help commands to work without a valid directory
	return nil
}

// ValidateDirectories checks if required directories exist.
func (c *Config) ValidateDirectories() error {
	if _, err := os.Stat(c.ModelsDir); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s\nHint: Create the directory or use --models-dir to specify a different path", c.ModelsDir)
	}
	return nil
}
