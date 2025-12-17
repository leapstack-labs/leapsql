// Package config provides shared configuration types for LeapSQL.
// This package is decoupled from CLI concerns and can be used by the LSP
// and other tools that need to load project configuration.
package config

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/adapter"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// TargetConfig holds database target configuration.
type TargetConfig struct {
	Type string `koanf:"type"` // duckdb, postgres, snowflake, bigquery

	// File-based databases (DuckDB, SQLite)
	Database string `koanf:"database"` // file path or database name

	// Network databases
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`

	// Common
	Schema string `koanf:"schema"`

	// Snowflake-specific
	Account   string `koanf:"account"`
	Warehouse string `koanf:"warehouse"`
	Role      string `koanf:"role"`

	// Additional driver-specific options
	Options map[string]string `koanf:"options"`

	// Params holds adapter-specific configuration (e.g., DuckDB extensions, secrets, settings)
	Params map[string]any `koanf:"params"`
}

// DefaultSchemaForType returns the default schema for a database type.
// It looks up the dialect in the registry; if not found, returns "main" as fallback.
func DefaultSchemaForType(dbType string) string {
	if d, ok := dialect.Get(dbType); ok && d.DefaultSchema != "" {
		return d.DefaultSchema
	}
	// Fallback for unknown types or dialects without a default schema
	return "main"
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

// ProjectConfig holds the minimal project configuration needed by tools like the LSP.
// This is a subset of the full CLI Config.
type ProjectConfig struct {
	ModelsDir string        `koanf:"models_dir"`
	SeedsDir  string        `koanf:"seeds_dir"`
	MacrosDir string        `koanf:"macros_dir"`
	Target    *TargetConfig `koanf:"target"`
}
