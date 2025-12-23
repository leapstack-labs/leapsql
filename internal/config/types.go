// Package config provides shared configuration types for LeapSQL.
// This package is decoupled from CLI concerns and can be used by the LSP
// and other tools that need to load project configuration.
package config

import (
	"fmt"
	"strings"

	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
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

// ToTargetInfo converts TargetConfig to a starlark.TargetInfo for template rendering.
// This extracts only the fields that should be exposed to templates (not credentials).
func (t *TargetConfig) ToTargetInfo() *starctx.TargetInfo {
	return &starctx.TargetInfo{
		Type:     t.Type,
		Schema:   t.Schema,
		Database: t.Database,
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

// LintConfig holds lint rule configuration.
type LintConfig struct {
	// Disabled contains rule IDs to disable
	Disabled []string `koanf:"disabled"`

	// Severity maps rule ID to severity override (error, warning, info, hint)
	Severity map[string]string `koanf:"severity"`

	// Rules contains rule-specific options
	Rules map[string]RuleOptions `koanf:"rules"`

	// ProjectHealth holds project-level linting configuration
	ProjectHealth *ProjectHealthConfig `koanf:"project_health"`
}

// RuleOptions holds rule-specific configuration options.
type RuleOptions map[string]any

// ProjectHealthConfig holds configuration for project-level linting.
type ProjectHealthConfig struct {
	// Enabled controls whether project health linting is enabled (default: true)
	Enabled *bool `koanf:"enabled"`

	// Thresholds for various rules
	Thresholds ProjectHealthThresholds `koanf:"thresholds"`

	// Rules maps rule IDs to severity overrides (off, info, warning, error)
	Rules map[string]string `koanf:"rules"`
}

// ProjectHealthThresholds holds configurable thresholds for project health rules.
type ProjectHealthThresholds struct {
	ModelFanout        int `koanf:"model_fanout"`        // PM04: default 3
	TooManyJoins       int `koanf:"too_many_joins"`      // PM05: default 7
	PassthroughColumns int `koanf:"passthrough_columns"` // PL01: default 20
	StarlarkComplexity int `koanf:"starlark_complexity"` // PT01: default 10
}

// IsEnabled returns whether project health linting is enabled.
// Defaults to true if not explicitly set.
func (c *ProjectHealthConfig) IsEnabled() bool {
	if c == nil || c.Enabled == nil {
		return true
	}
	return *c.Enabled
}

// ProjectConfig holds the minimal project configuration needed by tools like the LSP.
// This is a subset of the full CLI Config.
type ProjectConfig struct {
	ModelsDir string        `koanf:"models_dir"`
	SeedsDir  string        `koanf:"seeds_dir"`
	MacrosDir string        `koanf:"macros_dir"`
	Target    *TargetConfig `koanf:"target"`
	Lint      *LintConfig   `koanf:"lint"`
}
