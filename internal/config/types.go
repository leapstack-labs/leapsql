// Package config provides shared configuration types for LeapSQL.
// This package is decoupled from CLI concerns and can be used by the LSP
// and other tools that need to load project configuration.
package config

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/adapter"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// ProjectConfig is an alias for core.ProjectConfig for backward compatibility.
type ProjectConfig = core.ProjectConfig

// TargetConfig is an alias for core.TargetConfig for backward compatibility.
type TargetConfig = core.TargetConfig

// LintConfig is an alias for core.LintConfig for backward compatibility.
type LintConfig = core.LintConfig

// RuleOptions is an alias for core.RuleOptions for backward compatibility.
type RuleOptions = core.RuleOptions

// ProjectHealthConfig is an alias for core.ProjectHealthConfig for backward compatibility.
type ProjectHealthConfig = core.ProjectHealthConfig

// ProjectHealthThresholds is an alias for core.ProjectHealthThresholds for backward compatibility.
type ProjectHealthThresholds = core.ProjectHealthThresholds

// DefaultSchemaForType returns the default schema for a database type.
// It looks up the dialect in the registry; if not found, returns "main" as fallback.
func DefaultSchemaForType(dbType string) string {
	if d, ok := dialect.Get(dbType); ok && d.DefaultSchema != "" {
		return d.DefaultSchema
	}
	// Fallback for unknown types or dialects without a default schema
	return "main"
}

// ValidateTarget checks if the target configuration is valid.
// It uses the adapter registry to determine which adapter types are available.
func ValidateTarget(t *core.TargetConfig) error {
	if t == nil {
		return nil
	}
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
