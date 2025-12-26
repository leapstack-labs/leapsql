// Package config provides configuration management for LeapSQL CLI.
//
// This package extends the shared configuration types from pkg/core
// with CLI-specific fields and functionality. The shared types (TargetConfig,
// LintConfig) are defined in pkg/core and re-exported here via
// type aliases for convenience.
package config

import (
	sharedcfg "github.com/leapstack-labs/leapsql/internal/config"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// TargetConfig is an alias for the shared target configuration.
// This allows CLI code to use config.TargetConfig without importing pkg/core.
type TargetConfig = core.TargetConfig

// LintConfig is an alias for the shared lint configuration.
// This allows CLI code to use config.LintConfig without importing pkg/core.
type LintConfig = core.LintConfig

// RuleOptions is an alias for the shared rule options type.
// This allows CLI code to use config.RuleOptions without importing pkg/core.
type RuleOptions = core.RuleOptions

// Config holds all CLI configuration options.
type Config struct {
	ModelsDir    string               `koanf:"models_dir"`
	SeedsDir     string               `koanf:"seeds_dir"`
	MacrosDir    string               `koanf:"macros_dir"`
	DatabasePath string               `koanf:"database"` // Deprecated: use Target.Database
	StatePath    string               `koanf:"state_path"`
	Environment  string               `koanf:"environment"`
	Verbose      bool                 `koanf:"verbose"`
	OutputFormat string               `koanf:"output"`
	Target       *TargetConfig        `koanf:"target"`
	Lint         *LintConfig          `koanf:"lint"`
	Environments map[string]EnvConfig `koanf:"environments"`
}

// EnvConfig holds environment-specific configuration overrides.
type EnvConfig struct {
	DatabasePath string        `koanf:"database"` // Deprecated: use Target.Database
	ModelsDir    string        `koanf:"models_dir"`
	SeedsDir     string        `koanf:"seeds_dir"`
	MacrosDir    string        `koanf:"macros_dir"`
	Target       *TargetConfig `koanf:"target"`
}

// Default configuration values - uses shared defaults from internal/config
const (
	DefaultModelsDir = sharedcfg.DefaultModelsDir
	DefaultSeedsDir  = sharedcfg.DefaultSeedsDir
	DefaultMacrosDir = sharedcfg.DefaultMacrosDir
	DefaultStateFile = ".leapsql/state.db"
	DefaultEnv       = "dev"
	DefaultOutput    = "auto" // Auto-detect: TTY=text, non-TTY=markdown
)
