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

// DocsConfig is an alias for the shared docs configuration.
// This allows CLI code to use config.DocsConfig without importing pkg/core.
type DocsConfig = core.DocsConfig

// UIConfig holds configuration for the UI server.
type UIConfig struct {
	Port             int    `koanf:"port"`
	AutoOpen         bool   `koanf:"auto_open"`
	Watch            bool   `koanf:"watch"`
	Theme            string `koanf:"theme"`
	DataPreviewLimit int    `koanf:"data_preview_limit"`
}

// DefaultUIConfig returns a UIConfig with default values.
func DefaultUIConfig() *UIConfig {
	return &UIConfig{
		Port:             8765,
		AutoOpen:         true,
		Watch:            true,
		Theme:            "default",
		DataPreviewLimit: 50,
	}
}

// GetUIConfig returns the UI config with defaults applied for any unset values.
func (c *Config) GetUIConfig() *UIConfig {
	if c.UI == nil {
		return DefaultUIConfig()
	}
	ui := c.UI
	if ui.Port == 0 {
		ui.Port = 8765
	}
	if ui.DataPreviewLimit == 0 {
		ui.DataPreviewLimit = 50
	}
	return ui
}

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
	Docs         *DocsConfig          `koanf:"docs"`
	UI           *UIConfig            `koanf:"ui"`
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
