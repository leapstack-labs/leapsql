// Package config provides configuration management for LeapSQL CLI.
//
// This package provides CLI-specific configuration fields and functionality.
// Shared types (TargetConfig, LintConfig, DocsConfig, RuleOptions) are in pkg/core.
package config

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
)

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
	ProjectRoot  string               `koanf:"-"` // Computed project root, not from config file
	ModelsDir    string               `koanf:"models_dir"`
	SeedsDir     string               `koanf:"seeds_dir"`
	MacrosDir    string               `koanf:"macros_dir"`
	DatabasePath string               `koanf:"database"` // Deprecated: use Target.Database
	StatePath    string               `koanf:"state_path"`
	Environment  string               `koanf:"environment"`
	Verbose      bool                 `koanf:"verbose"`
	OutputFormat string               `koanf:"output"`
	Target       *core.TargetConfig   `koanf:"target"`
	Lint         *core.LintConfig     `koanf:"lint"`
	UI           *UIConfig            `koanf:"ui"`
	Environments map[string]EnvConfig `koanf:"environments"`
}

// EnvConfig holds environment-specific configuration overrides.
type EnvConfig struct {
	DatabasePath string             `koanf:"database"` // Deprecated: use Target.Database
	ModelsDir    string             `koanf:"models_dir"`
	SeedsDir     string             `koanf:"seeds_dir"`
	MacrosDir    string             `koanf:"macros_dir"`
	Target       *core.TargetConfig `koanf:"target"`
}

// CLI-specific default configuration values.
// Shared defaults (ModelsDir, SeedsDir, MacrosDir) come from internal/config.
const (
	DefaultStateFile = ".leapsql/state.db"
	DefaultEnv       = "dev"
	DefaultOutput    = "auto" // Auto-detect: TTY=text, non-TTY=markdown
)
