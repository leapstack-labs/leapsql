package config

import "github.com/leapstack-labs/leapsql/pkg/core"

// Default configuration values.
const (
	DefaultModelsDir = "models"
	DefaultSeedsDir  = "seeds"
	DefaultMacrosDir = "macros"
	DefaultDocsTheme = "vercel"
)

// ApplyDefaults applies default values to a ProjectConfig.
func ApplyDefaults(c *core.ProjectConfig) {
	if c == nil {
		return
	}
	if c.ModelsDir == "" {
		c.ModelsDir = DefaultModelsDir
	}
	if c.SeedsDir == "" {
		c.SeedsDir = DefaultSeedsDir
	}
	if c.MacrosDir == "" {
		c.MacrosDir = DefaultMacrosDir
	}
}

// ApplyDocsDefaults applies default values to a DocsConfig.
func ApplyDocsDefaults(cfg *core.DocsConfig) {
	if cfg == nil {
		return
	}
	if cfg.Theme == "" {
		cfg.Theme = DefaultDocsTheme
	}
}

// ApplyTargetDefaults applies default values to a TargetConfig based on the target type.
func ApplyTargetDefaults(t *core.TargetConfig) {
	if t == nil {
		return
	}

	// Apply default schema based on type
	if t.Schema == "" {
		t.Schema = DefaultSchemaForType(t.Type)
	}

	// Apply type-specific defaults
	if t.Type == "postgres" {
		if t.Port == 0 {
			t.Port = 5432
		}
	}
}
