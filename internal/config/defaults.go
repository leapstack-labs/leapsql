package config

// Default configuration values.
const (
	DefaultModelsDir = "models"
	DefaultSeedsDir  = "seeds"
	DefaultMacrosDir = "macros"
)

// ApplyDefaults applies default values to a ProjectConfig.
func (c *ProjectConfig) ApplyDefaults() {
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

// ApplyDefaults applies default values to a TargetConfig based on the target type.
func (t *TargetConfig) ApplyDefaults() {
	if t == nil {
		return
	}

	switch t.Type {
	case "postgres":
		if t.Port == 0 {
			t.Port = 5432
		}
		if t.Schema == "" {
			t.Schema = "public"
		}
	case "duckdb":
		if t.Schema == "" {
			t.Schema = "main"
		}
	}
}
