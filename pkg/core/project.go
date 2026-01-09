package core

// ProjectConfig holds project-level configuration.
type ProjectConfig struct {
	ModelsDir string        `koanf:"models_dir"`
	SeedsDir  string        `koanf:"seeds_dir"`
	MacrosDir string        `koanf:"macros_dir"`
	Target    *TargetConfig `koanf:"target"`
	Lint      *LintConfig   `koanf:"lint"`
}

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
func (c *ProjectHealthConfig) IsEnabled() bool {
	if c == nil || c.Enabled == nil {
		return true
	}
	return *c.Enabled
}
