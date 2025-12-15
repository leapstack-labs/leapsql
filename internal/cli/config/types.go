// Package config provides configuration management for LeapSQL CLI.
package config

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

// Default configuration values.
const (
	DefaultModelsDir = "models"
	DefaultSeedsDir  = "seeds"
	DefaultMacrosDir = "macros"
	DefaultStateFile = ".leapsql/state.db"
	DefaultEnv       = "dev"
	DefaultOutput    = "auto" // Auto-detect: TTY=text, non-TTY=markdown
)
