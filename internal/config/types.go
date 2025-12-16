// Package config provides shared configuration types for LeapSQL.
// This package is decoupled from CLI concerns and can be used by the LSP
// and other tools that need to load project configuration.
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

// ProjectConfig holds the minimal project configuration needed by tools like the LSP.
// This is a subset of the full CLI Config.
type ProjectConfig struct {
	ModelsDir string        `koanf:"models_dir"`
	SeedsDir  string        `koanf:"seeds_dir"`
	MacrosDir string        `koanf:"macros_dir"`
	Target    *TargetConfig `koanf:"target"`
}
