// Package cli provides the command-line interface for LeapSQL.
package cli

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/adapter"
	"github.com/spf13/viper"
)

// TargetConfig holds database target configuration.
type TargetConfig struct {
	Type string `mapstructure:"type"` // duckdb, postgres, snowflake, bigquery

	// File-based databases (DuckDB, SQLite)
	Database string `mapstructure:"database"` // file path or database name

	// Network databases
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`

	// Common
	Schema string `mapstructure:"schema"`

	// Snowflake-specific
	Account   string `mapstructure:"account"`
	Warehouse string `mapstructure:"warehouse"`
	Role      string `mapstructure:"role"`

	// Additional driver-specific options
	Options map[string]string `mapstructure:"options"`
}

// Config holds all CLI configuration options.
type Config struct {
	ModelsDir    string               `mapstructure:"models_dir"`
	SeedsDir     string               `mapstructure:"seeds_dir"`
	MacrosDir    string               `mapstructure:"macros_dir"`
	DatabasePath string               `mapstructure:"database"` // Deprecated: use Target.Database
	StatePath    string               `mapstructure:"state_path"`
	Environment  string               `mapstructure:"environment"`
	Verbose      bool                 `mapstructure:"verbose"`
	OutputFormat string               `mapstructure:"output"`
	Target       *TargetConfig        `mapstructure:"target"`
	Environments map[string]EnvConfig `mapstructure:"environments"`
}

// EnvConfig holds environment-specific configuration overrides.
type EnvConfig struct {
	DatabasePath string        `mapstructure:"database"` // Deprecated: use Target.Database
	ModelsDir    string        `mapstructure:"models_dir"`
	SeedsDir     string        `mapstructure:"seeds_dir"`
	MacrosDir    string        `mapstructure:"macros_dir"`
	Target       *TargetConfig `mapstructure:"target"`
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

// DefaultSchemaForType returns the default schema for a database type.
func DefaultSchemaForType(dbType string) string {
	switch strings.ToLower(dbType) {
	case "duckdb":
		return "main"
	case "postgres", "postgresql":
		return "public"
	case "snowflake":
		return "PUBLIC"
	case "bigquery":
		return "" // BigQuery uses project.dataset.table
	default:
		return "main"
	}
}

// expandEnvVars expands ${VAR} patterns in a string with environment variable values.
func expandEnvVars(s string) string {
	// Match ${VAR} pattern
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	return re.ReplaceAllStringFunc(s, func(match string) string {
		// Extract variable name from ${VAR}
		varName := match[2 : len(match)-1]
		if val := os.Getenv(varName); val != "" {
			return val
		}
		return match // Return original if not found
	})
}

// expandTargetEnvVars expands environment variables in sensitive target fields.
func expandTargetEnvVars(t *TargetConfig) {
	if t == nil {
		return
	}
	t.Password = expandEnvVars(t.Password)
	t.User = expandEnvVars(t.User)
	t.Host = expandEnvVars(t.Host)
	t.Database = expandEnvVars(t.Database)
	t.Account = expandEnvVars(t.Account)
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

// ApplyDefaults sets default values based on the database type.
func (t *TargetConfig) ApplyDefaults() {
	if t.Schema == "" {
		t.Schema = DefaultSchemaForType(t.Type)
	}
}

// LoadConfig loads configuration from file, environment variables, and flags.
// Precedence (highest to lowest): flags > env vars > config file > defaults
func LoadConfig(cfgFile string) (*Config, error) {
	return LoadConfigWithTarget(cfgFile, "")
}

// LoadConfigWithTarget loads configuration with an optional target override.
// The targetOverride parameter specifies which environment's target to use.
func LoadConfigWithTarget(cfgFile string, targetOverride string) (*Config, error) {
	// Set defaults on global viper
	viper.SetDefault("models_dir", DefaultModelsDir)
	viper.SetDefault("seeds_dir", DefaultSeedsDir)
	viper.SetDefault("macros_dir", DefaultMacrosDir)
	viper.SetDefault("state_path", DefaultStateFile)
	viper.SetDefault("environment", DefaultEnv)
	viper.SetDefault("verbose", false)
	viper.SetDefault("output", DefaultOutput)

	// Environment variable support
	viper.SetEnvPrefix("LEAPSQL")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	// Config file handling - only read if explicitly specified or file exists
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("error reading config file %s: %w", cfgFile, err)
		}
	} else {
		// Try to read config file from known locations (with explicit extensions)
		configPaths := []string{
			"leapsql.yaml",
			"leapsql.yml",
			".leapsql.yaml",
			".leapsql.yml",
		}

		// Add home directory paths
		if homeDir, err := os.UserHomeDir(); err == nil {
			configPaths = append(configPaths,
				homeDir+"/.leapsql/leapsql.yaml",
				homeDir+"/.leapsql/leapsql.yml",
			)
		}

		for _, configPath := range configPaths {
			if _, err := os.Stat(configPath); err == nil {
				viper.SetConfigFile(configPath)
				if err := viper.ReadInConfig(); err != nil {
					return nil, fmt.Errorf("error reading config file %s: %w", configPath, err)
				}
				break
			}
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Determine which environment to use for target selection
	envForTarget := cfg.Environment
	if targetOverride != "" {
		envForTarget = targetOverride
	}

	// Apply environment-specific overrides if an environment is selected
	if envForTarget != "" && cfg.Environments != nil {
		if envCfg, ok := cfg.Environments[envForTarget]; ok {
			// Apply directory overrides (only if not using targetOverride, or for legacy support)
			if targetOverride == "" {
				if envCfg.DatabasePath != "" {
					cfg.DatabasePath = envCfg.DatabasePath
				}
				if envCfg.ModelsDir != "" {
					cfg.ModelsDir = envCfg.ModelsDir
				}
				if envCfg.SeedsDir != "" {
					cfg.SeedsDir = envCfg.SeedsDir
				}
				if envCfg.MacrosDir != "" {
					cfg.MacrosDir = envCfg.MacrosDir
				}
			}

			// Merge environment target with base target
			if envCfg.Target != nil {
				cfg.Target = mergeTargetConfig(cfg.Target, envCfg.Target)
			}
		}
	}

	// Initialize default target if not specified
	if cfg.Target == nil {
		cfg.Target = &TargetConfig{
			Type:     "duckdb",
			Database: cfg.DatabasePath, // Use legacy database path
		}
	}

	// Apply defaults based on target type
	cfg.Target.ApplyDefaults()

	// Expand environment variables in target
	expandTargetEnvVars(cfg.Target)

	// For backward compatibility: sync DatabasePath with Target.Database
	if cfg.Target.Database == "" && cfg.DatabasePath != "" {
		cfg.Target.Database = cfg.DatabasePath
	} else if cfg.Target.Database != "" && cfg.DatabasePath == "" {
		cfg.DatabasePath = cfg.Target.Database
	}

	// Validate target configuration
	if err := cfg.Target.Validate(); err != nil {
		return nil, fmt.Errorf("invalid target configuration: %w", err)
	}

	return &cfg, nil
}

// mergeTargetConfig merges two target configs, with override taking precedence.
func mergeTargetConfig(base, override *TargetConfig) *TargetConfig {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	// Start with a copy of base
	merged := &TargetConfig{
		Type:      base.Type,
		Database:  base.Database,
		Host:      base.Host,
		Port:      base.Port,
		User:      base.User,
		Password:  base.Password,
		Schema:    base.Schema,
		Account:   base.Account,
		Warehouse: base.Warehouse,
		Role:      base.Role,
		Options:   make(map[string]string),
	}

	// Copy base options
	for k, v := range base.Options {
		merged.Options[k] = v
	}

	// Apply overrides
	if override.Type != "" {
		merged.Type = override.Type
	}
	if override.Database != "" {
		merged.Database = override.Database
	}
	if override.Host != "" {
		merged.Host = override.Host
	}
	if override.Port != 0 {
		merged.Port = override.Port
	}
	if override.User != "" {
		merged.User = override.User
	}
	if override.Password != "" {
		merged.Password = override.Password
	}
	if override.Schema != "" {
		merged.Schema = override.Schema
	}
	if override.Account != "" {
		merged.Account = override.Account
	}
	if override.Warehouse != "" {
		merged.Warehouse = override.Warehouse
	}
	if override.Role != "" {
		merged.Role = override.Role
	}

	// Merge options
	for k, v := range override.Options {
		merged.Options[k] = v
	}

	return merged
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.ModelsDir == "" {
		return fmt.Errorf("models_dir is required")
	}

	// Only validate directory existence if we're running a command that needs it
	// This allows help commands to work without a valid directory
	return nil
}

// ValidateDirectories checks if required directories exist.
func (c *Config) ValidateDirectories() error {
	if _, err := os.Stat(c.ModelsDir); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s\nHint: Create the directory or use --models-dir to specify a different path", c.ModelsDir)
	}
	return nil
}

// GetConfigFileUsed returns the path to the config file being used, if any.
func GetConfigFileUsed() string {
	return viper.ConfigFileUsed()
}
