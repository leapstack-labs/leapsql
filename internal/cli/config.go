// Package cli provides the command-line interface for LeapSQL.
package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all CLI configuration options.
type Config struct {
	ModelsDir    string               `mapstructure:"models_dir"`
	SeedsDir     string               `mapstructure:"seeds_dir"`
	MacrosDir    string               `mapstructure:"macros_dir"`
	DatabasePath string               `mapstructure:"database"`
	StatePath    string               `mapstructure:"state_path"`
	Environment  string               `mapstructure:"environment"`
	Verbose      bool                 `mapstructure:"verbose"`
	OutputFormat string               `mapstructure:"output"`
	Environments map[string]EnvConfig `mapstructure:"environments"`
}

// EnvConfig holds environment-specific configuration overrides.
type EnvConfig struct {
	DatabasePath string `mapstructure:"database"`
	ModelsDir    string `mapstructure:"models_dir"`
	SeedsDir     string `mapstructure:"seeds_dir"`
	MacrosDir    string `mapstructure:"macros_dir"`
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

// LoadConfig loads configuration from file, environment variables, and flags.
// Precedence (highest to lowest): flags > env vars > config file > defaults
func LoadConfig(cfgFile string) (*Config, error) {
	// Create a new viper instance to avoid global state issues
	v := viper.New()

	// Set defaults
	v.SetDefault("models_dir", DefaultModelsDir)
	v.SetDefault("seeds_dir", DefaultSeedsDir)
	v.SetDefault("macros_dir", DefaultMacrosDir)
	v.SetDefault("state_path", DefaultStateFile)
	v.SetDefault("environment", DefaultEnv)
	v.SetDefault("verbose", false)
	v.SetDefault("output", DefaultOutput)

	// Environment variable support
	v.SetEnvPrefix("LEAPSQL")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	// Config file handling - only read if explicitly specified or file exists
	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		if err := v.ReadInConfig(); err != nil {
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
				v.SetConfigFile(configPath)
				if err := v.ReadInConfig(); err != nil {
					return nil, fmt.Errorf("error reading config file %s: %w", configPath, err)
				}
				break
			}
		}
	}

	// Merge with global viper for flag bindings
	for _, key := range viper.AllKeys() {
		if viper.IsSet(key) {
			v.Set(key, viper.Get(key))
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Apply environment-specific overrides if an environment is selected
	if cfg.Environment != "" && cfg.Environments != nil {
		if envCfg, ok := cfg.Environments[cfg.Environment]; ok {
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
	}

	return &cfg, nil
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
