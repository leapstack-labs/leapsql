package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	sharedcfg "github.com/leapstack-labs/leapsql/internal/config"
	"github.com/spf13/pflag"
)

// loggerKey is used to store logger in context.
// This key is shared with root.go via both using the same type.
type loggerKey struct{}

// Package-level koanf instance and config file tracking
var (
	k              = koanf.New(".")
	configFileUsed string
	currentConfig  *Config // Stores the loaded config for access by commands
)

// findConfigFile finds the config file to use.
// Priority: explicit path > leapsql.yaml > leapsql.yml
func findConfigFile(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if _, err := os.Stat("leapsql.yaml"); err == nil {
		return "leapsql.yaml"
	}
	if _, err := os.Stat("leapsql.yml"); err == nil {
		return "leapsql.yml"
	}
	return ""
}

// resolvePathRelativeTo resolves a path relative to baseDir if it's not absolute.
// Returns the path unchanged if it's empty or already absolute.
func resolvePathRelativeTo(path, baseDir string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(baseDir, path)
}

// ResetConfig resets the koanf instance. Used for testing.
func ResetConfig() {
	k = koanf.New(".")
	configFileUsed = ""
	currentConfig = nil
}

// LoadConfig loads configuration from file, environment variables, and flags.
// Precedence (highest to lowest): flags > env vars > config file > defaults
func LoadConfig(cfgFile string, flags *pflag.FlagSet) (*Config, error) {
	return LoadConfigWithTarget(cfgFile, "", flags)
}

// LoadConfigWithTarget loads configuration with an optional target override.
// The targetOverride parameter specifies which environment's target to use.
// The flags parameter allows CLI flags to override config file and env var values.
func LoadConfigWithTarget(cfgFile string, targetOverride string, flags *pflag.FlagSet) (*Config, error) {
	// Reset koanf for fresh load
	k = koanf.New(".")

	// 1. Load defaults
	if err := k.Load(confmap.Provider(map[string]interface{}{
		"models_dir":  DefaultModelsDir,
		"seeds_dir":   DefaultSeedsDir,
		"macros_dir":  DefaultMacrosDir,
		"state_path":  DefaultStateFile,
		"environment": DefaultEnv,
		"verbose":     false,
		"output":      DefaultOutput,
	}, "."), nil); err != nil {
		return nil, fmt.Errorf("failed to load defaults: %w", err)
	}

	// 2. Find and load config file
	configFileUsed = findConfigFile(cfgFile)
	if configFileUsed != "" {
		if err := k.Load(file.Provider(configFileUsed), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("error reading config file %s: %w", configFileUsed, err)
		}
	}

	// 3. Load environment variables (LEAPSQL_ prefix)
	// Transform: LEAPSQL_MODELS_DIR -> models_dir
	if err := k.Load(env.Provider("LEAPSQL_", ".", func(s string) string {
		return strings.ToLower(strings.TrimPrefix(s, "LEAPSQL_"))
	}), nil); err != nil {
		return nil, fmt.Errorf("failed to load env vars: %w", err)
	}

	// 4. Load flags (highest priority - overrides env vars and config file)
	if flags != nil {
		if err := k.Load(posflag.ProviderWithFlag(flags, ".", k, func(f *pflag.Flag) (string, interface{}) {
			// Only load flags that were explicitly set
			if !f.Changed {
				return "", nil
			}
			// Transform kebab-case to snake_case for config keys
			key := strings.ReplaceAll(f.Name, "-", "_")
			return key, posflag.FlagVal(flags, f)
		}), nil); err != nil {
			return nil, fmt.Errorf("failed to load flags: %w", err)
		}
	}

	// 5. Unmarshal into Config struct
	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// 6. Resolve relative paths in config relative to config file directory.
	// This ensures paths like "models/" in a config file at "/projects/app/leapsql.yaml"
	// resolve to "/projects/app/models/" rather than being relative to CWD.
	if configFileUsed != "" {
		configDir := filepath.Dir(configFileUsed)
		if absDir, err := filepath.Abs(configDir); err == nil {
			configDir = absDir
		}
		cfg.ModelsDir = resolvePathRelativeTo(cfg.ModelsDir, configDir)
		cfg.SeedsDir = resolvePathRelativeTo(cfg.SeedsDir, configDir)
		cfg.MacrosDir = resolvePathRelativeTo(cfg.MacrosDir, configDir)
		cfg.StatePath = resolvePathRelativeTo(cfg.StatePath, configDir)
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
				cfg.Target = MergeTargetConfig(cfg.Target, envCfg.Target)
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
	sharedcfg.ApplyTargetDefaults(cfg.Target)

	// Expand environment variables in target
	expandTargetEnvVars(cfg.Target)

	// For backward compatibility: sync DatabasePath with Target.Database
	if cfg.Target.Database == "" && cfg.DatabasePath != "" {
		cfg.Target.Database = cfg.DatabasePath
	} else if cfg.Target.Database != "" && cfg.DatabasePath == "" {
		cfg.DatabasePath = cfg.Target.Database
	}

	// Validate target configuration
	if err := sharedcfg.ValidateTarget(cfg.Target); err != nil {
		return nil, fmt.Errorf("invalid target configuration: %w", err)
	}

	// Store config for access by commands
	currentConfig = &cfg

	return &cfg, nil
}

// GetConfigFileUsed returns the path to the config file being used, if any.
func GetConfigFileUsed() string {
	return configFileUsed
}

// GetCurrentConfig returns the currently loaded configuration.
// This is available after LoadConfig or LoadConfigWithTarget is called.
func GetCurrentConfig() *Config {
	return currentConfig
}

// LoggerKey returns the context key used for storing the logger.
// This allows the commands package to retrieve the logger from context
// without creating an import cycle with the cli package.
func LoggerKey() interface{} {
	return loggerKey{}
}

// GetLogger retrieves the logger from the command context.
func GetLogger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	// Return discard logger as safe fallback
	return slog.New(slog.DiscardHandler)
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

// MergeTargetConfig merges two target configs, with override taking precedence.
func MergeTargetConfig(base, override *TargetConfig) *TargetConfig {
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
		Params:    make(map[string]any),
	}

	// Copy base options
	for k, v := range base.Options {
		merged.Options[k] = v
	}

	// Copy base params
	for k, v := range base.Params {
		merged.Params[k] = v
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

	// Merge params (override takes precedence)
	for k, v := range override.Params {
		merged.Params[k] = v
	}

	return merged
}
