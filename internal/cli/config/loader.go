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
	intconfig "github.com/leapstack-labs/leapsql/internal/config"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/spf13/pflag"
)

// loggerKey is used to store logger in context.
// This key is shared with root.go via both using the same type.
type loggerKey struct{}

// maxUpwardSearchLevels limits how far up the directory tree to search for config files.
const maxUpwardSearchLevels = 10

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

// configExistsIn checks if a leapsql config file exists in the directory.
func configExistsIn(dir string) bool {
	for _, name := range []string{"leapsql.yaml", "leapsql.yml"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// findProjectRootUpward searches upward from startDir for a leapsql config file.
// Returns empty string if not found within maxUpwardSearchLevels.
func findProjectRootUpward(startDir string) string {
	dir := startDir
	for i := 0; i < maxUpwardSearchLevels; i++ {
		if configExistsIn(dir) {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root
			break
		}
		dir = parent
	}
	return ""
}

// inferProjectRoot determines the project root from CLI flags and filesystem.
// Priority:
//  1. Explicit --project-dir flag
//  2. Infer from --models-dir (parent if contains config or named "models")
//  3. Search upward from CWD for leapsql.yaml
//  4. Current working directory
func inferProjectRoot(flags *pflag.FlagSet) string {
	// 1. Check explicit --project-dir
	if flags != nil {
		if projectDir, _ := flags.GetString("project-dir"); projectDir != "" && flags.Changed("project-dir") {
			abs, err := filepath.Abs(projectDir)
			if err == nil {
				return abs
			}
			return filepath.Clean(projectDir)
		}
	}

	// 2. Infer from --models-dir
	if flags != nil {
		if modelsDir, _ := flags.GetString("models-dir"); modelsDir != "" && flags.Changed("models-dir") {
			absModels, err := filepath.Abs(modelsDir)
			if err == nil {
				parent := filepath.Dir(absModels)

				// If parent has a config file, it's the project root
				if configExistsIn(parent) {
					return parent
				}

				// If folder is named "models", assume parent is root
				if filepath.Base(absModels) == "models" {
					return parent
				}
			}
		}
	}

	// 3. Search upward from CWD for leapsql.yaml
	if cwd, err := os.Getwd(); err == nil {
		if root := findProjectRootUpward(cwd); root != "" {
			return root
		}
	}

	// 4. Default to CWD
	cwd, _ := os.Getwd()
	if cwd == "" {
		cwd = "."
	}
	return cwd
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

	// Infer project root from flags before loading config
	// This enables the "anchor pattern" where --models-dir testdata/models
	// implies project root is testdata/
	projectRoot := inferProjectRoot(flags)

	// Track paths that were explicitly provided as flags (already relative to CWD).
	// These will be converted to absolute paths before the normal resolution step,
	// to prevent double-resolution when project root was inferred from them.
	var flagModelsDir, flagMacrosDir, flagSeedsDir, flagStatePath, flagDatabase string
	if flags != nil {
		if flags.Changed("models-dir") {
			if v, _ := flags.GetString("models-dir"); v != "" {
				flagModelsDir, _ = filepath.Abs(v)
			}
		}
		if flags.Changed("macros-dir") {
			if v, _ := flags.GetString("macros-dir"); v != "" {
				flagMacrosDir, _ = filepath.Abs(v)
			}
		}
		if flags.Changed("seeds-dir") {
			if v, _ := flags.GetString("seeds-dir"); v != "" {
				flagSeedsDir, _ = filepath.Abs(v)
			}
		}
		if flags.Changed("state") {
			if v, _ := flags.GetString("state"); v != "" {
				flagStatePath, _ = filepath.Abs(v)
			}
		}
		if flags.Changed("database") {
			if v, _ := flags.GetString("database"); v != "" {
				// Database path can be :memory: or a file path
				if v != ":memory:" {
					flagDatabase, _ = filepath.Abs(v)
				} else {
					flagDatabase = v
				}
			}
		}
	}

	// If an explicit config file is provided, use its directory as project root
	// (unless a more specific hint was given via flags)
	if cfgFile != "" && projectRoot == inferProjectRoot(nil) {
		// No flag-based inference happened, use config file's directory
		if absPath, err := filepath.Abs(cfgFile); err == nil {
			projectRoot = filepath.Dir(absPath)
		}
	}

	// 1. Load defaults
	if err := k.Load(confmap.Provider(map[string]interface{}{
		"models_dir":  intconfig.DefaultModelsDir,
		"seeds_dir":   intconfig.DefaultSeedsDir,
		"macros_dir":  intconfig.DefaultMacrosDir,
		"state_path":  DefaultStateFile,
		"environment": DefaultEnv,
		"verbose":     false,
		"output":      DefaultOutput,
	}, "."), nil); err != nil {
		return nil, fmt.Errorf("failed to load defaults: %w", err)
	}

	// 2. Find and load config file
	// Search in project root if no explicit config file provided
	if cfgFile == "" {
		// Look for config in inferred project root
		for _, name := range []string{"leapsql.yaml", "leapsql.yml"} {
			candidate := filepath.Join(projectRoot, name)
			if _, err := os.Stat(candidate); err == nil {
				cfgFile = candidate
				break
			}
		}
	}
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

			// EXPLICIT MAPPING: Bridge the gap between --state flag and state_path config key
			// The CLI uses --state for brevity, but the config struct uses state_path for clarity
			if key == "state" {
				return "state_path", posflag.FlagVal(flags, f)
			}

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

	// 6. Set project root and resolve relative paths
	// Use project root as base for all path resolution (not config file directory)
	// This implements the "anchor pattern" for intuitive path resolution
	cfg.ProjectRoot = projectRoot

	// For paths explicitly provided via flags, use the pre-computed absolute paths
	// (already computed relative to CWD at flag parse time).
	// For paths from config file or defaults, resolve relative to project root.
	if flagModelsDir != "" {
		cfg.ModelsDir = flagModelsDir
	} else {
		cfg.ModelsDir = resolvePathRelativeTo(cfg.ModelsDir, projectRoot)
	}
	if flagSeedsDir != "" {
		cfg.SeedsDir = flagSeedsDir
	} else {
		cfg.SeedsDir = resolvePathRelativeTo(cfg.SeedsDir, projectRoot)
	}
	if flagMacrosDir != "" {
		cfg.MacrosDir = flagMacrosDir
	} else {
		cfg.MacrosDir = resolvePathRelativeTo(cfg.MacrosDir, projectRoot)
	}
	if flagStatePath != "" {
		cfg.StatePath = flagStatePath
	} else {
		cfg.StatePath = resolvePathRelativeTo(cfg.StatePath, projectRoot)
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
		cfg.Target = &core.TargetConfig{
			Type:     "duckdb",
			Database: cfg.DatabasePath, // Use legacy database path
		}
	}

	// Apply defaults based on target type
	intconfig.ApplyTargetDefaults(cfg.Target)

	// Expand environment variables in target
	expandTargetEnvVars(cfg.Target)

	// For backward compatibility: sync DatabasePath with Target.Database
	// If --database flag was explicitly set, it takes precedence over config file
	if flagDatabase != "" {
		cfg.DatabasePath = flagDatabase
		cfg.Target.Database = flagDatabase
	} else if cfg.Target.Database == "" && cfg.DatabasePath != "" {
		cfg.Target.Database = cfg.DatabasePath
	} else if cfg.Target.Database != "" && cfg.DatabasePath == "" {
		cfg.DatabasePath = cfg.Target.Database
	}

	// Validate target configuration
	if err := intconfig.ValidateTarget(cfg.Target); err != nil {
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
func expandTargetEnvVars(t *core.TargetConfig) {
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
func MergeTargetConfig(base, override *core.TargetConfig) *core.TargetConfig {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	// Start with a copy of base
	merged := &core.TargetConfig{
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
