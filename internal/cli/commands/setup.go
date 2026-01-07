package commands

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/leapstack-labs/leapsql/internal/cli/output"
	intconfig "github.com/leapstack-labs/leapsql/internal/config"
	"github.com/leapstack-labs/leapsql/internal/engine"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/spf13/cobra"
)

// CommandContext holds common dependencies for CLI commands.
type CommandContext struct {
	Cfg      *config.Config
	Logger   *slog.Logger
	Engine   *engine.Engine
	Renderer *output.Renderer
}

// NewCommandContext creates a CommandContext with engine and renderer.
// Returns the context and a cleanup function that must be called (typically via defer).
func NewCommandContext(cmd *cobra.Command) (*CommandContext, func(), error) {
	cfg := getConfig()
	logger := config.GetLogger(cmd.Context())

	eng, err := createEngine(cfg, logger)
	if err != nil {
		return nil, nil, err
	}

	mode := output.Mode(cfg.OutputFormat)
	r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

	cleanup := func() {
		_ = eng.Close()
	}

	return &CommandContext{
		Cfg:      cfg,
		Logger:   logger,
		Engine:   eng,
		Renderer: r,
	}, cleanup, nil
}

// NewCommandContextWithoutEngine creates a CommandContext without an engine.
// Useful for commands that don't need database access.
func NewCommandContextWithoutEngine(cmd *cobra.Command) *CommandContext {
	cfg := getConfig()
	logger := config.GetLogger(cmd.Context())
	mode := output.Mode(cfg.OutputFormat)
	r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

	return &CommandContext{
		Cfg:      cfg,
		Logger:   logger,
		Renderer: r,
	}
}

// Helper functions shared across commands

// getConfig returns the current configuration.
// It uses config.GetCurrentConfig() if available, otherwise falls back to environment variables.
func getConfig() *config.Config {
	if cfg := config.GetCurrentConfig(); cfg != nil {
		return cfg
	}

	// Fallback: read from environment with defaults
	modelsDir := getEnvOrDefault("LEAPSQL_MODELS_DIR", intconfig.DefaultModelsDir)
	seedsDir := getEnvOrDefault("LEAPSQL_SEEDS_DIR", intconfig.DefaultSeedsDir)
	macrosDir := getEnvOrDefault("LEAPSQL_MACROS_DIR", intconfig.DefaultMacrosDir)
	database := os.Getenv("LEAPSQL_DATABASE")
	statePath := getEnvOrDefault("LEAPSQL_STATE_PATH", config.DefaultStateFile)
	environment := getEnvOrDefault("LEAPSQL_ENVIRONMENT", config.DefaultEnv)
	verbose := os.Getenv("LEAPSQL_VERBOSE") == "true"
	outputFormat := os.Getenv("LEAPSQL_OUTPUT")

	return &config.Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		MacrosDir:    macrosDir,
		DatabasePath: database,
		StatePath:    statePath,
		Environment:  environment,
		Verbose:      verbose,
		OutputFormat: outputFormat,
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func createEngine(cfg *config.Config, logger *slog.Logger) (*engine.Engine, error) {
	// Ensure state directory exists
	stateDir := filepath.Dir(cfg.StatePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0750); err != nil {
			return nil, err
		}
	}

	// Build target info for template rendering
	var targetInfo *starctx.TargetInfo
	var adapterConfig *core.AdapterConfig

	if cfg.Target != nil {
		targetInfo = starctx.TargetInfoFromConfig(cfg.Target)
		adapterConfig = &core.AdapterConfig{
			Type:     cfg.Target.Type,
			Path:     cfg.Target.Database,
			Database: cfg.Target.Database,
			Schema:   cfg.Target.Schema,
			Host:     cfg.Target.Host,
			Port:     cfg.Target.Port,
			Username: cfg.Target.User,
			Password: cfg.Target.Password,
			Options:  cfg.Target.Options,
			Params:   cfg.Target.Params,
		}
	}

	engineCfg := engine.Config{
		ModelsDir:     cfg.ModelsDir,
		SeedsDir:      cfg.SeedsDir,
		MacrosDir:     cfg.MacrosDir,
		DatabasePath:  cfg.DatabasePath,
		StatePath:     cfg.StatePath,
		Environment:   cfg.Environment,
		Target:        targetInfo,
		AdapterConfig: adapterConfig,
		Logger:        logger,
	}

	return engine.New(engineCfg)
}
