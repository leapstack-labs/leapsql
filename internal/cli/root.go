// Package cli provides the command-line interface for LeapSQL.
package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/leapstack-labs/leapsql/internal/adapter"
	"github.com/leapstack-labs/leapsql/internal/cli/commands"
	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/spf13/cobra"
)

var (
	cfgFile    string
	targetFlag string
	cfg        *config.Config
)

// Version information (set at build time).
var (
	Version   = "0.1.0"
	BuildDate = "unknown"
	GitCommit = "unknown"
)

// configKey is used to store config in context.
type configKey struct{}

// rendererKey is used to store renderer in context.
type rendererKey struct{}

// NewRootCmd creates and returns the root command.
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "leapsql",
		Short: "LeapSQL - Data Transformation Engine",
		Long: `LeapSQL is a SQL-based data transformation engine built with Go and DuckDB.

It allows you to define SQL models with dependencies, templating, and macros,
then execute them in the correct order with state tracking and lineage.`,
		Version: Version,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Skip config loading for help and completion commands
			if cmd.Name() == "help" || cmd.Name() == "completion" || cmd.Name() == "__complete" {
				return nil
			}

			// Load configuration with optional target override and CLI flags
			var err error
			cfg, err = config.LoadConfigWithTarget(cfgFile, targetFlag, cmd.Root().PersistentFlags())
			if err != nil {
				return err
			}

			// Store config in context
			ctx := context.WithValue(cmd.Context(), configKey{}, cfg)

			// Create and store renderer based on output mode
			mode := output.Mode(cfg.OutputFormat)
			renderer := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)
			ctx = context.WithValue(ctx, rendererKey{}, renderer)
			cmd.SetContext(ctx)

			// Print config file used (if verbose)
			if cfg.Verbose {
				if configFile := config.GetConfigFileUsed(); configFile != "" {
					fmt.Fprintf(os.Stderr, "Using config file: %s\n", configFile)
				}
				if targetFlag != "" {
					fmt.Fprintf(os.Stderr, "Using target: %s\n", targetFlag)
				}
			}

			return nil
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Set version template
	rootCmd.SetVersionTemplate(`{{.Name}} {{.Version}}
Built with Go and DuckDB
`)

	// Global persistent flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: ./leapsql.yaml)")
	rootCmd.PersistentFlags().StringVarP(&targetFlag, "target", "t", "", "Target environment to use (e.g., dev, staging, prod)")
	rootCmd.PersistentFlags().String("models-dir", "", "Path to models directory")
	rootCmd.PersistentFlags().String("seeds-dir", "", "Path to seeds directory")
	rootCmd.PersistentFlags().String("macros-dir", "", "Path to macros directory")
	rootCmd.PersistentFlags().String("database", "", "Path to DuckDB database (empty for in-memory)")
	rootCmd.PersistentFlags().String("state", "", "Path to state database")
	rootCmd.PersistentFlags().String("env", "", "Environment name")
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "Verbose output")
	rootCmd.PersistentFlags().StringP("output", "o", "", "Output format (auto|text|markdown|json)")

	// Register completion for output flag
	_ = rootCmd.RegisterFlagCompletionFunc("output", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"auto", "text", "markdown", "json"}, cobra.ShellCompDirectiveNoFileComp
	})

	// Register completion for target flag
	_ = rootCmd.RegisterFlagCompletionFunc("target", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		// Return common environment names
		return []string{"dev", "staging", "prod"}, cobra.ShellCompDirectiveNoFileComp
	})

	// Add subcommands
	rootCmd.AddCommand(commands.NewVersionCommand(Version))
	rootCmd.AddCommand(commands.NewRunCommand())
	rootCmd.AddCommand(commands.NewListCommand())
	rootCmd.AddCommand(commands.NewLineageCommand())
	rootCmd.AddCommand(commands.NewRenderCommand())
	rootCmd.AddCommand(commands.NewSeedCommand())
	rootCmd.AddCommand(commands.NewDAGCommand())
	rootCmd.AddCommand(commands.NewDocsCommand())
	rootCmd.AddCommand(commands.NewDiscoverCommand())
	rootCmd.AddCommand(commands.NewLSPCommand())
	rootCmd.AddCommand(commands.NewInitCommand())
	rootCmd.AddCommand(NewCompletionCommand())

	return rootCmd
}

// Execute runs the root command.
func Execute() error {
	rootCmd := NewRootCmd()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return err
	}
	return nil
}

// GetConfig retrieves the config from the command context.
func GetConfig(ctx context.Context) *config.Config {
	if c, ok := ctx.Value(configKey{}).(*config.Config); ok {
		return c
	}
	// Return default config if none in context
	return &config.Config{
		ModelsDir:   config.DefaultModelsDir,
		SeedsDir:    config.DefaultSeedsDir,
		MacrosDir:   config.DefaultMacrosDir,
		StatePath:   config.DefaultStateFile,
		Environment: config.DefaultEnv,
	}
}

// GetRenderer retrieves the renderer from the command context.
func GetRenderer(ctx context.Context) *output.Renderer {
	if r, ok := ctx.Value(rendererKey{}).(*output.Renderer); ok {
		return r
	}
	// Return default renderer if none in context
	return output.NewRenderer(os.Stdout, os.Stderr, output.ModeAuto)
}

// CreateEngine creates an engine from the current configuration.
func CreateEngine(cfg *config.Config) (*engine.Engine, error) {
	// Ensure state directory exists
	stateDir := filepath.Dir(cfg.StatePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0750); err != nil {
			return nil, fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	// Build target info for template rendering
	var targetInfo *starctx.TargetInfo
	if cfg.Target != nil {
		targetInfo = &starctx.TargetInfo{
			Type:     cfg.Target.Type,
			Schema:   cfg.Target.Schema,
			Database: cfg.Target.Database,
		}
	}

	// Build adapter config from target
	var adapterConfig *adapter.Config
	if cfg.Target != nil {
		adapterConfig = &adapter.Config{
			Type:     cfg.Target.Type,
			Path:     cfg.Target.Database,
			Database: cfg.Target.Database,
			Schema:   cfg.Target.Schema,
			Host:     cfg.Target.Host,
			Port:     cfg.Target.Port,
			Username: cfg.Target.User,
			Password: cfg.Target.Password,
			Options:  cfg.Target.Options,
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
	}

	return engine.New(engineCfg)
}

// NewCompletionCommand creates the completion command.
func NewCompletionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate shell completion scripts",
		Long: `Generate shell completion scripts for LeapSQL.

To load completions:

Bash:
  $ source <(leapsql completion bash)
  
  # To load completions for each session, execute once:
  # Linux:
  $ leapsql completion bash > /etc/bash_completion.d/leapsql
  # macOS:
  $ leapsql completion bash > $(brew --prefix)/etc/bash_completion.d/leapsql

Zsh:
  # If shell completion is not already enabled in your environment,
  # you will need to enable it. Execute the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
  
  # To load completions for each session, execute once:
  $ leapsql completion zsh > "${fpath[1]}/_leapsql"
  
  # You will need to start a new shell for this setup to take effect.

Fish:
  $ leapsql completion fish | source
  
  # To load completions for each session, execute once:
  $ leapsql completion fish > ~/.config/fish/completions/leapsql.fish

PowerShell:
  PS> leapsql completion powershell | Out-String | Invoke-Expression
  
  # To load completions for every new session, run:
  PS> leapsql completion powershell > leapsql.ps1
  # and source this file from your PowerShell profile.
`,
		DisableFlagsInUseLine: true,
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
			}
			return nil
		},
	}
	return cmd
}
