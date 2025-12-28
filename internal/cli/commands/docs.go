package commands

import (
	"fmt"
	"os"

	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/leapstack-labs/leapsql/internal/docs"
	"github.com/spf13/cobra"
)

// DocsOptions holds options for docs commands.
type DocsOptions struct {
	ModelsPath  string
	OutputPath  string
	ProjectName string
	Port        int
	Theme       string
}

// NewDocsCommand creates the docs command with subcommands.
func NewDocsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "docs",
		Short: "Generate and serve documentation site",
		Long: `Generate static documentation or serve it locally.

The documentation includes model definitions, dependencies, lineage visualization,
and execution history.`,
	}

	// Add subcommands
	cmd.AddCommand(newDocsBuildCommand())
	cmd.AddCommand(newDocsServeCommand())
	cmd.AddCommand(newDocsDevCommand())

	return cmd
}

func newDocsBuildCommand() *cobra.Command {
	opts := &DocsOptions{}

	cmd := &cobra.Command{
		Use:   "build",
		Short: "Generate static documentation site",
		Long:  `Generate a static HTML documentation site for your models.`,
		Example: `  # Build docs with defaults
  leapsql docs build

  # Build to custom directory
  leapsql docs build --output ./public

  # Build with custom project name
  leapsql docs build --project "My Data Platform"

  # Build with a specific theme
  leapsql docs build --theme claude`,
		RunE: func(_ *cobra.Command, _ []string) error {
			// Get config inside RunE, not at command definition time
			cfg := getConfig()
			if opts.ModelsPath == "" {
				opts.ModelsPath = cfg.ModelsDir
			}
			// Get theme from config if not specified via flag
			if opts.Theme == "" && cfg.Docs != nil && cfg.Docs.Theme != "" {
				opts.Theme = cfg.Docs.Theme
			}
			return runDocsBuild(opts)
		},
	}

	cmd.Flags().StringVar(&opts.ModelsPath, "models", "", "Path to models directory (default: from config)")
	cmd.Flags().StringVar(&opts.OutputPath, "output", "./docs-site", "Output directory for generated site")
	cmd.Flags().StringVar(&opts.ProjectName, "project", "LeapSQL Project", "Project name for documentation")
	cmd.Flags().StringVar(&opts.Theme, "theme", "", "Theme name: vercel, claude, corporate (default: from config or 'vercel')")

	return cmd
}

func newDocsServeCommand() *cobra.Command {
	opts := &DocsOptions{}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Build and serve documentation locally",
		Long:  `Build the documentation and serve it on a local HTTP server.`,
		Example: `  # Serve docs on default port
  leapsql docs serve

  # Serve on custom port
  leapsql docs serve --port 3000

  # Serve with a specific theme
  leapsql docs serve --theme corporate`,
		RunE: func(_ *cobra.Command, _ []string) error {
			// Get config inside RunE, not at command definition time
			cfg := getConfig()
			if opts.ModelsPath == "" {
				opts.ModelsPath = cfg.ModelsDir
			}
			// Get theme from config if not specified via flag
			if opts.Theme == "" && cfg.Docs != nil && cfg.Docs.Theme != "" {
				opts.Theme = cfg.Docs.Theme
			}
			return runDocsServe(opts)
		},
	}

	cmd.Flags().StringVar(&opts.ModelsPath, "models", "", "Path to models directory (default: from config)")
	cmd.Flags().StringVar(&opts.OutputPath, "output", "./.leapsql-docs", "Output directory for generated site")
	cmd.Flags().StringVar(&opts.ProjectName, "project", "LeapSQL Project", "Project name for documentation")
	cmd.Flags().IntVar(&opts.Port, "port", 8080, "Port to serve on")
	cmd.Flags().StringVar(&opts.Theme, "theme", "", "Theme name: vercel, claude, corporate (default: from config or 'vercel')")

	return cmd
}

func runDocsBuild(opts *DocsOptions) error {
	// Validate models directory exists
	if _, err := os.Stat(opts.ModelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", opts.ModelsPath)
	}

	theme := opts.Theme
	if theme == "" {
		theme = "vercel"
	}

	fmt.Printf("Building documentation...\n")
	fmt.Printf("  Models:  %s\n", opts.ModelsPath)
	fmt.Printf("  Output:  %s\n", opts.OutputPath)
	fmt.Printf("  Project: %s\n", opts.ProjectName)
	fmt.Printf("  Theme:   %s\n", theme)
	fmt.Println()

	gen := docs.NewGenerator(opts.ProjectName)
	gen.SetTheme(opts.Theme)

	if err := gen.LoadModels(opts.ModelsPath); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	if err := gen.Build(opts.OutputPath); err != nil {
		return fmt.Errorf("failed to build docs: %w", err)
	}

	fmt.Printf("Documentation generated successfully!\n")
	fmt.Printf("Open %s/index.html in your browser\n", opts.OutputPath)

	return nil
}

func runDocsServe(opts *DocsOptions) error {
	// Validate models directory exists
	if _, err := os.Stat(opts.ModelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", opts.ModelsPath)
	}

	theme := opts.Theme
	if theme == "" {
		theme = "vercel"
	}

	fmt.Printf("Building documentation...\n")
	fmt.Printf("  Models:  %s\n", opts.ModelsPath)
	fmt.Printf("  Project: %s\n", opts.ProjectName)
	fmt.Printf("  Theme:   %s\n", theme)
	fmt.Println()

	gen := docs.NewGenerator(opts.ProjectName)
	gen.SetTheme(opts.Theme)

	if err := gen.LoadModels(opts.ModelsPath); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	if err := gen.Serve(opts.OutputPath, opts.Port); err != nil {
		return fmt.Errorf("failed to serve docs: %w", err)
	}

	return nil
}

func newDocsDevCommand() *cobra.Command {
	opts := &DocsOptions{}

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start development server with live reload",
		Long: `Start a development server that watches for changes and automatically rebuilds.

Changes to model files (.sql) and frontend source files (.tsx, .ts, .css) 
will trigger a rebuild, and connected browsers will automatically reload.`,
		Example: `  # Start dev server on default port
  leapsql docs dev

  # Start on custom port
  leapsql docs dev --port 3000

  # Start with a specific theme
  leapsql docs dev --theme claude`,
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg := getConfig()
			if opts.ModelsPath == "" {
				opts.ModelsPath = cfg.ModelsDir
			}
			// Get theme from config if not specified via flag
			if opts.Theme == "" && cfg.Docs != nil && cfg.Docs.Theme != "" {
				opts.Theme = cfg.Docs.Theme
			}
			return runDocsDev(opts)
		},
	}

	cmd.Flags().StringVar(&opts.ModelsPath, "models", "", "Path to models directory (default: from config)")
	cmd.Flags().StringVar(&opts.ProjectName, "project", "LeapSQL Project", "Project name for documentation")
	cmd.Flags().IntVar(&opts.Port, "port", 8080, "Port to serve on")
	cmd.Flags().StringVar(&opts.Theme, "theme", "", "Theme name: vercel, claude, corporate (default: from config or 'vercel')")

	return cmd
}

func runDocsDev(opts *DocsOptions) error {
	// Validate models directory exists
	if _, err := os.Stat(opts.ModelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", opts.ModelsPath)
	}

	return docs.ServeDev(opts.ProjectName, opts.ModelsPath, opts.Port, opts.Theme)
}

// Ensure config package is imported for getConfig usage
var _ = config.DefaultModelsDir
