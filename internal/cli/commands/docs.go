package commands

import (
	"fmt"
	"os"

	"github.com/leapstack-labs/leapsql/internal/docs"
	"github.com/spf13/cobra"
)

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

	return cmd
}

func newDocsBuildCommand() *cobra.Command {
	var outputPath string
	var projectName string
	var modelsPath string

	cmd := &cobra.Command{
		Use:   "build",
		Short: "Generate static documentation site",
		Long:  `Generate a static HTML documentation site for your models.`,
		Example: `  # Build docs with defaults
  leapsql docs build

  # Build to custom directory
  leapsql docs build --output ./public

  # Build with custom project name
  leapsql docs build --project "My Data Platform"`,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDocsBuild(modelsPath, outputPath, projectName)
		},
	}

	cfg := getConfig()
	cmd.Flags().StringVar(&modelsPath, "models", cfg.ModelsDir, "Path to models directory")
	cmd.Flags().StringVar(&outputPath, "output", "./docs-site", "Output directory for generated site")
	cmd.Flags().StringVar(&projectName, "project", "LeapSQL Project", "Project name for documentation")

	return cmd
}

func newDocsServeCommand() *cobra.Command {
	var outputPath string
	var projectName string
	var modelsPath string
	var port int

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Build and serve documentation locally",
		Long:  `Build the documentation and serve it on a local HTTP server.`,
		Example: `  # Serve docs on default port
  leapsql docs serve

  # Serve on custom port
  leapsql docs serve --port 3000`,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDocsServe(modelsPath, outputPath, projectName, port)
		},
	}

	cfg := getConfig()
	cmd.Flags().StringVar(&modelsPath, "models", cfg.ModelsDir, "Path to models directory")
	cmd.Flags().StringVar(&outputPath, "output", "./.leapsql-docs", "Output directory for generated site")
	cmd.Flags().StringVar(&projectName, "project", "LeapSQL Project", "Project name for documentation")
	cmd.Flags().IntVar(&port, "port", 8080, "Port to serve on")

	return cmd
}

func runDocsBuild(modelsPath, outputPath, projectName string) error {
	// Validate models directory exists
	if _, err := os.Stat(modelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", modelsPath)
	}

	fmt.Printf("Building documentation...\n")
	fmt.Printf("  Models:  %s\n", modelsPath)
	fmt.Printf("  Output:  %s\n", outputPath)
	fmt.Printf("  Project: %s\n", projectName)
	fmt.Println()

	gen := docs.NewGenerator(projectName)

	if err := gen.LoadModels(modelsPath); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	if err := gen.Build(outputPath); err != nil {
		return fmt.Errorf("failed to build docs: %w", err)
	}

	fmt.Printf("Documentation generated successfully!\n")
	fmt.Printf("Open %s/index.html in your browser\n", outputPath)

	return nil
}

func runDocsServe(modelsPath, outputPath, projectName string, port int) error {
	// Validate models directory exists
	if _, err := os.Stat(modelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", modelsPath)
	}

	fmt.Printf("Building documentation...\n")
	fmt.Printf("  Models:  %s\n", modelsPath)
	fmt.Printf("  Project: %s\n", projectName)
	fmt.Println()

	gen := docs.NewGenerator(projectName)

	if err := gen.LoadModels(modelsPath); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	if err := gen.Serve(outputPath, port); err != nil {
		return fmt.Errorf("failed to serve docs: %w", err)
	}

	return nil
}
