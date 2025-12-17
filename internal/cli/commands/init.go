package commands

import (
	"fmt"
	"os"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/spf13/cobra"
)

// NewInitCommand creates the init command.
func NewInitCommand() *cobra.Command {
	var force bool
	var example bool

	cmd := &cobra.Command{
		Use:   "init [directory]",
		Short: "Initialize a new LeapSQL project",
		Long: `Initialize a new LeapSQL project with default directory structure and configuration.

This creates:
  - models/ directory for SQL models
  - seeds/ directory for seed data CSV files
  - macros/ directory for Starlark macros
  - leapsql.yaml configuration file

Use --example to create a full working demo project with sample data, 
models (staging + marts), and macros demonstrating best practices.`,
		Example: `  # Initialize in current directory
  leapsql init

  # Initialize with a full working example
  leapsql init --example

  # Initialize in a new directory
  leapsql init my-project --example

  # Force overwrite existing config
  leapsql init --force`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) > 0 {
				dir = args[0]
			}

			// Create renderer
			cfg := getConfig()
			mode := output.Mode(cfg.OutputFormat)
			r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

			if example {
				return runInitExample(r, dir, force)
			}
			return runInit(r, dir, force)
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing configuration")
	cmd.Flags().BoolVar(&example, "example", false, "Create a full example project with seeds, models, and macros")

	return cmd
}

func runInit(r *output.Renderer, dir string, force bool) error {
	// Create directory if specified and doesn't exist
	if dir != "." {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Check if config already exists
	configPath := dir + "/leapsql.yaml"
	if _, err := os.Stat(configPath); err == nil && !force {
		return fmt.Errorf("leapsql.yaml already exists. Use --force to overwrite")
	}

	// Copy minimal template
	if err := copyTemplate("minimal", dir, force); err != nil {
		return fmt.Errorf("failed to initialize project: %w", err)
	}

	// List created files
	files, _ := listTemplateFiles("minimal")
	for _, f := range files {
		r.StatusLine(f, "success", "")
	}

	r.Println("")
	r.Success("LeapSQL project initialized!")
	r.Println("")
	r.Println("Next steps:")
	r.Println("  1. Add your seed data to seeds/")
	r.Println("  2. Create SQL models in models/")
	r.Println("  3. Run 'leapsql run' to execute models")
	r.Println("  4. Run 'leapsql list' to see all models")

	return nil
}

func runInitExample(r *output.Renderer, dir string, force bool) error {
	// Create directory if specified and doesn't exist
	if dir != "." {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Check if config already exists
	configPath := dir + "/leapsql.yaml"
	if _, err := os.Stat(configPath); err == nil && !force {
		return fmt.Errorf("leapsql.yaml already exists. Use --force to overwrite")
	}

	// Copy example template
	if err := copyTemplate("example", dir, force); err != nil {
		return fmt.Errorf("failed to initialize project: %w", err)
	}

	// List created files by category
	files, _ := listTemplateFiles("example")
	groups := groupTemplateFiles(files)

	// Display files by category
	r.Header(2, "Configuration")
	for _, f := range groups["config"] {
		r.StatusLine(f, "success", "")
	}

	r.Println("")
	r.Header(2, "Seeds")
	for _, f := range groups["seeds"] {
		r.StatusLine(f, "success", "")
	}

	r.Println("")
	r.Header(2, "Models")
	for _, f := range groups["models"] {
		r.StatusLine(f, "success", "")
	}

	r.Println("")
	r.Header(2, "Macros")
	for _, f := range groups["macros"] {
		r.StatusLine(f, "success", "")
	}

	r.Println("")
	r.Success("LeapSQL project initialized with example data!")
	r.Println("")
	r.Println("Next steps:")
	r.Println("  leapsql seed     Load CSV data into DuckDB")
	r.Println("  leapsql run      Execute all models in dependency order")
	r.Println("  leapsql list     View models and dependencies")
	r.Println("  leapsql dag      Visualize the dependency graph")

	return nil
}
