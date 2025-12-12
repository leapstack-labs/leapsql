package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewRenderCommand creates the render command.
func NewRenderCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "render <model>",
		Short: "Render SQL for a model with templates expanded",
		Long: `Render the final SQL for a model with all templates and macros expanded.

This is useful for debugging template issues and seeing the exact SQL
that will be executed.`,
		Example: `  # Render a model's SQL
  leapsql render staging.stg_customers

  # Render and save to file
  leapsql render staging.stg_customers > rendered.sql`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRender(args[0])
		},
	}

	return cmd
}

func runRender(modelPath string) error {
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	sql, err := eng.RenderModel(modelPath)
	if err != nil {
		return fmt.Errorf("failed to render model: %w", err)
	}

	fmt.Println(sql)
	return nil
}
