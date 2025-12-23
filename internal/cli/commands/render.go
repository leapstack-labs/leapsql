package commands

import (
	"encoding/json"
	"fmt"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/spf13/cobra"
)

// NewRenderCommand creates the render command.
func NewRenderCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "render <model>",
		Short: "Render SQL for a model with templates expanded",
		Long: `Render the final SQL for a model with all templates and macros expanded.

This is useful for debugging template issues and seeing the exact SQL
that will be executed.

Output adapts to environment:
  - Terminal: Plain SQL (suitable for syntax highlighting)
  - Piped/Scripted: Markdown with code block`,
		Example: `  # Render a model's SQL
  leapsql render staging.stg_customers

  # Render and save to file
  leapsql render staging.stg_customers > rendered.sql

  # Render as JSON
  leapsql render staging.stg_customers --output json

  # Render as Markdown (with code block)
  leapsql render staging.stg_customers --output markdown`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRender(cmd, args[0])
		},
	}

	return cmd
}

func runRender(cmd *cobra.Command, modelPath string) error {
	cmdCtx, cleanup, err := NewCommandContext(cmd)
	if err != nil {
		return err
	}
	defer cleanup()

	eng := cmdCtx.Engine
	r := cmdCtx.Renderer

	if _, err := eng.Discover(engine.DiscoveryOptions{}); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	sql, err := eng.RenderModel(modelPath)
	if err != nil {
		return fmt.Errorf("failed to render model: %w", err)
	}

	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		renderOutput := output.RenderOutput{
			Model: modelPath,
			SQL:   sql,
		}
		enc := json.NewEncoder(r.Writer())
		enc.SetIndent("", "  ")
		return enc.Encode(renderOutput)
	case output.ModeMarkdown:
		r.Println(output.FormatHeader(1, fmt.Sprintf("Rendered SQL: %s", modelPath)))
		r.Println("")
		r.Println(output.FormatCodeBlock("sql", sql))
	default:
		// Text mode: just output the SQL directly
		r.Println(sql)
	}

	return nil
}
