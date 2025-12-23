package commands

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/spf13/cobra"
)

// GraphQuerier provides read-only access to DAG structure.
type GraphQuerier interface {
	GetParents(string) []string
	GetChildren(string) []string
	NodeCount() int
	EdgeCount() int
}

// NewDAGCommand creates the dag command.
func NewDAGCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dag",
		Short: "Show the dependency graph",
		Long: `Display the dependency graph (DAG) of all models.

Models are grouped by execution level, showing which models can run
in parallel and their dependency relationships.

Output adapts to environment:
  - Terminal: Styled output with colors
  - Piped/Scripted: Markdown format (agent-friendly)`,
		Example: `  # Show the DAG
  leapsql dag

  # Show DAG with verbose model info
  leapsql dag -v

  # Output as JSON
  leapsql dag --output json

  # Output as Markdown
  leapsql dag --output markdown`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDAG(cmd)
		},
	}

	return cmd
}

func runDAG(cmd *cobra.Command) error {
	cmdCtx, cleanup, err := NewCommandContext(cmd)
	if err != nil {
		return err
	}
	defer cleanup()

	eng := cmdCtx.Engine
	r := cmdCtx.Renderer

	// Discover models
	if _, err := eng.Discover(engine.DiscoveryOptions{}); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	graph := eng.GetGraph()

	// Get execution levels
	levels, err := graph.GetExecutionLevels()
	if err != nil {
		return fmt.Errorf("failed to get execution levels: %w", err)
	}

	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		return dagJSON(r, graph, levels)
	case output.ModeMarkdown:
		return dagMarkdown(r, graph, levels)
	default:
		return dagText(r, graph, levels)
	}
}

// dagText outputs DAG in styled text format.
func dagText(r *output.Renderer, graph GraphQuerier, levels [][]string) error {
	styles := r.Styles()

	r.Header(1, "Dependency Graph")

	for i, level := range levels {
		r.Println(styles.Header2.Render(fmt.Sprintf("Level %d:", i)))
		for _, model := range level {
			deps := graph.GetParents(model)
			children := graph.GetChildren(model)

			r.Printf("  %s\n", styles.ModelPath.Render(model))
			if len(deps) > 0 {
				r.Printf("    %s %s\n", styles.Muted.Render("depends on:"), strings.Join(deps, ", "))
			}
			if len(children) > 0 {
				r.Printf("    %s %s\n", styles.Muted.Render("used by:"), strings.Join(children, ", "))
			}
		}
		r.Println("")
	}

	r.Println(styles.Muted.Render(fmt.Sprintf("Total: %d models, %d dependencies", graph.NodeCount(), graph.EdgeCount())))

	return nil
}

// dagMarkdown outputs DAG in markdown format.
func dagMarkdown(r *output.Renderer, graph GraphQuerier, levels [][]string) error {
	r.Println(output.FormatHeader(1, "Dependency Graph"))
	r.Println("")

	for i, level := range levels {
		levelName := fmt.Sprintf("Level %d", i)
		if i == 0 {
			levelName = "Level 0 (Sources)"
		}
		r.Println(output.FormatHeader(2, levelName))

		for _, model := range level {
			deps := graph.GetParents(model)
			children := graph.GetChildren(model)

			r.Printf("- %s\n", model)
			if len(deps) > 0 {
				r.Printf("  - depends on: %s\n", strings.Join(deps, ", "))
			}
			if len(children) > 0 {
				r.Printf("  - used by: %s\n", strings.Join(children, ", "))
			}
		}
		r.Println("")
	}

	r.Println(output.FormatHeader(2, "Summary"))
	r.Println(output.FormatKeyValue("Total Models", fmt.Sprintf("%d", graph.NodeCount())))
	r.Println(output.FormatKeyValue("Total Dependencies", fmt.Sprintf("%d", graph.EdgeCount())))

	return nil
}

// dagJSON outputs DAG in JSON format.
func dagJSON(r *output.Renderer, graph GraphQuerier, levels [][]string) error {
	dagOutput := output.DAGOutput{
		Levels:      make([]output.DAGLevel, 0, len(levels)),
		TotalModels: graph.NodeCount(),
		TotalEdges:  graph.EdgeCount(),
	}

	for i, level := range levels {
		dagLevel := output.DAGLevel{
			Level:  i,
			Models: make([]output.DAGNode, 0, len(level)),
		}

		for _, model := range level {
			dagLevel.Models = append(dagLevel.Models, output.DAGNode{
				Path:      model,
				DependsOn: graph.GetParents(model),
				UsedBy:    graph.GetChildren(model),
			})
		}

		dagOutput.Levels = append(dagOutput.Levels, dagLevel)
	}

	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(dagOutput)
}
