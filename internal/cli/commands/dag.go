package commands

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// NewDAGCommand creates the dag command.
func NewDAGCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dag",
		Short: "Show the dependency graph",
		Long: `Display the dependency graph (DAG) of all models.

Models are grouped by execution level, showing which models can run
in parallel and their dependency relationships.`,
		Example: `  # Show the DAG
  leapsql dag

  # Show DAG with verbose model info
  leapsql dag -v`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDAG()
		},
	}

	return cmd
}

func runDAG() error {
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	// Discover models
	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	graph := eng.GetGraph()

	// Get execution levels
	levels, err := graph.GetExecutionLevels()
	if err != nil {
		return fmt.Errorf("failed to get execution levels: %w", err)
	}

	fmt.Println("Dependency Graph (execution levels):")
	fmt.Println()

	for i, level := range levels {
		fmt.Printf("Level %d:\n", i)
		for _, model := range level {
			deps := graph.GetParents(model)
			children := graph.GetChildren(model)

			fmt.Printf("  %s\n", model)
			if len(deps) > 0 {
				fmt.Printf("    depends on: %s\n", strings.Join(deps, ", "))
			}
			if len(children) > 0 {
				fmt.Printf("    used by: %s\n", strings.Join(children, ", "))
			}
		}
		fmt.Println()
	}

	fmt.Printf("Total: %d models, %d dependencies\n", graph.NodeCount(), graph.EdgeCount())

	return nil
}
