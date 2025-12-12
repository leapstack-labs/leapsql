package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/spf13/cobra"
)

// LineageOptions holds options for the lineage command.
type LineageOptions struct {
	OutputFormat string
	Upstream     bool
	Downstream   bool
	Depth        int
}

// NewLineageCommand creates the lineage command.
func NewLineageCommand() *cobra.Command {
	opts := &LineageOptions{}

	cmd := &cobra.Command{
		Use:   "lineage <model>",
		Short: "Show lineage for a model",
		Long: `Display the upstream dependencies and downstream dependents of a model.

The lineage shows how data flows through your models, helping you understand
the impact of changes and debug data issues.`,
		Example: `  # Show full lineage for a model
  leapsql lineage staging.stg_customers

  # Show only upstream dependencies
  leapsql lineage staging.stg_customers --downstream=false

  # Show only downstream dependents
  leapsql lineage staging.stg_customers --upstream=false

  # Limit traversal depth
  leapsql lineage staging.stg_customers --depth 2

  # Output as JSON
  leapsql lineage staging.stg_customers --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLineage(cmd, args[0], opts)
		},
	}

	cmd.Flags().StringVarP(&opts.OutputFormat, "output", "o", "text", "Output format (text|json)")
	cmd.Flags().BoolVar(&opts.Upstream, "upstream", true, "Include upstream dependencies")
	cmd.Flags().BoolVar(&opts.Downstream, "downstream", true, "Include downstream dependents")
	cmd.Flags().IntVar(&opts.Depth, "depth", 0, "Max traversal depth (0 = unlimited)")

	return cmd
}

func runLineage(cmd *cobra.Command, modelPath string, opts *LineageOptions) error {
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	models := eng.GetModels()
	graph := eng.GetGraph()

	// Verify model exists
	if _, ok := models[modelPath]; !ok {
		// Check if it could be in the graph but not in models (external source)
		if _, exists := graph.GetNode(modelPath); !exists {
			return fmt.Errorf("model not found: %s", modelPath)
		}
	}

	if opts.OutputFormat == "json" {
		return lineageJSON(eng, modelPath, opts.Upstream, opts.Downstream, opts.Depth)
	}
	return lineageText(eng, modelPath, opts.Upstream, opts.Downstream, opts.Depth)
}

// lineageText outputs lineage in text format.
func lineageText(eng *engine.Engine, modelPath string, upstream, downstream bool, depth int) error {
	graph := eng.GetGraph()

	fmt.Printf("Lineage for: %s\n\n", modelPath)

	if upstream {
		upstreamNodes := getUpstreamWithDepth(graph, modelPath, depth)
		fmt.Printf("Upstream dependencies (%d):\n", len(upstreamNodes))
		for _, node := range upstreamNodes {
			fmt.Printf("  - %s\n", node)
		}
		fmt.Println()
	}

	if downstream {
		downstreamNodes := getDownstreamWithDepth(graph, modelPath, depth)
		fmt.Printf("Downstream dependents (%d):\n", len(downstreamNodes))
		for _, node := range downstreamNodes {
			fmt.Printf("  - %s\n", node)
		}
	}

	return nil
}

// lineageJSON outputs lineage in JSON format.
func lineageJSON(eng *engine.Engine, modelPath string, upstream, downstream bool, depth int) error {
	models := eng.GetModels()
	graph := eng.GetGraph()

	lineageOutput := output.LineageOutput{
		Root:  modelPath,
		Nodes: []output.LineageNode{},
		Edges: []output.LineageEdge{},
		Stats: output.LineageStats{},
	}

	// Collect all relevant nodes
	nodeSet := make(map[string]bool)
	nodeSet[modelPath] = true

	var upstreamNodes, downstreamNodes []string

	if upstream {
		upstreamNodes = getUpstreamWithDepth(graph, modelPath, depth)
		for _, n := range upstreamNodes {
			nodeSet[n] = true
		}
	}

	if downstream {
		downstreamNodes = getDownstreamWithDepth(graph, modelPath, depth)
		for _, n := range downstreamNodes {
			nodeSet[n] = true
		}
	}

	// Build nodes list
	for nodeID := range nodeSet {
		node := output.LineageNode{
			ID:   nodeID,
			Type: "model",
		}

		if m, ok := models[nodeID]; ok {
			node.Materialized = m.Materialized
			absPath, _ := filepath.Abs(m.FilePath)
			node.FilePath = &absPath
		} else {
			// External source (seed table, etc.)
			node.Type = "source"
			node.FilePath = nil
		}

		lineageOutput.Nodes = append(lineageOutput.Nodes, node)
	}

	// Build edges list - only edges between nodes in our set
	for nodeID := range nodeSet {
		// Add edges from parents
		for _, parent := range graph.GetParents(nodeID) {
			if nodeSet[parent] {
				lineageOutput.Edges = append(lineageOutput.Edges, output.LineageEdge{
					From: parent,
					To:   nodeID,
				})
			}
		}
	}

	// Stats
	lineageOutput.Stats.TotalNodes = len(lineageOutput.Nodes)
	lineageOutput.Stats.UpstreamCount = len(upstreamNodes)
	lineageOutput.Stats.DownstreamCount = len(downstreamNodes)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(lineageOutput)
}

// getUpstreamWithDepth returns upstream nodes with optional depth limit.
func getUpstreamWithDepth(graph *dag.Graph, nodeID string, maxDepth int) []string {
	if maxDepth == 0 {
		return graph.GetUpstreamNodes(nodeID)
	}

	visited := make(map[string]bool)
	var result []string

	var traverse func(id string, depth int)
	traverse = func(id string, depth int) {
		if depth > maxDepth {
			return
		}
		for _, parent := range graph.GetParents(id) {
			if !visited[parent] {
				visited[parent] = true
				result = append(result, parent)
				traverse(parent, depth+1)
			}
		}
	}

	traverse(nodeID, 1)
	return result
}

// getDownstreamWithDepth returns downstream nodes with optional depth limit.
func getDownstreamWithDepth(graph *dag.Graph, nodeID string, maxDepth int) []string {
	if maxDepth == 0 {
		result := graph.GetAffectedNodes([]string{nodeID})
		if len(result) > 1 {
			return result[1:] // Exclude self
		}
		return []string{}
	}

	visited := make(map[string]bool)
	var result []string

	var traverse func(id string, depth int)
	traverse = func(id string, depth int) {
		if depth > maxDepth {
			return
		}
		for _, child := range graph.GetChildren(id) {
			if !visited[child] {
				visited[child] = true
				result = append(result, child)
				traverse(child, depth+1)
			}
		}
	}

	traverse(nodeID, 1)
	return result
}
