package commands

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/spf13/cobra"
)

// LineageOptions holds options for the lineage command.
type LineageOptions struct {
	Upstream   bool
	Downstream bool
	Depth      int
}

// NewLineageCommand creates the lineage command.
func NewLineageCommand() *cobra.Command {
	opts := &LineageOptions{}

	cmd := &cobra.Command{
		Use:   "lineage <model>",
		Short: "Show lineage for a model",
		Long: `Display the upstream dependencies and downstream dependents of a model.

The lineage shows how data flows through your models, helping you understand
the impact of changes and debug data issues.

Output adapts to environment:
  - Terminal: Styled tree with colored arrows
  - Piped/Scripted: Markdown format (agent-friendly)`,
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

	cmd.Flags().BoolVar(&opts.Upstream, "upstream", true, "Include upstream dependencies")
	cmd.Flags().BoolVar(&opts.Downstream, "downstream", true, "Include downstream dependents")
	cmd.Flags().IntVar(&opts.Depth, "depth", 0, "Max traversal depth (0 = unlimited)")

	return cmd
}

func runLineage(cmd *cobra.Command, modelPath string, opts *LineageOptions) error {
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

	models := eng.GetModels()
	graph := eng.GetGraph()

	// Verify model exists
	if _, ok := models[modelPath]; !ok {
		// Check if it could be in the graph but not in models (external source)
		if _, exists := graph.GetNode(modelPath); !exists {
			return fmt.Errorf("model not found: %s", modelPath)
		}
	}

	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		return lineageJSON(eng, r, modelPath, opts.Upstream, opts.Downstream, opts.Depth)
	case output.ModeMarkdown:
		return lineageMarkdown(eng, r, modelPath, opts.Upstream, opts.Downstream, opts.Depth)
	default:
		return lineageText(eng, r, modelPath, opts.Upstream, opts.Downstream, opts.Depth)
	}
}

// lineageText outputs lineage in styled text format.
func lineageText(eng *engine.Engine, r *output.Renderer, modelPath string, upstream, downstream bool, depth int) error {
	graph := eng.GetGraph()
	styles := r.Styles()

	r.Header(1, fmt.Sprintf("Lineage: %s", modelPath))

	if upstream {
		upstreamNodes := getUpstreamWithDepth(graph, modelPath, depth)
		r.Println(styles.Header2.Render(fmt.Sprintf("Upstream (%d):", len(upstreamNodes))))
		for _, node := range upstreamNodes {
			nodeType := getNodeType(eng, node)
			r.Printf("    %s %s %s\n",
				styles.Dependency.Render("\u2190"), // left arrow
				styles.ModelPath.Render(node),
				styles.Muted.Render("("+nodeType+")"))
		}
		r.Println("")
	}

	if downstream {
		downstreamNodes := getDownstreamWithDepth(graph, modelPath, depth)
		r.Println(styles.Header2.Render(fmt.Sprintf("Downstream (%d):", len(downstreamNodes))))
		for _, node := range downstreamNodes {
			r.Printf("    %s %s\n",
				styles.Success.Render("\u2192"), // right arrow
				styles.ModelPath.Render(node))
		}
	}

	return nil
}

// lineageMarkdown outputs lineage in markdown format.
func lineageMarkdown(eng *engine.Engine, r *output.Renderer, modelPath string, upstream, downstream bool, depth int) error {
	graph := eng.GetGraph()

	r.Println(output.FormatHeader(1, fmt.Sprintf("Lineage: %s", modelPath)))
	r.Println("")

	if upstream {
		upstreamNodes := getUpstreamWithDepth(graph, modelPath, depth)
		r.Println(output.FormatHeader(2, fmt.Sprintf("Upstream (%d)", len(upstreamNodes))))
		if len(upstreamNodes) > 0 {
			var items []string
			for _, node := range upstreamNodes {
				nodeType := getNodeType(eng, node)
				items = append(items, fmt.Sprintf("%s (%s)", node, nodeType))
			}
			r.Print(output.FormatList(items))
		} else {
			r.Println("*No upstream dependencies*")
		}
		r.Println("")
	}

	if downstream {
		downstreamNodes := getDownstreamWithDepth(graph, modelPath, depth)
		r.Println(output.FormatHeader(2, fmt.Sprintf("Downstream (%d)", len(downstreamNodes))))
		if len(downstreamNodes) > 0 {
			items := append([]string{}, downstreamNodes...)
			r.Print(output.FormatList(items))
		} else {
			r.Println("*No downstream dependents*")
		}
	}

	return nil
}

// lineageJSON outputs lineage in JSON format.
func lineageJSON(eng *engine.Engine, r *output.Renderer, modelPath string, upstream, downstream bool, depth int) error {
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

	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(lineageOutput)
}

// getNodeType returns the type of a node (model, source, seed).
func getNodeType(eng *engine.Engine, nodeID string) string {
	models := eng.GetModels()
	if m, ok := models[nodeID]; ok {
		return m.Materialized
	}
	// Check if it's a seed by looking at the naming convention
	if strings.HasPrefix(nodeID, "raw_") || strings.HasPrefix(nodeID, "seed_") {
		return "seed"
	}
	return "source"
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
