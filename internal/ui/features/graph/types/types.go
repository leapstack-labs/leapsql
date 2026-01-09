// Package types provides shared types for the graph feature.
package types //nolint:revive // intentional: imported with alias graphtypes

// GraphViewData holds data for the DAG visualization.
type GraphViewData struct {
	Nodes []GraphNode
	Edges []GraphEdge
}

// GraphNode represents a node in the graph.
type GraphNode struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Type  string `json:"type"` // "view", "table", "incremental", "source"
}

// GraphEdge represents an edge between two nodes.
type GraphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
}
