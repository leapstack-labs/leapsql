// Package graph provides DAG visualization handlers for the UI.
package graph

// nodeClass returns the CSS class for a node type.
func nodeClass(nodeType string) string {
	switch nodeType {
	case "table":
		return "graph-node--table"
	case "incremental":
		return "graph-node--incremental"
	case "source":
		return "graph-node--source"
	default:
		return "graph-node--view"
	}
}

// extractName extracts the name from a path like "staging.orders" -> "orders"
func extractName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '.' {
			return path[i+1:]
		}
	}
	return path
}
