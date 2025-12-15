// Package dag provides directed acyclic graph operations for model dependencies.
// It supports cycle detection, topological sorting, and incremental change detection.
package dag

import (
	"fmt"
	"sort"
)

// Node represents a node in the DAG.
type Node struct {
	// ID is the unique identifier (model path)
	ID string
	// Data holds arbitrary node data
	Data interface{}
}

// Graph represents a directed acyclic graph.
type Graph struct {
	nodes   map[string]*Node
	edges   map[string][]string // parent -> children (dependents)
	parents map[string][]string // child -> parents (dependencies)
}

// NewGraph creates a new empty graph.
func NewGraph() *Graph {
	return &Graph{
		nodes:   make(map[string]*Node),
		edges:   make(map[string][]string),
		parents: make(map[string][]string),
	}
}

// Clear removes all nodes and edges from the graph.
func (g *Graph) Clear() {
	g.nodes = make(map[string]*Node)
	g.edges = make(map[string][]string)
	g.parents = make(map[string][]string)
}

// AddNode adds a node to the graph.
func (g *Graph) AddNode(id string, data interface{}) {
	if _, exists := g.nodes[id]; !exists {
		g.nodes[id] = &Node{ID: id, Data: data}
		g.edges[id] = []string{}
		g.parents[id] = []string{}
	} else {
		// Update data if node already exists
		g.nodes[id].Data = data
	}
}

// AddEdge adds a directed edge from parent to child (child depends on parent).
func (g *Graph) AddEdge(parentID, childID string) error {
	// Ensure both nodes exist
	if _, exists := g.nodes[parentID]; !exists {
		return fmt.Errorf("parent node %q does not exist", parentID)
	}
	if _, exists := g.nodes[childID]; !exists {
		return fmt.Errorf("child node %q does not exist", childID)
	}

	// Check for self-loops
	if parentID == childID {
		return fmt.Errorf("self-loop detected: %s", parentID)
	}

	// Add edge (avoid duplicates)
	if !contains(g.edges[parentID], childID) {
		g.edges[parentID] = append(g.edges[parentID], childID)
	}
	if !contains(g.parents[childID], parentID) {
		g.parents[childID] = append(g.parents[childID], parentID)
	}

	return nil
}

// GetNode returns a node by ID.
func (g *Graph) GetNode(id string) (*Node, bool) {
	node, exists := g.nodes[id]
	return node, exists
}

// GetParents returns the parents (dependencies) of a node.
func (g *Graph) GetParents(id string) []string {
	return g.parents[id]
}

// GetChildren returns the children (dependents) of a node.
func (g *Graph) GetChildren(id string) []string {
	return g.edges[id]
}

// GetAllNodes returns all nodes in the graph.
func (g *Graph) GetAllNodes() []*Node {
	nodes := make([]*Node, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, node)
	}
	// Sort for deterministic output
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

// NodeCount returns the number of nodes in the graph.
func (g *Graph) NodeCount() int {
	return len(g.nodes)
}

// EdgeCount returns the number of edges in the graph.
func (g *Graph) EdgeCount() int {
	count := 0
	for _, children := range g.edges {
		count += len(children)
	}
	return count
}

// HasCycle returns true if the graph contains a cycle, along with the cycle path.
func (g *Graph) HasCycle() (bool, []string) {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	path := make(map[string]string) // Track the path for error reporting

	var cyclePath []string

	var dfs func(id string) bool
	dfs = func(id string) bool {
		visited[id] = true
		recStack[id] = true

		for _, childID := range g.edges[id] {
			if !visited[childID] {
				path[childID] = id
				if dfs(childID) {
					return true
				}
			} else if recStack[childID] {
				// Found cycle, reconstruct path
				cyclePath = []string{childID}
				for curr := id; curr != childID; curr = path[curr] {
					cyclePath = append([]string{curr}, cyclePath...)
				}
				cyclePath = append([]string{childID}, cyclePath...)
				return true
			}
		}

		recStack[id] = false
		return false
	}

	for id := range g.nodes {
		if !visited[id] {
			if dfs(id) {
				return true, cyclePath
			}
		}
	}

	return false, nil
}

// TopologicalSort returns nodes in topological order (dependencies before dependents).
// Returns an error if the graph contains a cycle.
func (g *Graph) TopologicalSort() ([]*Node, error) {
	if hasCycle, cyclePath := g.HasCycle(); hasCycle {
		return nil, fmt.Errorf("cycle detected: %v", cyclePath)
	}

	visited := make(map[string]bool)
	var result []*Node

	var visit func(id string)
	visit = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true

		// Visit all parents first
		for _, parentID := range g.parents[id] {
			visit(parentID)
		}

		result = append(result, g.nodes[id])
	}

	// Process all nodes
	// Sort node IDs first for deterministic order
	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		visit(id)
	}

	return result, nil
}

// GetExecutionLevels returns nodes grouped by execution level.
// Nodes at level N can be executed in parallel after level N-1 completes.
// Level 0 contains nodes with no dependencies.
func (g *Graph) GetExecutionLevels() ([][]string, error) {
	if hasCycle, cyclePath := g.HasCycle(); hasCycle {
		return nil, fmt.Errorf("cycle detected: %v", cyclePath)
	}

	levels := [][]string{}
	assigned := make(map[string]int)

	// Calculate level for each node
	var getLevel func(id string) int
	getLevel = func(id string) int {
		if level, ok := assigned[id]; ok {
			return level
		}

		parents := g.parents[id]
		if len(parents) == 0 {
			assigned[id] = 0
			return 0
		}

		maxParentLevel := 0
		for _, parentID := range parents {
			parentLevel := getLevel(parentID)
			if parentLevel > maxParentLevel {
				maxParentLevel = parentLevel
			}
		}

		level := maxParentLevel + 1
		assigned[id] = level
		return level
	}

	// Calculate levels for all nodes
	maxLevel := 0
	for id := range g.nodes {
		level := getLevel(id)
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Group nodes by level
	for i := 0; i <= maxLevel; i++ {
		levels = append(levels, []string{})
	}
	for id, level := range assigned {
		levels[level] = append(levels[level], id)
	}

	// Sort each level for deterministic output
	for i := range levels {
		sort.Strings(levels[i])
	}

	return levels, nil
}

// GetAffectedNodes returns all nodes affected by changes to the given nodes.
// This includes the changed nodes and all their downstream dependents.
func (g *Graph) GetAffectedNodes(changedIDs []string) []string {
	affected := make(map[string]bool)

	var markAffected func(id string)
	markAffected = func(id string) {
		if affected[id] {
			return
		}
		affected[id] = true

		// Mark all children as affected
		for _, childID := range g.edges[id] {
			markAffected(childID)
		}
	}

	for _, id := range changedIDs {
		if _, exists := g.nodes[id]; exists {
			markAffected(id)
		}
	}

	result := make([]string, 0, len(affected))
	for id := range affected {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// GetUpstreamNodes returns all nodes upstream of the given node (its dependencies and their dependencies).
func (g *Graph) GetUpstreamNodes(id string) []string {
	upstream := make(map[string]bool)

	var markUpstream func(nodeID string)
	markUpstream = func(nodeID string) {
		for _, parentID := range g.parents[nodeID] {
			if !upstream[parentID] {
				upstream[parentID] = true
				markUpstream(parentID)
			}
		}
	}

	markUpstream(id)

	result := make([]string, 0, len(upstream))
	for nodeID := range upstream {
		result = append(result, nodeID)
	}
	sort.Strings(result)
	return result
}

// GetRoots returns nodes with no parents (no dependencies).
func (g *Graph) GetRoots() []string {
	var roots []string
	for id := range g.nodes {
		if len(g.parents[id]) == 0 {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)
	return roots
}

// GetLeaves returns nodes with no children (no dependents).
func (g *Graph) GetLeaves() []string {
	var leaves []string
	for id := range g.nodes {
		if len(g.edges[id]) == 0 {
			leaves = append(leaves, id)
		}
	}
	sort.Strings(leaves)
	return leaves
}

// Subgraph returns a new graph containing only the specified nodes and their edges.
func (g *Graph) Subgraph(nodeIDs []string) *Graph {
	subgraph := NewGraph()
	nodeSet := make(map[string]bool)

	for _, id := range nodeIDs {
		nodeSet[id] = true
		if node, exists := g.nodes[id]; exists {
			subgraph.AddNode(id, node.Data)
		}
	}

	// Add edges between included nodes
	for _, id := range nodeIDs {
		for _, childID := range g.edges[id] {
			if nodeSet[childID] {
				_ = subgraph.AddEdge(id, childID)
			}
		}
	}

	return subgraph
}

// contains checks if a slice contains a string.
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
