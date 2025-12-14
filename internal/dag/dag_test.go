package dag

import (
	"testing"
)

func TestGraph_AddNodeAndEdge(t *testing.T) {
	g := NewGraph()

	g.AddNode("a", "node A")
	g.AddNode("b", "node B")
	g.AddNode("c", "node C")

	if g.NodeCount() != 3 {
		t.Errorf("expected 3 nodes, got %d", g.NodeCount())
	}

	// b depends on a
	if err := g.AddEdge("a", "b"); err != nil {
		t.Errorf("failed to add edge: %v", err)
	}
	// c depends on b
	if err := g.AddEdge("b", "c"); err != nil {
		t.Errorf("failed to add edge: %v", err)
	}

	if g.EdgeCount() != 2 {
		t.Errorf("expected 2 edges, got %d", g.EdgeCount())
	}
}

func TestGraph_AddEdge_Errors(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(g *Graph)
		from   string
		to     string
		errMsg string
	}{
		{
			name:   "nonexistent child node",
			setup:  func(g *Graph) { g.AddNode("a", nil) },
			from:   "a",
			to:     "nonexistent",
			errMsg: "expected error for nonexistent child node",
		},
		{
			name:   "nonexistent parent node",
			setup:  func(g *Graph) { g.AddNode("a", nil) },
			from:   "nonexistent",
			to:     "a",
			errMsg: "expected error for nonexistent parent node",
		},
		{
			name:   "self loop",
			setup:  func(g *Graph) { g.AddNode("a", nil) },
			from:   "a",
			to:     "a",
			errMsg: "expected error for self-loop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGraph()
			tt.setup(g)

			err := g.AddEdge(tt.from, tt.to)
			if err == nil {
				t.Error(tt.errMsg)
			}
		})
	}
}

func TestGraph_GetParentsAndChildren(t *testing.T) {
	g := NewGraph()
	g.AddNode("a", nil)
	g.AddNode("b", nil)
	g.AddNode("c", nil)

	// b depends on a, c depends on both a and b
	g.AddEdge("a", "b")
	g.AddEdge("a", "c")
	g.AddEdge("b", "c")

	parents := g.GetParents("c")
	if len(parents) != 2 {
		t.Errorf("expected c to have 2 parents, got %d", len(parents))
	}

	children := g.GetChildren("a")
	if len(children) != 2 {
		t.Errorf("expected a to have 2 children, got %d", len(children))
	}
}

func TestGraph_HasCycle(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(g *Graph)
		wantCycle bool
	}{
		{
			name: "no cycle",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddEdge("a", "b")
				g.AddEdge("b", "c")
			},
			wantCycle: false,
		},
		{
			name: "with cycle",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddEdge("a", "b")
				g.AddEdge("b", "c")
				g.AddEdge("c", "a") // Creates cycle
			},
			wantCycle: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGraph()
			tt.setup(g)

			hasCycle, path := g.HasCycle()
			if hasCycle != tt.wantCycle {
				t.Errorf("expected hasCycle=%v, got %v (path: %v)", tt.wantCycle, hasCycle, path)
			}
			if tt.wantCycle && len(path) == 0 {
				t.Error("expected cycle path to be non-empty")
			}
		})
	}
}

func TestGraph_TopologicalSort(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(g *Graph)
		wantNodes int
		wantErr   bool
		verify    func(t *testing.T, sorted []*Node)
	}{
		{
			name: "simple chain",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddEdge("a", "b")
				g.AddEdge("b", "c")
			},
			wantNodes: 3,
			verify: func(t *testing.T, sorted []*Node) {
				positions := make(map[string]int)
				for i, node := range sorted {
					positions[node.ID] = i
				}
				if positions["a"] >= positions["b"] {
					t.Error("a should come before b")
				}
				if positions["b"] >= positions["c"] {
					t.Error("b should come before c")
				}
			},
		},
		{
			name: "diamond dependency",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddNode("d", nil)
				g.AddEdge("a", "b")
				g.AddEdge("a", "c")
				g.AddEdge("b", "d")
				g.AddEdge("c", "d")
			},
			wantNodes: 4,
			verify: func(t *testing.T, sorted []*Node) {
				positions := make(map[string]int)
				for i, node := range sorted {
					positions[node.ID] = i
				}
				if positions["a"] != 0 {
					t.Error("a should be first")
				}
				if positions["d"] != 3 {
					t.Error("d should be last")
				}
				if positions["b"] <= positions["a"] || positions["b"] >= positions["d"] {
					t.Error("b should be between a and d")
				}
				if positions["c"] <= positions["a"] || positions["c"] >= positions["d"] {
					t.Error("c should be between a and d")
				}
			},
		},
		{
			name: "with cycle",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddEdge("a", "b")
				g.AddEdge("b", "a") // Cycle
			},
			wantErr: true,
		},
		{
			name: "disconnected components",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddNode("d", nil)
				g.AddEdge("a", "b")
				g.AddEdge("c", "d")
			},
			wantNodes: 4,
			verify: func(t *testing.T, sorted []*Node) {
				positions := make(map[string]int)
				for i, node := range sorted {
					positions[node.ID] = i
				}
				if positions["a"] >= positions["b"] {
					t.Error("a should come before b")
				}
				if positions["c"] >= positions["d"] {
					t.Error("c should come before d")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGraph()
			tt.setup(g)

			sorted, err := g.TopologicalSort()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error for cyclic graph")
				}
				return
			}

			if err != nil {
				t.Fatalf("failed to sort: %v", err)
			}
			if len(sorted) != tt.wantNodes {
				t.Fatalf("expected %d nodes, got %d", tt.wantNodes, len(sorted))
			}
			if tt.verify != nil {
				tt.verify(t, sorted)
			}
		})
	}
}

func TestGraph_GetExecutionLevels(t *testing.T) {
	g := NewGraph()
	g.AddNode("raw1", nil)
	g.AddNode("raw2", nil)
	g.AddNode("staging1", nil)
	g.AddNode("staging2", nil)
	g.AddNode("mart", nil)

	// staging1 depends on raw1
	// staging2 depends on raw2
	// mart depends on both staging1 and staging2
	g.AddEdge("raw1", "staging1")
	g.AddEdge("raw2", "staging2")
	g.AddEdge("staging1", "mart")
	g.AddEdge("staging2", "mart")

	levels, err := g.GetExecutionLevels()
	if err != nil {
		t.Fatalf("failed to get levels: %v", err)
	}

	if len(levels) != 3 {
		t.Fatalf("expected 3 levels, got %d", len(levels))
	}

	// Level 0: raw1, raw2 (no dependencies)
	if len(levels[0]) != 2 {
		t.Errorf("expected 2 nodes at level 0, got %d", len(levels[0]))
	}

	// Level 1: staging1, staging2
	if len(levels[1]) != 2 {
		t.Errorf("expected 2 nodes at level 1, got %d", len(levels[1]))
	}

	// Level 2: mart
	if len(levels[2]) != 1 || levels[2][0] != "mart" {
		t.Errorf("expected [mart] at level 2, got %v", levels[2])
	}
}

func TestGraph_GetAffectedNodes(t *testing.T) {
	g := NewGraph()
	g.AddNode("a", nil)
	g.AddNode("b", nil)
	g.AddNode("c", nil)
	g.AddNode("d", nil)

	// b depends on a, c depends on b, d is independent
	g.AddEdge("a", "b")
	g.AddEdge("b", "c")

	affected := g.GetAffectedNodes([]string{"a"})
	if len(affected) != 3 {
		t.Errorf("expected 3 affected nodes, got %d: %v", len(affected), affected)
	}

	// Check that a, b, c are affected
	affectedSet := make(map[string]bool)
	for _, id := range affected {
		affectedSet[id] = true
	}
	if !affectedSet["a"] || !affectedSet["b"] || !affectedSet["c"] {
		t.Error("expected a, b, c to be affected")
	}
	if affectedSet["d"] {
		t.Error("d should not be affected")
	}
}

func TestGraph_GetUpstreamNodes(t *testing.T) {
	g := NewGraph()
	g.AddNode("a", nil)
	g.AddNode("b", nil)
	g.AddNode("c", nil)
	g.AddNode("d", nil)

	// c depends on a and b, d depends on c
	g.AddEdge("a", "c")
	g.AddEdge("b", "c")
	g.AddEdge("c", "d")

	upstream := g.GetUpstreamNodes("d")
	if len(upstream) != 3 {
		t.Errorf("expected 3 upstream nodes, got %d: %v", len(upstream), upstream)
	}
}

func TestGraph_GetRootsAndLeaves(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(g *Graph)
		wantRoots  int
		wantLeaves int
	}{
		{
			name: "diamond",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddEdge("a", "b")
				g.AddEdge("a", "c")
			},
			wantRoots:  1,
			wantLeaves: 2,
		},
		{
			name: "merge",
			setup: func(g *Graph) {
				g.AddNode("a", nil)
				g.AddNode("b", nil)
				g.AddNode("c", nil)
				g.AddEdge("a", "c")
				g.AddEdge("b", "c")
			},
			wantRoots:  2,
			wantLeaves: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGraph()
			tt.setup(g)

			roots := g.GetRoots()
			if len(roots) != tt.wantRoots {
				t.Errorf("expected %d roots, got %d", tt.wantRoots, len(roots))
			}

			leaves := g.GetLeaves()
			if len(leaves) != tt.wantLeaves {
				t.Errorf("expected %d leaves, got %d", tt.wantLeaves, len(leaves))
			}
		})
	}
}

func TestGraph_Subgraph(t *testing.T) {
	g := NewGraph()
	g.AddNode("a", "A")
	g.AddNode("b", "B")
	g.AddNode("c", "C")
	g.AddNode("d", "D")

	g.AddEdge("a", "b")
	g.AddEdge("b", "c")
	g.AddEdge("c", "d")

	// Create subgraph with only b and c
	sub := g.Subgraph([]string{"b", "c"})

	if sub.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", sub.NodeCount())
	}
	if sub.EdgeCount() != 1 {
		t.Errorf("expected 1 edge, got %d", sub.EdgeCount())
	}

	// Verify the edge exists
	children := sub.GetChildren("b")
	if len(children) != 1 || children[0] != "c" {
		t.Error("expected edge from b to c")
	}
}

func TestGraph_DuplicateEdges(t *testing.T) {
	g := NewGraph()
	g.AddNode("a", nil)
	g.AddNode("b", nil)

	// Add same edge twice
	g.AddEdge("a", "b")
	g.AddEdge("a", "b")

	if g.EdgeCount() != 1 {
		t.Errorf("expected 1 edge (no duplicates), got %d", g.EdgeCount())
	}
}
