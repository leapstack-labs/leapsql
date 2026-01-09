// Package common provides shared types and utilities for UI features.
package common

// TreeNode represents a node in the explorer tree.
type TreeNode struct {
	Name     string
	Path     string
	Type     string // "folder" or "model"
	Children []TreeNode
}

// SidebarData holds data needed for the sidebar/shell rendering.
// This is the minimal data structure that avoids import cycles.
type SidebarData struct {
	ExplorerTree []TreeNode
	CurrentPath  string
	FullWidth    bool // true for graph, runs, query pages (no context panel)
}
