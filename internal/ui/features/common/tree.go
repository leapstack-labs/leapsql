// Package common provides shared utilities for UI features.
package common

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/ui/features/common/components"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// BuildExplorerTree groups models into a tree structure by folder.
func BuildExplorerTree(models []*core.PersistedModel) []components.TreeNode {
	folders := make(map[string]*components.TreeNode)

	for _, m := range models {
		folder := ExtractFolder(m.Path)

		if _, ok := folders[folder]; !ok {
			folders[folder] = &components.TreeNode{
				Name:     folder,
				Path:     folder,
				Type:     "folder",
				Children: []components.TreeNode{},
			}
		}

		folders[folder].Children = append(folders[folder].Children, components.TreeNode{
			Name: m.Name,
			Path: m.Path,
			Type: "model",
		})
	}

	// Convert map to sorted slice
	result := make([]components.TreeNode, 0, len(folders))
	for _, node := range folders {
		// Sort children by name
		sort.Slice(node.Children, func(i, j int) bool {
			return node.Children[i].Name < node.Children[j].Name
		})
		result = append(result, *node)
	}

	// Sort folders by name
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// ExtractFolder extracts the folder name from a model path.
// e.g., "staging.customers" -> "staging"
// e.g., "marts.finance.revenue" -> "marts/finance"
func ExtractFolder(modelPath string) string {
	parts := strings.Split(modelPath, ".")
	if len(parts) <= 1 {
		return "models"
	}
	return filepath.Join(parts[:len(parts)-1]...)
}
