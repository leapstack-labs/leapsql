// Package docs provides manifest generation for instant shell rendering.
package docs

import (
	"time"
)

// Manifest is the minimal data needed for instant shell render.
// It is embedded in HTML and parsed immediately, before WASM loads.
type Manifest struct {
	ProjectName string     `json:"project_name"`
	GeneratedAt time.Time  `json:"generated_at"`
	NavTree     []NavGroup `json:"nav_tree"`
	Stats       Stats      `json:"stats"`
}

// NavGroup represents a folder of models in the navigation tree.
type NavGroup struct {
	Folder string    `json:"folder"`
	Models []NavItem `json:"models"`
}

// NavItem represents a single model in the navigation tree.
type NavItem struct {
	Name         string `json:"name"`
	Path         string `json:"path"`
	Materialized string `json:"materialized,omitempty"`
}

// SourceNavItem represents a source in the navigation.
type SourceNavItem struct {
	Name string `json:"name"`
}

// Stats contains counts for the overview page.
type Stats struct {
	ModelCount  int `json:"model_count"`
	SourceCount int `json:"source_count"`
	ColumnCount int `json:"column_count"`
	FolderCount int `json:"folder_count"`
	TableCount  int `json:"table_count"`
	ViewCount   int `json:"view_count"`
}

// GenerateManifest creates a Manifest from a Catalog.
// The manifest contains only the data needed for instant sidebar/nav rendering.
func GenerateManifest(catalog *Catalog) *Manifest {
	// Group models by folder
	folderModels := make(map[string][]NavItem)
	columnCount := 0

	tableCount := 0
	viewCount := 0

	for _, model := range catalog.Models {
		folder := extractFolder(model.Path)
		folderModels[folder] = append(folderModels[folder], NavItem{
			Name:         model.Name,
			Path:         model.Path,
			Materialized: model.Materialized,
		})
		columnCount += len(model.Columns)

		switch model.Materialized {
		case "table":
			tableCount++
		case "view":
			viewCount++
		}
	}

	// Build nav tree sorted by folder name
	navTree := make([]NavGroup, 0, len(folderModels))
	for folder, models := range folderModels {
		// Sort models by name within the folder
		sortNavItems(models)
		navTree = append(navTree, NavGroup{
			Folder: folder,
			Models: models,
		})
	}
	// Sort folders alphabetically
	sortNavGroups(navTree)

	return &Manifest{
		ProjectName: catalog.ProjectName,
		GeneratedAt: catalog.GeneratedAt,
		NavTree:     navTree,
		Stats: Stats{
			ModelCount:  len(catalog.Models),
			SourceCount: len(catalog.Sources),
			ColumnCount: columnCount,
			FolderCount: len(folderModels),
			TableCount:  tableCount,
			ViewCount:   viewCount,
		},
	}
}

// sortNavItems sorts NavItems by name (simple bubble sort for small lists).
func sortNavItems(items []NavItem) {
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].Name > items[j].Name {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
}

// sortNavGroups sorts NavGroups by folder name.
func sortNavGroups(groups []NavGroup) {
	for i := 0; i < len(groups); i++ {
		for j := i + 1; j < len(groups); j++ {
			if groups[i].Folder > groups[j].Folder {
				groups[i], groups[j] = groups[j], groups[i]
			}
		}
	}
}
