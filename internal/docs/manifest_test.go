package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateManifest_EmptyCatalog(t *testing.T) {
	catalog := newTestCatalog()

	manifest := GenerateManifest(catalog)

	require.NotNil(t, manifest)
	assert.Equal(t, "test_project", manifest.ProjectName)
	assert.Equal(t, catalog.GeneratedAt, manifest.GeneratedAt)
	assert.Empty(t, manifest.NavTree)
	assert.Equal(t, Stats{
		ModelCount:  0,
		SourceCount: 0,
		ColumnCount: 0,
		FolderCount: 0,
		TableCount:  0,
		ViewCount:   0,
	}, manifest.Stats)
}

func TestGenerateManifest_SingleModel(t *testing.T) {
	model := newTestModel("staging.customers", "customers", "view")
	catalog := newTestCatalogWithModels(model)

	manifest := GenerateManifest(catalog)

	require.Len(t, manifest.NavTree, 1)
	assert.Equal(t, "staging", manifest.NavTree[0].Folder)
	require.Len(t, manifest.NavTree[0].Models, 1)
	assert.Equal(t, "customers", manifest.NavTree[0].Models[0].Name)
	assert.Equal(t, "staging.customers", manifest.NavTree[0].Models[0].Path)
	assert.Equal(t, "view", manifest.NavTree[0].Models[0].Materialized)
}

func TestGenerateManifest_GroupsByFolder(t *testing.T) {
	models := []*ModelDoc{
		newTestModel("staging.customers", "customers", "view"),
		newTestModel("staging.orders", "orders", "view"),
		newTestModel("marts.customer_summary", "customer_summary", "table"),
	}
	catalog := newTestCatalogWithModels(models...)

	manifest := GenerateManifest(catalog)

	require.Len(t, manifest.NavTree, 2)

	// Should be sorted alphabetically
	assert.Equal(t, "marts", manifest.NavTree[0].Folder)
	assert.Len(t, manifest.NavTree[0].Models, 1)

	assert.Equal(t, "staging", manifest.NavTree[1].Folder)
	assert.Len(t, manifest.NavTree[1].Models, 2)
}

func TestGenerateManifest_Stats(t *testing.T) {
	models := []*ModelDoc{
		newTestModelWithColumns("staging.customers", "customers", "view", []ColumnDoc{
			newTestColumn("id", 0),
			newTestColumn("name", 1),
		}),
		newTestModelWithColumns("staging.orders", "orders", "view", []ColumnDoc{
			newTestColumn("id", 0),
			newTestColumn("amount", 1),
			newTestColumn("customer_id", 2),
		}),
		newTestModelWithColumns("marts.customer_summary", "customer_summary", "table", []ColumnDoc{
			newTestColumn("id", 0),
			newTestColumn("order_count", 1),
		}),
	}
	catalog := newTestCatalogWithModels(models...)
	catalog.Sources = []SourceDoc{
		{Name: "raw_customers", ReferencedBy: []string{"staging.customers"}},
		{Name: "raw_orders", ReferencedBy: []string{"staging.orders"}},
	}

	manifest := GenerateManifest(catalog)

	assert.Equal(t, 3, manifest.Stats.ModelCount)
	assert.Equal(t, 2, manifest.Stats.SourceCount)
	assert.Equal(t, 7, manifest.Stats.ColumnCount) // 2 + 3 + 2
	assert.Equal(t, 2, manifest.Stats.FolderCount) // marts, staging
	assert.Equal(t, 1, manifest.Stats.TableCount)
	assert.Equal(t, 2, manifest.Stats.ViewCount)
}

func TestGenerateManifest_SortsAlphabetically(t *testing.T) {
	// Models in reverse alphabetical order
	models := []*ModelDoc{
		newTestModel("staging.zebra", "zebra", "view"),
		newTestModel("staging.apple", "apple", "view"),
		newTestModel("staging.mango", "mango", "view"),
	}
	catalog := newTestCatalogWithModels(models...)

	manifest := GenerateManifest(catalog)

	require.Len(t, manifest.NavTree, 1)
	require.Len(t, manifest.NavTree[0].Models, 3)

	// Should be sorted alphabetically
	assert.Equal(t, "apple", manifest.NavTree[0].Models[0].Name)
	assert.Equal(t, "mango", manifest.NavTree[0].Models[1].Name)
	assert.Equal(t, "zebra", manifest.NavTree[0].Models[2].Name)
}

func TestGenerateManifest_DefaultFolder(t *testing.T) {
	// Model without folder prefix (no dot in path)
	model := newTestModel("customers", "customers", "view")
	catalog := newTestCatalogWithModels(model)

	manifest := GenerateManifest(catalog)

	require.Len(t, manifest.NavTree, 1)
	assert.Equal(t, "default", manifest.NavTree[0].Folder)
}

func TestSortNavItems(t *testing.T) {
	tests := []struct {
		name     string
		input    []NavItem
		expected []NavItem
	}{
		{
			name:     "empty",
			input:    []NavItem{},
			expected: []NavItem{},
		},
		{
			name:     "single",
			input:    []NavItem{{Name: "a"}},
			expected: []NavItem{{Name: "a"}},
		},
		{
			name:     "already sorted",
			input:    []NavItem{{Name: "a"}, {Name: "b"}, {Name: "c"}},
			expected: []NavItem{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		},
		{
			name:     "reverse order",
			input:    []NavItem{{Name: "c"}, {Name: "b"}, {Name: "a"}},
			expected: []NavItem{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		},
		{
			name:     "random order",
			input:    []NavItem{{Name: "mango"}, {Name: "apple"}, {Name: "banana"}},
			expected: []NavItem{{Name: "apple"}, {Name: "banana"}, {Name: "mango"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying the test case
			items := make([]NavItem, len(tt.input))
			copy(items, tt.input)

			sortNavItems(items)

			assert.Equal(t, tt.expected, items)
		})
	}
}

func TestSortNavGroups(t *testing.T) {
	tests := []struct {
		name     string
		input    []NavGroup
		expected []NavGroup
	}{
		{
			name:     "empty",
			input:    []NavGroup{},
			expected: []NavGroup{},
		},
		{
			name:     "single",
			input:    []NavGroup{{Folder: "staging"}},
			expected: []NavGroup{{Folder: "staging"}},
		},
		{
			name:     "already sorted",
			input:    []NavGroup{{Folder: "marts"}, {Folder: "staging"}},
			expected: []NavGroup{{Folder: "marts"}, {Folder: "staging"}},
		},
		{
			name:     "reverse order",
			input:    []NavGroup{{Folder: "staging"}, {Folder: "marts"}},
			expected: []NavGroup{{Folder: "marts"}, {Folder: "staging"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := make([]NavGroup, len(tt.input))
			copy(groups, tt.input)

			sortNavGroups(groups)

			assert.Equal(t, tt.expected, groups)
		})
	}
}
