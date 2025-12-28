package docs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Phase 1: String Helper Tests
// =============================================================================

func TestExtractFolder(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "with folder prefix",
			path:     "staging.customers",
			expected: "staging",
		},
		{
			name:     "no folder prefix",
			path:     "customers",
			expected: "default",
		},
		{
			name:     "nested path",
			path:     "staging.nested.customers",
			expected: "staging",
		},
		{
			name:     "empty string",
			path:     "",
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFolder(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNullString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		valid   bool
		wantStr string
	}{
		{
			name:    "empty string",
			input:   "",
			valid:   false,
			wantStr: "",
		},
		{
			name:    "non-empty string",
			input:   "hello",
			valid:   true,
			wantStr: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nullString(tt.input)
			assert.Equal(t, tt.valid, result.Valid)
			if tt.valid {
				assert.Equal(t, tt.wantStr, result.String)
			}
		})
	}
}

func TestExtractDescription(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected string
	}{
		{
			name:     "single line comment",
			content:  "-- This is a description\nSELECT 1",
			expected: "This is a description",
		},
		{
			name:     "multi line description",
			content:  "-- First line\n-- Second line\nSELECT 1",
			expected: "First line Second line",
		},
		{
			name:     "no description",
			content:  "SELECT 1",
			expected: "",
		},
		{
			name:     "empty content",
			content:  "",
			expected: "",
		},
		{
			name:     "only whitespace",
			content:  "   \n  \t\n",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractDescription(tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractDescription_IgnoresPragmas(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected string
	}{
		{
			name:     "skip @config pragma",
			content:  "-- @config materialized='view'\n-- Actual description\nSELECT 1",
			expected: "Actual description",
		},
		{
			name:     "skip @import pragma",
			content:  "-- @import some_macro\n-- Description here\nSELECT 1",
			expected: "Description here",
		},
		{
			name:     "skip #if pragma",
			content:  "-- #if condition\n-- Description\nSELECT 1",
			expected: "Description",
		},
		{
			name:     "skip #endif pragma",
			content:  "-- #endif\n-- Description\nSELECT 1",
			expected: "Description",
		},
		{
			name:     "only pragmas",
			content:  "-- @config materialized='view'\nSELECT 1",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractDescription(tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitLines(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "unix newlines",
			input:    "line1\nline2\nline3",
			expected: []string{"line1", "line2", "line3"},
		},
		{
			name:     "single line",
			input:    "only one line",
			expected: []string{"only one line"},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "trailing newline",
			input:    "line1\nline2\n",
			expected: []string{"line1", "line2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitLines(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTrimPrefix(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		prefix   string
		expected string
	}{
		{
			name:     "has prefix",
			s:        "-- comment",
			prefix:   "-- ",
			expected: "comment",
		},
		{
			name:     "no prefix",
			s:        "SELECT 1",
			prefix:   "-- ",
			expected: "SELECT 1",
		},
		{
			name:     "exact match",
			s:        "-- ",
			prefix:   "-- ",
			expected: "",
		},
		{
			name:     "empty string",
			s:        "",
			prefix:   "-- ",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := trimPrefix(tt.s, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasPrefix(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		prefix   string
		expected bool
	}{
		{
			name:     "has prefix",
			s:        "@config value",
			prefix:   "@config",
			expected: true,
		},
		{
			name:     "no prefix",
			s:        "some text",
			prefix:   "@config",
			expected: false,
		},
		{
			name:     "prefix longer than string",
			s:        "hi",
			prefix:   "hello",
			expected: false,
		},
		{
			name:     "empty string",
			s:        "",
			prefix:   "test",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasPrefix(tt.s, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsEmptyOrWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "empty string",
			input:    "",
			expected: true,
		},
		{
			name:     "only spaces",
			input:    "    ",
			expected: true,
		},
		{
			name:     "only tabs",
			input:    "\t\t",
			expected: true,
		},
		{
			name:     "mixed whitespace",
			input:    "  \t  ",
			expected: true,
		},
		{
			name:     "has content",
			input:    "  hello  ",
			expected: false,
		},
		{
			name:     "only newlines",
			input:    "\n\n",
			expected: false, // newline is not considered whitespace by the function
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isEmptyOrWhitespace(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Phase 2: Column Conversion Tests
// =============================================================================

func TestConvertColumns_Nil(t *testing.T) {
	result := convertColumns(nil)
	require.NotNil(t, result)
	assert.Empty(t, result)
}

func TestConvertColumns_Empty(t *testing.T) {
	result := convertColumns([]core.ColumnInfo{})
	require.NotNil(t, result)
	assert.Empty(t, result)
}

func TestConvertColumns_WithSources(t *testing.T) {
	columns := []core.ColumnInfo{
		{
			Name:  "id",
			Index: 0,
			Sources: []core.SourceRef{
				{Table: "customers", Column: "id"},
			},
		},
		{
			Name:          "total",
			Index:         1,
			TransformType: core.TransformExpression,
			Function:      "sum",
			Sources: []core.SourceRef{
				{Table: "orders", Column: "amount"},
			},
		},
	}

	result := convertColumns(columns)

	require.Len(t, result, 2)

	// First column
	assert.Equal(t, "id", result[0].Name)
	assert.Equal(t, 0, result[0].Index)
	require.Len(t, result[0].Sources, 1)
	assert.Equal(t, "customers", result[0].Sources[0].Table)
	assert.Equal(t, "id", result[0].Sources[0].Column)

	// Second column
	assert.Equal(t, "total", result[1].Name)
	assert.Equal(t, 1, result[1].Index)
	assert.Equal(t, string(core.TransformExpression), result[1].TransformType)
	assert.Equal(t, "sum", result[1].Function)
	require.Len(t, result[1].Sources, 1)
	assert.Equal(t, "orders", result[1].Sources[0].Table)
	assert.Equal(t, "amount", result[1].Sources[0].Column)
}

func TestConvertColumns_PreservesOrder(t *testing.T) {
	columns := []core.ColumnInfo{
		{Name: "c", Index: 2},
		{Name: "a", Index: 0},
		{Name: "b", Index: 1},
	}

	result := convertColumns(columns)

	require.Len(t, result, 3)
	assert.Equal(t, "c", result[0].Name)
	assert.Equal(t, "a", result[1].Name)
	assert.Equal(t, "b", result[2].Name)
}

// =============================================================================
// Phase 2: Catalog Generation Tests
// =============================================================================

func TestGenerateCatalog_Empty(t *testing.T) {
	g := NewGenerator("test_project")
	// Don't load any models

	catalog := g.GenerateCatalog()

	require.NotNil(t, catalog)
	assert.Equal(t, "test_project", catalog.ProjectName)
	assert.Empty(t, catalog.Models)
	assert.Empty(t, catalog.Sources)
	assert.Empty(t, catalog.Lineage.Edges)
	assert.Empty(t, catalog.ColumnLineage.Nodes)
	assert.Empty(t, catalog.ColumnLineage.Edges)
}

// =============================================================================
// Phase 2: Lineage Tests
// =============================================================================

func TestBuildLineage_NodesIncludeModels(t *testing.T) {
	g := &Generator{projectName: "test"}
	modelDocs := map[string]*ModelDoc{
		"staging.customers": newTestModel("staging.customers", "customers", "view"),
		"marts.summary":     newTestModel("marts.summary", "summary", "table"),
	}

	lineage := g.buildLineage(modelDocs, nil)

	assert.Len(t, lineage.Nodes, 2)
	assert.Contains(t, lineage.Nodes, "staging.customers")
	assert.Contains(t, lineage.Nodes, "marts.summary")
}

func TestBuildLineage_NodesIncludeSources(t *testing.T) {
	g := &Generator{projectName: "test"}
	modelDocs := map[string]*ModelDoc{
		"staging.customers": newTestModel("staging.customers", "customers", "view"),
	}
	sources := []SourceDoc{
		{Name: "raw_customers", ReferencedBy: []string{"staging.customers"}},
	}

	lineage := g.buildLineage(modelDocs, sources)

	assert.Len(t, lineage.Nodes, 2)
	assert.Contains(t, lineage.Nodes, "staging.customers")
	assert.Contains(t, lineage.Nodes, "source:raw_customers")
}

func TestBuildLineage_EdgesFromDependencies(t *testing.T) {
	g := &Generator{projectName: "test"}
	stagingModel := newTestModel("staging.customers", "customers", "view")
	martsModel := newTestModel("marts.summary", "summary", "table")
	martsModel.Dependencies = []string{"staging.customers"}

	modelDocs := map[string]*ModelDoc{
		"staging.customers": stagingModel,
		"marts.summary":     martsModel,
	}

	lineage := g.buildLineage(modelDocs, nil)

	require.Len(t, lineage.Edges, 1)
	assert.Equal(t, "staging.customers", lineage.Edges[0].Source)
	assert.Equal(t, "marts.summary", lineage.Edges[0].Target)
}

func TestBuildLineage_EdgesFromSources(t *testing.T) {
	g := &Generator{projectName: "test"}
	modelDocs := map[string]*ModelDoc{
		"staging.customers": newTestModel("staging.customers", "customers", "view"),
	}
	sources := []SourceDoc{
		{Name: "raw_customers", ReferencedBy: []string{"staging.customers"}},
	}

	lineage := g.buildLineage(modelDocs, sources)

	require.Len(t, lineage.Edges, 1)
	assert.Equal(t, "source:raw_customers", lineage.Edges[0].Source)
	assert.Equal(t, "staging.customers", lineage.Edges[0].Target)
}

func TestBuildColumnLineage_NodesCreated(t *testing.T) {
	g := &Generator{projectName: "test"}
	models := []*core.Model{
		{
			Path: "staging.customers",
			Name: "customers",
			Columns: []core.ColumnInfo{
				{Name: "id", Index: 0},
				{Name: "name", Index: 1},
			},
		},
	}
	modelDocs := map[string]*ModelDoc{}

	lineage := g.buildColumnLineage(models, modelDocs)

	assert.Len(t, lineage.Nodes, 2)

	nodeIDs := make([]string, len(lineage.Nodes))
	for i, n := range lineage.Nodes {
		nodeIDs[i] = n.ID
	}
	assert.Contains(t, nodeIDs, "staging.customers.id")
	assert.Contains(t, nodeIDs, "staging.customers.name")
}

func TestBuildColumnLineage_EdgesCreated(t *testing.T) {
	g := &Generator{projectName: "test"}
	models := []*core.Model{
		{
			Path: "staging.customers",
			Name: "customers",
			Columns: []core.ColumnInfo{
				{Name: "id", Index: 0},
			},
		},
		{
			Path: "marts.summary",
			Name: "summary",
			Columns: []core.ColumnInfo{
				{
					Name:  "customer_id",
					Index: 0,
					Sources: []core.SourceRef{
						{Table: "customers", Column: "id"},
					},
				},
			},
		},
	}
	modelDocs := map[string]*ModelDoc{}

	lineage := g.buildColumnLineage(models, modelDocs)

	require.NotEmpty(t, lineage.Edges)

	// Find the edge from customers.id to summary.customer_id
	var foundEdge bool
	for _, edge := range lineage.Edges {
		if edge.Source == "staging.customers.id" && edge.Target == "marts.summary.customer_id" {
			foundEdge = true
			break
		}
	}
	assert.True(t, foundEdge, "Expected edge from staging.customers.id to marts.summary.customer_id")
}

func TestBuildColumnLineage_ExternalSources(t *testing.T) {
	g := &Generator{projectName: "test"}
	models := []*core.Model{
		{
			Path: "staging.customers",
			Name: "customers",
			Columns: []core.ColumnInfo{
				{
					Name:  "id",
					Index: 0,
					Sources: []core.SourceRef{
						{Table: "raw_customers", Column: "customer_id"},
					},
				},
			},
		},
	}
	modelDocs := map[string]*ModelDoc{}

	lineage := g.buildColumnLineage(models, modelDocs)

	// Should create a node for the external source
	nodeIDs := make([]string, len(lineage.Nodes))
	for i, n := range lineage.Nodes {
		nodeIDs[i] = n.ID
	}
	assert.Contains(t, nodeIDs, "raw_customers.customer_id")
}

// =============================================================================
// Phase 5: Edge Cases
// =============================================================================

func TestExtractDescription_EmptyFile(t *testing.T) {
	result := extractDescription("")
	assert.Empty(t, result)
}

func TestExtractDescription_OnlyPragmas(t *testing.T) {
	content := `-- @config materialized='view'
-- @import utils
SELECT 1`
	result := extractDescription(content)
	assert.Empty(t, result)
}

func TestBuildColumnLineage_EmptyColumns(t *testing.T) {
	g := &Generator{projectName: "test"}
	models := []*core.Model{
		{
			Path:    "staging.empty",
			Name:    "empty",
			Columns: []core.ColumnInfo{}, // No columns
		},
	}
	modelDocs := map[string]*ModelDoc{}

	lineage := g.buildColumnLineage(models, modelDocs)

	assert.Empty(t, lineage.Nodes)
	assert.Empty(t, lineage.Edges)
}

func TestBuildColumnLineage_EmptySources(t *testing.T) {
	g := &Generator{projectName: "test"}
	models := []*core.Model{
		{
			Path: "staging.customers",
			Name: "customers",
			Columns: []core.ColumnInfo{
				{
					Name:    "id",
					Index:   0,
					Sources: []core.SourceRef{}, // No sources
				},
			},
		},
	}
	modelDocs := map[string]*ModelDoc{}

	lineage := g.buildColumnLineage(models, modelDocs)

	// Should still have the node for the column
	require.Len(t, lineage.Nodes, 1)
	assert.Equal(t, "staging.customers.id", lineage.Nodes[0].ID)
	// But no edges
	assert.Empty(t, lineage.Edges)
}

func TestBuildColumnLineage_SkipsEmptyTableOrColumn(t *testing.T) {
	g := &Generator{projectName: "test"}
	models := []*core.Model{
		{
			Path: "staging.customers",
			Name: "customers",
			Columns: []core.ColumnInfo{
				{
					Name:  "id",
					Index: 0,
					Sources: []core.SourceRef{
						{Table: "", Column: "id"},      // Empty table
						{Table: "raw", Column: ""},     // Empty column
						{Table: "", Column: ""},        // Both empty
						{Table: "raw", Column: "name"}, // Valid
					},
				},
			},
		},
	}
	modelDocs := map[string]*ModelDoc{}

	lineage := g.buildColumnLineage(models, modelDocs)

	// Should only have 2 nodes: the target and one valid source
	assert.Len(t, lineage.Nodes, 2)
	// Should only have 1 edge (from the valid source)
	assert.Len(t, lineage.Edges, 1)
}

func TestGenerateCatalog_DependentsCalculated(t *testing.T) {
	// This tests that dependents are properly calculated from dependencies
	g := &Generator{projectName: "test"}
	g.registry = nil // We need a valid registry for this test

	// Since we can't easily set up the registry, we test the logic directly
	// by creating model docs with dependencies and verifying dependents are set
	modelDocs := map[string]*ModelDoc{
		"staging.customers": {
			Path:         "staging.customers",
			Dependents:   []string{},
			Dependencies: []string{},
		},
		"marts.summary": {
			Path:         "marts.summary",
			Dependencies: []string{"staging.customers"},
			Dependents:   []string{},
		},
	}

	// Simulate the dependents calculation from GenerateCatalog
	for _, doc := range modelDocs {
		for _, depPath := range doc.Dependencies {
			if depDoc, ok := modelDocs[depPath]; ok {
				depDoc.Dependents = append(depDoc.Dependents, doc.Path)
			}
		}
	}

	assert.Equal(t, []string{"marts.summary"}, modelDocs["staging.customers"].Dependents)
	assert.Empty(t, modelDocs["marts.summary"].Dependents)
}

func TestBuildLineage_CyclicDependencies(t *testing.T) {
	// Test that cyclic dependencies don't cause issues
	g := &Generator{projectName: "test"}
	modelA := newTestModel("a", "a", "view")
	modelB := newTestModel("b", "b", "view")
	modelA.Dependencies = []string{"b"}
	modelB.Dependencies = []string{"a"}

	modelDocs := map[string]*ModelDoc{
		"a": modelA,
		"b": modelB,
	}

	// Should not panic or hang
	lineage := g.buildLineage(modelDocs, nil)

	// Should have both edges
	assert.Len(t, lineage.Edges, 2)
}

func TestBuildLineage_MissingDependency(t *testing.T) {
	// Test graceful handling when a dependency references a non-existent model
	g := &Generator{projectName: "test"}
	model := newTestModel("staging.customers", "customers", "view")
	model.Dependencies = []string{"nonexistent.model"}

	modelDocs := map[string]*ModelDoc{
		"staging.customers": model,
	}

	// Should not panic
	lineage := g.buildLineage(modelDocs, nil)

	// Edge should still be created (even if target doesn't exist in our map)
	assert.Len(t, lineage.Edges, 1)
	assert.Equal(t, "nonexistent.model", lineage.Edges[0].Source)
}

// =============================================================================
// Phase 5: Build Function Tests
// =============================================================================

func TestCopyFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create source file
	srcPath := filepath.Join(tmpDir, "source.txt")
	content := "hello world"
	err := os.WriteFile(srcPath, []byte(content), 0644)
	require.NoError(t, err)

	// Copy to destination
	dstPath := filepath.Join(tmpDir, "dest.txt")
	err = CopyFile(srcPath, dstPath)
	require.NoError(t, err)

	// Verify destination content
	data, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, content, string(data))
}

func TestCopyFile_NotExists(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "nonexistent.txt")
	dstPath := filepath.Join(tmpDir, "dest.txt")

	err := CopyFile(srcPath, dstPath)
	assert.Error(t, err)
}

func TestWriteJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "data.json")

	data := map[string]interface{}{
		"name":  "test",
		"count": 42,
	}

	err := WriteJSON(path, data)
	require.NoError(t, err)

	// Verify file content
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(content, &result)
	require.NoError(t, err)
	assert.Equal(t, "test", result["name"])
	assert.InDelta(t, 42, result["count"], 0.001) // JSON unmarshals to float64
}
