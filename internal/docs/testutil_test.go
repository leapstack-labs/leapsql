package docs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestCatalog creates an empty test catalog.
func newTestCatalog() *Catalog {
	return &Catalog{
		GeneratedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		ProjectName: "test_project",
		Models:      []*ModelDoc{},
		Sources:     []SourceDoc{},
		Lineage:     LineageDoc{Nodes: []string{}, Edges: []LineageEdge{}},
		ColumnLineage: ColumnLineageDoc{
			Nodes: []ColumnLineageNode{},
			Edges: []ColumnLineageEdge{},
		},
	}
}

// newTestModel creates a test model with the given parameters.
func newTestModel(path, name, materialized string) *ModelDoc {
	return &ModelDoc{
		ID:           path,
		Name:         name,
		Path:         path,
		Materialized: materialized,
		SQL:          "SELECT 1",
		FilePath:     "models/" + name + ".sql",
		Sources:      []string{},
		Dependencies: []string{},
		Dependents:   []string{},
		Columns:      []ColumnDoc{},
		UpdatedAt:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}
}

// newTestModelWithColumns creates a test model with columns.
func newTestModelWithColumns(path, name, materialized string, columns []ColumnDoc) *ModelDoc {
	m := newTestModel(path, name, materialized)
	m.Columns = columns
	return m
}

// newTestCatalogWithModels creates a catalog with the given models.
func newTestCatalogWithModels(models ...*ModelDoc) *Catalog {
	catalog := newTestCatalog()
	catalog.Models = models
	return catalog
}

// setupTestDB creates an in-memory database with schema initialized.
func setupTestDB(t *testing.T) *MetadataDB {
	t.Helper()

	db, err := OpenMemoryDB()
	require.NoError(t, err)

	err = db.InitSchema()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

// newTestColumn creates a test column.
func newTestColumn(name string, index int) ColumnDoc {
	return ColumnDoc{
		Name:    name,
		Index:   index,
		Sources: []SourceRef{},
	}
}

// newTestColumnWithSources creates a test column with source references.
func newTestColumnWithSources(name string, index int, sources ...SourceRef) ColumnDoc {
	return ColumnDoc{
		Name:    name,
		Index:   index,
		Sources: sources,
	}
}

// newTestSourceRef creates a test source reference.
func newTestSourceRef(table, column string) SourceRef {
	return SourceRef{
		Table:  table,
		Column: column,
	}
}
