package docs

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Database Infrastructure Tests
// =============================================================================

func TestOpenTestMemoryDB(t *testing.T) {
	db, err := openTestMemoryDB()
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	// Verify connection works
	_, err = db.DB().ExecContext(context.Background(), "SELECT 1")
	require.NoError(t, err)
}

func TestTestMetadataDB_Close(t *testing.T) {
	db, err := openTestMemoryDB()
	require.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)
}

func TestInitSchema(t *testing.T) {
	db := setupTestDB(t)

	ctx := context.Background()
	// Schema should already be initialized by setupTestDB
	// Verify by querying the schema
	rows, err := db.DB().QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		require.NoError(t, err)
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())

	assert.Contains(t, tables, "models")
	assert.Contains(t, tables, "sources")
	assert.Contains(t, tables, "dependencies")
	assert.Contains(t, tables, "columns")
	assert.Contains(t, tables, "column_sources")
	assert.Contains(t, tables, "catalog_meta")
	assert.Contains(t, tables, "lineage_edges")
	assert.Contains(t, tables, "column_lineage_nodes")
	assert.Contains(t, tables, "column_lineage_edges")
}

func TestInitSchema_FTS5Created(t *testing.T) {
	db := setupTestDB(t)

	ctx := context.Background()
	// Check for FTS5 virtual table
	var count int
	err := db.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='models_fts'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "FTS5 virtual table should exist")
}

// =============================================================================
// Population Tests
// =============================================================================

func TestPopulateFromCatalog_Models(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModelWithColumns("staging.customers", "customers", "view", []ColumnDoc{
			newTestColumn("id", 0),
			newTestColumn("name", 1),
		}),
	)
	catalog.Models[0].Description = "Test description"
	catalog.Models[0].UniqueKey = "id"

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query the model
	var path, name, folder, materialized, sqlContent string
	var uniqueKey, description *string
	err = db.DB().QueryRowContext(ctx,
		"SELECT path, name, folder, materialized, unique_key, sql_content, description FROM models WHERE path = ?",
		"staging.customers",
	).Scan(&path, &name, &folder, &materialized, &uniqueKey, &sqlContent, &description)
	require.NoError(t, err)

	assert.Equal(t, "staging.customers", path)
	assert.Equal(t, "customers", name)
	assert.Equal(t, "staging", folder)
	assert.Equal(t, "view", materialized)
	assert.NotNil(t, uniqueKey)
	assert.Equal(t, "id", *uniqueKey)
	assert.Equal(t, "SELECT 1", sqlContent)
	assert.NotNil(t, description)
	assert.Equal(t, "Test description", *description)
}

func TestPopulateFromCatalog_Dependencies(t *testing.T) {
	db := setupTestDB(t)
	model := newTestModel("marts.summary", "summary", "table")
	model.Dependencies = []string{"staging.customers", "staging.orders"}
	catalog := newTestCatalogWithModels(model)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query dependencies
	rows, err := db.DB().QueryContext(ctx,
		"SELECT parent_path FROM dependencies WHERE model_path = ? ORDER BY parent_path",
		"marts.summary",
	)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var deps []string
	for rows.Next() {
		var dep string
		err := rows.Scan(&dep)
		require.NoError(t, err)
		deps = append(deps, dep)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []string{"staging.customers", "staging.orders"}, deps)
}

func TestPopulateFromCatalog_Dependents(t *testing.T) {
	db := setupTestDB(t)
	model := newTestModel("staging.customers", "customers", "view")
	model.Dependents = []string{"marts.summary", "marts.analytics"}
	catalog := newTestCatalogWithModels(model)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query dependents
	rows, err := db.DB().QueryContext(ctx,
		"SELECT dependent_path FROM dependents WHERE model_path = ? ORDER BY dependent_path",
		"staging.customers",
	)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var dependents []string
	for rows.Next() {
		var dep string
		err := rows.Scan(&dep)
		require.NoError(t, err)
		dependents = append(dependents, dep)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []string{"marts.analytics", "marts.summary"}, dependents)
}

func TestPopulateFromCatalog_Sources(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalog()
	catalog.Sources = []SourceDoc{
		{Name: "raw_customers", ReferencedBy: []string{"staging.customers"}},
		{Name: "raw_orders", ReferencedBy: []string{"staging.orders"}},
	}

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query sources
	var count int
	err = db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM sources").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Query source_refs
	err = db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM source_refs").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestPopulateFromCatalog_Columns(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModelWithColumns("staging.customers", "customers", "view", []ColumnDoc{
			newTestColumn("id", 0),
			newTestColumnWithSources("customer_id", 1, newTestSourceRef("raw_customers", "id")),
		}),
	)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query columns
	rows, err := db.DB().QueryContext(ctx,
		"SELECT name, idx FROM columns WHERE model_path = ? ORDER BY idx",
		"staging.customers",
	)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var columns []struct {
		Name  string
		Index int
	}
	for rows.Next() {
		var col struct {
			Name  string
			Index int
		}
		err := rows.Scan(&col.Name, &col.Index)
		require.NoError(t, err)
		columns = append(columns, col)
	}
	require.NoError(t, rows.Err())

	require.Len(t, columns, 2)
	assert.Equal(t, "id", columns[0].Name)
	assert.Equal(t, 0, columns[0].Index)
	assert.Equal(t, "customer_id", columns[1].Name)
	assert.Equal(t, 1, columns[1].Index)

	// Query column_sources
	var sourceTable, sourceColumn string
	err = db.DB().QueryRowContext(ctx,
		"SELECT source_table, source_column FROM column_sources WHERE model_path = ? AND column_name = ?",
		"staging.customers", "customer_id",
	).Scan(&sourceTable, &sourceColumn)
	require.NoError(t, err)
	assert.Equal(t, "raw_customers", sourceTable)
	assert.Equal(t, "id", sourceColumn)
}

func TestPopulateFromCatalog_LineageEdges(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalog()
	catalog.Lineage = LineageDoc{
		Nodes: []string{"staging.customers", "marts.summary"},
		Edges: []LineageEdge{
			{Source: "staging.customers", Target: "marts.summary"},
		},
	}

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query lineage edges
	var source, target string
	err = db.DB().QueryRowContext(ctx, "SELECT source_node, target_node FROM lineage_edges").Scan(&source, &target)
	require.NoError(t, err)
	assert.Equal(t, "staging.customers", source)
	assert.Equal(t, "marts.summary", target)
}

// Note: TestPopulateFromCatalog_ColumnLineage removed - column lineage is now
// queried directly from state.db views, not populated from Catalog.

func TestPopulateFromCatalog_CatalogMeta(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalog()
	catalog.ProjectName = "my_project"
	catalog.GeneratedAt = time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query catalog_meta
	var projectName, generatedAt string
	err = db.DB().QueryRowContext(ctx, "SELECT value FROM catalog_meta WHERE key = 'project_name'").Scan(&projectName)
	require.NoError(t, err)
	assert.Equal(t, "my_project", projectName)

	err = db.DB().QueryRowContext(ctx, "SELECT value FROM catalog_meta WHERE key = 'generated_at'").Scan(&generatedAt)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15T10:30:00Z", generatedAt)
}

// =============================================================================
// Round-Trip Verification Tests
// =============================================================================

func TestDatabaseRoundTrip_Models(t *testing.T) {
	db := setupTestDB(t)
	original := newTestModelWithColumns("staging.customers", "customers", "view", []ColumnDoc{
		newTestColumn("id", 0),
		newTestColumn("name", 1),
	})
	original.Description = "A customer model"
	original.SQL = "SELECT id, name FROM raw_customers"
	catalog := newTestCatalogWithModels(original)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query back
	rows, err := db.DB().QueryContext(ctx, `
		SELECT path, name, folder, materialized, sql_content, description 
		FROM models WHERE path = ?
	`, "staging.customers")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	require.True(t, rows.Next())
	var path, name, folder, materialized, sql string
	var description *string
	err = rows.Scan(&path, &name, &folder, &materialized, &sql, &description)
	require.NoError(t, err)
	require.NoError(t, rows.Err())

	assert.Equal(t, original.Path, path)
	assert.Equal(t, original.Name, name)
	assert.Equal(t, "staging", folder)
	assert.Equal(t, original.Materialized, materialized)
	assert.Equal(t, original.SQL, sql)
	require.NotNil(t, description)
	assert.Equal(t, original.Description, *description)
}

func TestDatabaseRoundTrip_Lineage(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalog()
	catalog.Lineage = LineageDoc{
		Nodes: []string{"a", "b", "c"},
		Edges: []LineageEdge{
			{Source: "a", Target: "b"},
			{Source: "b", Target: "c"},
		},
	}

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Query all edges
	rows, err := db.DB().QueryContext(ctx, "SELECT source_node, target_node FROM lineage_edges ORDER BY source_node")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var edges []LineageEdge
	for rows.Next() {
		var e LineageEdge
		err := rows.Scan(&e.Source, &e.Target)
		require.NoError(t, err)
		edges = append(edges, e)
	}
	require.NoError(t, rows.Err())

	require.Len(t, edges, 2)
	assert.Equal(t, "a", edges[0].Source)
	assert.Equal(t, "b", edges[0].Target)
	assert.Equal(t, "b", edges[1].Source)
	assert.Equal(t, "c", edges[1].Target)
}

// =============================================================================
// FTS5 Search Tests
// =============================================================================

func TestFTS5Search_ByName(t *testing.T) {
	db := setupTestDB(t)

	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
		newTestModel("staging.orders", "orders", "view"),
		newTestModel("marts.customer_summary", "customer_summary", "table"),
	)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Search by name
	rows, err := db.DB().QueryContext(ctx, `
		SELECT m.path FROM models m
		JOIN models_fts fts ON m.rowid = fts.rowid
		WHERE models_fts MATCH 'customer*'
		ORDER BY m.path
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var paths []string
	for rows.Next() {
		var path string
		err := rows.Scan(&path)
		require.NoError(t, err)
		paths = append(paths, path)
	}
	require.NoError(t, rows.Err())

	assert.Len(t, paths, 2)
	assert.Contains(t, paths, "staging.customers")
	assert.Contains(t, paths, "marts.customer_summary")
}

func TestFTS5Search_BySQL(t *testing.T) {
	db := setupTestDB(t)

	model := newTestModel("staging.orders", "orders", "view")
	model.SQL = "SELECT * FROM raw_orders WHERE amount > 100"
	catalog := newTestCatalogWithModels(model)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Search by SQL content
	rows, err := db.DB().QueryContext(ctx, `
		SELECT m.path FROM models m
		JOIN models_fts fts ON m.rowid = fts.rowid
		WHERE models_fts MATCH 'amount'
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var paths []string
	for rows.Next() {
		var path string
		err := rows.Scan(&path)
		require.NoError(t, err)
		paths = append(paths, path)
	}
	require.NoError(t, rows.Err())

	require.Len(t, paths, 1)
	assert.Equal(t, "staging.orders", paths[0])
}

func TestFTS5Search_NoResults(t *testing.T) {
	db := setupTestDB(t)

	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
	)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Search for non-existent term
	rows, err := db.DB().QueryContext(ctx, `
		SELECT m.path FROM models m
		JOIN models_fts fts ON m.rowid = fts.rowid
		WHERE models_fts MATCH 'nonexistent'
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, count)
}

// =============================================================================
// Production Function Tests
// =============================================================================

func TestCopyFromState(t *testing.T) {
	// Create a source database with test schema and data
	srcDB := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
		newTestModel("marts.summary", "summary", "table"),
	)
	err := srcDB.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	// Write it to a temp file (simulating state.db)
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "state.db")

	// Use VACUUM INTO to write to file
	_, err = srcDB.DB().ExecContext(context.Background(), "VACUUM INTO '"+srcPath+"'")
	require.NoError(t, err)

	// Now test CopyFromState
	dstPath := filepath.Join(tmpDir, "metadata.db")
	err = CopyFromState(srcPath, dstPath)
	require.NoError(t, err)

	// Verify the copy exists and is readable
	_, err = os.Stat(dstPath)
	require.NoError(t, err)

	// Open and verify content
	dstDB, err := openTestDBFromFile(dstPath)
	require.NoError(t, err)
	defer func() { _ = dstDB.Close() }()

	ctx := context.Background()
	var count int
	err = dstDB.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM models").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// openTestDBFromFile opens a file-based database for testing.
func openTestDBFromFile(path string) (*TestMetadataDB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	return &TestMetadataDB{db: db}, nil
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestPopulateFromCatalog_EmptyColumns(t *testing.T) {
	db := setupTestDB(t)
	model := newTestModel("staging.customers", "customers", "view")
	model.Columns = []ColumnDoc{} // Empty columns
	catalog := newTestCatalogWithModels(model)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	var count int
	err = db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM columns WHERE model_path = ?", "staging.customers").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestPopulateFromCatalog_EmptySources(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalog()
	catalog.Sources = []SourceDoc{} // Empty sources

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	var count int
	err = db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM sources").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestFTS5Search_SpecialCharacters(t *testing.T) {
	db := setupTestDB(t)

	model := newTestModel("staging.special_chars", "special_chars", "view")
	model.Description = "Test with 'quotes' and \"double quotes\""
	model.SQL = "SELECT * FROM raw WHERE name = 'test'"
	catalog := newTestCatalogWithModels(model)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	// Search should still work with special characters in content
	rows, err := db.DB().QueryContext(ctx, `
		SELECT m.path FROM models m
		JOIN models_fts fts ON m.rowid = fts.rowid
		WHERE models_fts MATCH 'test'
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count)
}

func TestPopulateFromCatalog_ColumnSourcesWithEmptyFields(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModelWithColumns("staging.customers", "customers", "view", []ColumnDoc{
			{
				Name:  "id",
				Index: 0,
				Sources: []SourceRef{
					{Table: "", Column: ""},         // Both empty - should be skipped
					{Table: "raw", Column: ""},      // Empty column - should be skipped
					{Table: "", Column: "col"},      // Empty table - should be skipped
					{Table: "valid", Column: "col"}, // Valid - should be inserted
				},
			},
		}),
	)

	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	var count int
	err = db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM column_sources WHERE model_path = ?", "staging.customers").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // Only the valid one
}
