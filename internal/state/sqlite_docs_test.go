package state

import (
	"context"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupDocsTestStore creates a store and populates it with test data for docs queries.
func setupDocsTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store := setupTestStore(t)

	// Create some test models with different folders
	models := []*Model{
		{
			Model: &core.Model{
				Path:         "staging.customers",
				Name:         "customers",
				Materialized: "view",
				FilePath:     "models/staging/customers.sql",
				Description:  "Staging customer data",
				SQL:          "SELECT * FROM raw.customers",
				RawContent:   "---\nmaterialized: view\n---\nSELECT * FROM raw.customers",
			},
			ContentHash: "hash1",
		},
		{
			Model: &core.Model{
				Path:         "staging.orders",
				Name:         "orders",
				Materialized: "view",
				FilePath:     "models/staging/orders.sql",
				Description:  "Staging orders data",
				SQL:          "SELECT * FROM raw.orders",
				RawContent:   "---\nmaterialized: view\n---\nSELECT * FROM raw.orders",
			},
			ContentHash: "hash2",
		},
		{
			Model: &core.Model{
				Path:         "marts.customer_orders",
				Name:         "customer_orders",
				Materialized: "table",
				FilePath:     "models/marts/customer_orders.sql",
				Description:  "Customer orders mart",
				SQL:          "SELECT c.id, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
				RawContent:   "---\nmaterialized: table\n---\nSELECT c.id, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
			},
			ContentHash: "hash3",
		},
	}

	for _, m := range models {
		require.NoError(t, store.RegisterModel(m))
	}

	// Set up dependencies: customer_orders depends on customers and orders
	customerOrders, err := store.GetModelByPath("marts.customer_orders")
	require.NoError(t, err)
	customers, err := store.GetModelByPath("staging.customers")
	require.NoError(t, err)
	orders, err := store.GetModelByPath("staging.orders")
	require.NoError(t, err)

	require.NoError(t, store.SetDependencies(customerOrders.ID, []string{customers.ID, orders.ID}))

	// Set up columns for customer_orders with their source lineage
	columns := []core.ColumnInfo{
		{
			Name:          "id",
			Index:         0,
			TransformType: "passthrough",
			Sources: []core.SourceRef{
				{Table: "staging.customers", Column: "id"},
			},
		},
		{
			Name:          "amount",
			Index:         1,
			TransformType: "passthrough",
			Sources: []core.SourceRef{
				{Table: "staging.orders", Column: "amount"},
			},
		},
	}
	require.NoError(t, store.SaveModelColumns("marts.customer_orders", columns))

	return store
}

func TestDocsQueries_GetModelsForDocs(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	models, err := store.queries.GetModelsForDocs(context.Background())
	require.NoError(t, err)

	assert.Len(t, models, 3)

	// Should be ordered by folder, then name
	assert.Equal(t, "marts.customer_orders", models[0].Path)
	assert.Equal(t, "marts", models[0].Folder)
	assert.Equal(t, "staging.customers", models[1].Path)
	assert.Equal(t, "staging", models[1].Folder)
	assert.Equal(t, "staging.orders", models[2].Path)
	assert.Equal(t, "staging", models[2].Folder)
}

func TestDocsQueries_GetModelForDocs(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	model, err := store.queries.GetModelForDocs(context.Background(), "marts.customer_orders")
	require.NoError(t, err)

	assert.Equal(t, "marts.customer_orders", model.Path)
	assert.Equal(t, "customer_orders", model.Name)
	assert.Equal(t, "marts", model.Folder)
	assert.Equal(t, "table", model.Materialized)
	require.NotNil(t, model.Description)
	assert.Equal(t, "Customer orders mart", *model.Description)
	require.NotNil(t, model.SqlContent)
	assert.Contains(t, *model.SqlContent, "SELECT c.id")
}

func TestDocsQueries_GetModelDependenciesByPath(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	deps, err := store.queries.GetModelDependenciesByPath(context.Background(), "marts.customer_orders")
	require.NoError(t, err)

	assert.Len(t, deps, 2)
	assert.Contains(t, deps, "staging.customers")
	assert.Contains(t, deps, "staging.orders")
}

func TestDocsQueries_GetModelDependentsByPath(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	dependents, err := store.queries.GetModelDependentsByPath(context.Background(), "staging.customers")
	require.NoError(t, err)

	assert.Len(t, dependents, 1)
	assert.Contains(t, dependents, "marts.customer_orders")
}

func TestDocsQueries_GetLineageEdges(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	edges, err := store.queries.GetLineageEdges(context.Background())
	require.NoError(t, err)

	// Should have edges from staging models to marts model
	assert.GreaterOrEqual(t, len(edges), 2)

	// Check that we have the expected edges
	edgeMap := make(map[string]string)
	for _, e := range edges {
		edgeMap[e.SourceNode] = e.TargetNode
	}
	assert.Equal(t, "marts.customer_orders", edgeMap["staging.customers"])
	assert.Equal(t, "marts.customer_orders", edgeMap["staging.orders"])
}

func TestDocsQueries_GetColumnsForModel(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	columns, err := store.queries.GetColumnsForModel(context.Background(), "marts.customer_orders")
	require.NoError(t, err)

	assert.Len(t, columns, 2)
	assert.Equal(t, "id", columns[0].Name)
	assert.Equal(t, int64(0), columns[0].Idx)
	assert.Equal(t, "amount", columns[1].Name)
	assert.Equal(t, int64(1), columns[1].Idx)
}

func TestDocsQueries_GetColumnLineageNodes(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	nodes, err := store.queries.GetColumnLineageNodes(context.Background())
	require.NoError(t, err)

	// Should have nodes for output columns and source columns
	assert.GreaterOrEqual(t, len(nodes), 4) // 2 output + 2 source columns

	// Extract IDs
	nodeIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if id, ok := n.ID.(string); ok {
			nodeIDs = append(nodeIDs, id)
		}
	}

	assert.Contains(t, nodeIDs, "marts.customer_orders.id")
	assert.Contains(t, nodeIDs, "marts.customer_orders.amount")
	assert.Contains(t, nodeIDs, "staging.customers.id")
	assert.Contains(t, nodeIDs, "staging.orders.amount")
}

func TestDocsQueries_GetColumnLineageEdges(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	edges, err := store.queries.GetColumnLineageEdges(context.Background())
	require.NoError(t, err)

	// Should have edges connecting source columns to output columns
	assert.GreaterOrEqual(t, len(edges), 2)
}

func TestDocsQueries_GetColumnLineageNodesForModel(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	nodes, err := store.queries.GetColumnLineageNodesForModel(context.Background(), "marts.customer_orders")
	require.NoError(t, err)

	// Should include the model's columns and their upstream sources
	assert.GreaterOrEqual(t, len(nodes), 4)

	// Extract models
	models := make(map[string]bool)
	for _, n := range nodes {
		models[n.Model] = true
	}

	assert.True(t, models["marts.customer_orders"], "should include the target model")
	assert.True(t, models["staging.customers"], "should include upstream source")
	assert.True(t, models["staging.orders"], "should include upstream source")
}

func TestDocsQueries_GetColumnLineageEdgesForModel(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	edges, err := store.queries.GetColumnLineageEdgesForModel(context.Background(), "marts.customer_orders")
	require.NoError(t, err)

	// Should have edges pointing to this model's columns
	assert.GreaterOrEqual(t, len(edges), 2)
}

func TestDocsQueries_GetAllColumnSourcesForModel(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	sources, err := store.queries.GetAllColumnSourcesForModel(context.Background(), "marts.customer_orders")
	require.NoError(t, err)

	assert.Len(t, sources, 2)

	// Create a map for easier assertion
	sourceMap := make(map[string]sqlcgen.GetAllColumnSourcesForModelRow)
	for _, s := range sources {
		sourceMap[s.ColumnName] = s
	}

	assert.Equal(t, "staging.customers", sourceMap["id"].SourceTable)
	assert.Equal(t, "id", sourceMap["id"].SourceColumn)
	assert.Equal(t, "staging.orders", sourceMap["amount"].SourceTable)
	assert.Equal(t, "amount", sourceMap["amount"].SourceColumn)
}

func TestDocsQueries_GetColumnSourcesForColumn(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	sources, err := store.queries.GetColumnSourcesForColumn(context.Background(), sqlcgen.GetColumnSourcesForColumnParams{
		ModelPath:  "marts.customer_orders",
		ColumnName: "id",
	})
	require.NoError(t, err)

	assert.Len(t, sources, 1)
	assert.Equal(t, "staging.customers", sources[0].SourceTable)
	assert.Equal(t, "id", sources[0].SourceColumn)
}

func TestDocsQueries_Stats(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	t.Run("GetModelCount", func(t *testing.T) {
		count, err := store.queries.GetModelCount(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("GetColumnCount", func(t *testing.T) {
		count, err := store.queries.GetColumnCount(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("GetFolderCount", func(t *testing.T) {
		count, err := store.queries.GetFolderCount(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // staging, marts
	})

	t.Run("GetMaterializationCounts", func(t *testing.T) {
		counts, err := store.queries.GetMaterializationCounts(context.Background())
		require.NoError(t, err)
		assert.Len(t, counts, 2)

		countMap := make(map[string]int64)
		for _, c := range counts {
			countMap[c.Materialized] = c.Count
		}
		assert.Equal(t, int64(2), countMap["view"])
		assert.Equal(t, int64(1), countMap["table"])
	})
}

func TestDocsQueries_SearchModels(t *testing.T) {
	store := setupDocsTestStore(t)
	defer func() { _ = store.Close() }()

	t.Run("search_by_name", func(t *testing.T) {
		// FTS5 syntax: use * for prefix matching
		results, err := store.SearchModels("customer*")
		require.NoError(t, err)

		// Should find customers and customer_orders
		assert.Len(t, results, 2)

		names := make([]string, 0, len(results))
		for _, r := range results {
			names = append(names, r.Name)
		}
		assert.Contains(t, names, "customers")
		assert.Contains(t, names, "customer_orders")
	})

	t.Run("search_by_description", func(t *testing.T) {
		results, err := store.SearchModels("mart")
		require.NoError(t, err)

		// Should find customer_orders (has "mart" in description)
		assert.Len(t, results, 1)
		assert.Equal(t, "customer_orders", results[0].Name)
	})

	t.Run("search_no_results", func(t *testing.T) {
		results, err := store.SearchModels("nonexistent")
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("search_ranking", func(t *testing.T) {
		// Results should be ranked by relevance
		results, err := store.SearchModels("orders")
		require.NoError(t, err)

		// Should find at least orders model
		assert.NotEmpty(t, results)
		// Model named "orders" should likely rank higher than customer_orders
		// (though both should appear)
	})
}

func TestDocsQueries_GetExternalSources(t *testing.T) {
	store := setupTestStore(t) // Use fresh store
	defer func() { _ = store.Close() }()

	// Create a model that references an external source
	model := &Model{
		Model: &core.Model{
			Path:         "staging.data",
			Name:         "data",
			Materialized: "view",
			FilePath:     "models/staging/data.sql",
		},
		ContentHash: "hash",
	}
	require.NoError(t, store.RegisterModel(model))

	// Add columns with lineage from an external source (not a model)
	columns := []core.ColumnInfo{
		{
			Name:  "col1",
			Index: 0,
			Sources: []core.SourceRef{
				{Table: "external_db.raw_table", Column: "field1"},
			},
		},
	}
	require.NoError(t, store.SaveModelColumns("staging.data", columns))

	sources, err := store.queries.GetExternalSources(context.Background())
	require.NoError(t, err)

	assert.Len(t, sources, 1)
	assert.Equal(t, "external_db.raw_table", sources[0])
}

func TestDocsQueries_GetSourceReferencedBy(t *testing.T) {
	store := setupTestStore(t) // Use fresh store
	defer func() { _ = store.Close() }()

	// Create two models that reference the same external source
	for i, path := range []string{"staging.a", "staging.b"} {
		model := &Model{
			Model: &core.Model{
				Path:         path,
				Name:         path[8:], // "a" or "b"
				Materialized: "view",
				FilePath:     "models/" + path + ".sql",
			},
			ContentHash: "hash" + string(rune('0'+i)),
		}
		require.NoError(t, store.RegisterModel(model))

		columns := []core.ColumnInfo{
			{
				Name:  "col1",
				Index: 0,
				Sources: []core.SourceRef{
					{Table: "external.source", Column: "field1"},
				},
			},
		}
		require.NoError(t, store.SaveModelColumns(path, columns))
	}

	models, err := store.queries.GetSourceReferencedBy(context.Background(), "external.source")
	require.NoError(t, err)

	assert.Len(t, models, 2)
	assert.Contains(t, models, "staging.a")
	assert.Contains(t, models, "staging.b")
}

func TestDocsQueries_EmptyResults(t *testing.T) {
	store := setupTestStore(t) // Fresh store with no data
	defer func() { _ = store.Close() }()

	t.Run("GetModelsForDocs_empty", func(t *testing.T) {
		models, err := store.queries.GetModelsForDocs(context.Background())
		require.NoError(t, err)
		assert.Empty(t, models)
	})

	t.Run("GetModelDependenciesByPath_not_found", func(t *testing.T) {
		deps, err := store.queries.GetModelDependenciesByPath(context.Background(), "nonexistent")
		require.NoError(t, err)
		assert.Empty(t, deps)
	})

	t.Run("GetLineageEdges_empty", func(t *testing.T) {
		edges, err := store.queries.GetLineageEdges(context.Background())
		require.NoError(t, err)
		assert.Empty(t, edges)
	})

	t.Run("GetExternalSources_empty", func(t *testing.T) {
		sources, err := store.queries.GetExternalSources(context.Background())
		require.NoError(t, err)
		assert.Empty(t, sources)
	})

	t.Run("Stats_zero", func(t *testing.T) {
		count, err := store.queries.GetModelCount(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})
}
