package loader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import duckdb dialect so it registers itself
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
)

// testDialect returns the DuckDB dialect for testing.
func testDialect(t *testing.T) *dialect.Dialect {
	t.Helper()
	d, ok := dialect.Get("duckdb")
	require.True(t, ok, "DuckDB dialect not found - ensure duckdb/dialect package is imported")
	return d
}

func TestParser_ParseContent(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantName      string
		wantPath      string
		wantMatl      string
		wantUniqueKey string
		wantImports   []string
		checkFunc     func(t *testing.T, config *core.Model)
	}{
		{
			name: "basic model",
			sql: `-- Basic model with no pragmas
SELECT id, name, email
FROM raw.users
WHERE active = true`,
			wantName: "users",
			wantPath: "staging.users",
			wantMatl: "table",
		},
		{
			name: "with config",
			sql: `-- @config(materialized='view')
SELECT id, name
FROM staging.users`,
			wantName: "user_summary",
			wantPath: "marts.user_summary",
			wantMatl: "view",
		},
		{
			name: "with unique_key",
			sql: `-- @config(materialized='incremental', unique_key='id')
SELECT id, name, updated_at
FROM staging.users
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})`,
			wantName:      "users",
			wantPath:      "marts.users",
			wantMatl:      "incremental",
			wantUniqueKey: "id",
		},
		{
			name: "with single import",
			sql: `-- @import(staging.orders)
-- @import(staging.customers)
SELECT 
    o.id,
    c.name
FROM staging.orders o
JOIN staging.customers c ON o.customer_id = c.id`,
			wantName:    "order_summary",
			wantPath:    "marts.order_summary",
			wantImports: []string{"staging.orders", "staging.customers"},
		},
		{
			name: "with multiple imports",
			sql: `-- @import(staging.orders, staging.customers, staging.products)
SELECT * FROM staging.orders`,
			wantName:    "summary",
			wantPath:    "marts.summary",
			wantImports: []string{"staging.orders", "staging.customers", "staging.products"},
		},
		{
			name: "with conditional",
			sql: `-- @config(materialized='table')
SELECT id, name
FROM staging.users
-- #if env == 'prod'
WHERE created_at > '2024-01-01'
-- #endif`,
			wantName: "users",
			wantPath: "marts.users",
			checkFunc: func(t *testing.T, config *core.Model) {
				require.Len(t, config.Conditionals, 1)
				assert.Equal(t, "env == 'prod'", config.Conditionals[0].Condition)
				assert.Equal(t, "WHERE created_at > '2024-01-01'\n", config.Conditionals[0].Content)
			},
		},
		{
			name: "SQL without pragmas",
			sql: `-- @config(materialized='view')
-- @import(staging.users)
SELECT id, name
FROM staging.users
WHERE active = true`,
			wantName: "active_users",
			wantPath: "active_users",
			checkFunc: func(t *testing.T, config *core.Model) {
				expectedSQL := `SELECT id, name
FROM staging.users
WHERE active = true`
				assert.Equal(t, expectedSQL, config.SQL)
			},
		},
	}

	// Map test name to file path
	filePaths := map[string]string{
		"basic model":           "/models/staging/users.sql",
		"with config":           "/models/marts/user_summary.sql",
		"with unique_key":       "/models/marts/users.sql",
		"with single import":    "/models/marts/order_summary.sql",
		"with multiple imports": "/models/marts/summary.sql",
		"with conditional":      "/models/marts/users.sql",
		"SQL without pragmas":   "/models/active_users.sql",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewLoader("/models", testDialect(t))
			filePath := filePaths[tt.name]
			config, err := p.ParseContent(filePath, tt.sql)
			require.NoError(t, err)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, config.Name)
			}
			if tt.wantPath != "" {
				assert.Equal(t, tt.wantPath, config.Path)
			}
			if tt.wantMatl != "" {
				assert.Equal(t, tt.wantMatl, config.Materialized)
			}
			if tt.wantUniqueKey != "" {
				assert.Equal(t, tt.wantUniqueKey, config.UniqueKey)
			}
			if tt.wantImports != nil {
				require.Len(t, config.Imports, len(tt.wantImports))
				for i, imp := range tt.wantImports {
					assert.Equal(t, imp, config.Imports[i])
				}
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, config)
			}
		})
	}
}

func TestParser_ParseContent_AutoDetect(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantSources []string
	}{
		{
			name:        "simple FROM clause",
			sql:         `SELECT id, name FROM customers`,
			wantSources: []string{"customers"},
		},
		{
			name:        "qualified table names",
			sql:         `SELECT id, name FROM staging.stg_customers`,
			wantSources: []string{"staging.stg_customers"},
		},
		{
			name: "JOIN with multiple sources",
			sql: `SELECT 
		c.customer_id,
		o.order_id
	FROM staging.stg_customers c
	LEFT JOIN staging.stg_orders o ON c.customer_id = o.customer_id`,
			wantSources: []string{"staging.stg_customers", "staging.stg_orders"},
		},
		{
			name: "SQL with pragmas",
			sql: `-- @config(materialized='table')
-- @import(staging.stg_customers)
-- Comment line
SELECT 
	customer_id,
	customer_name
FROM staging.stg_customers`,
			wantSources: []string{"staging.stg_customers"},
		},
		{
			name: "subquery with inner table reference",
			sql: `SELECT * FROM (
		SELECT id, name FROM raw_customers
	) subq`,
			wantSources: []string{"raw_customers"},
		},
		{
			name: "CTE",
			sql: `WITH customer_orders AS (
		SELECT customer_id, COUNT(*) as order_count
		FROM raw_orders
		GROUP BY customer_id
	)
	SELECT c.*, co.order_count
	FROM raw_customers c
	JOIN customer_orders co ON c.id = co.customer_id`,
			wantSources: []string{"raw_orders", "raw_customers"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewLoader("/models", testDialect(t))
			config, err := p.ParseContent("/models/test.sql", tt.sql)
			require.NoError(t, err)

			sourcesMap := make(map[string]bool)
			for _, s := range config.Sources {
				sourcesMap[s] = true
			}

			for _, want := range tt.wantSources {
				assert.True(t, sourcesMap[want], "expected source %q in sources %v", want, config.Sources)
			}

			// Special check for CTE test: CTE names should NOT be in sources
			if tt.name == "CTE" {
				assert.False(t, sourcesMap["customer_orders"], "CTE 'customer_orders' should NOT be in sources")
			}
		})
	}
}

func TestParser_filePathToModelPath(t *testing.T) {
	tests := []struct {
		name     string
		baseDir  string
		filePath string
		expected string
	}{
		{"staging model", "/models", "/models/staging/users.sql", "staging.users"},
		{"nested marts model", "/models", "/models/marts/core/orders.sql", "marts.core.orders"},
		{"root model", "/models", "/models/users.sql", "users"},
		{"different base", "/app/models", "/app/models/staging/customers.sql", "staging.customers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewLoader(tt.baseDir, nil) // dialect not needed for path conversion
			result := p.filePathToModelPath(tt.filePath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScanner_ScanDir(t *testing.T) {
	// Create temp directory with test models
	tmpDir, err := os.MkdirTemp("", "parser-test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create subdirectory
	stagingDir := filepath.Join(tmpDir, "staging")
	require.NoError(t, os.MkdirAll(stagingDir, 0750))

	// Create test files
	files := map[string]string{
		filepath.Join(stagingDir, "users.sql"): `-- @config(materialized='table')
SELECT * FROM raw.users`,
		filepath.Join(stagingDir, "orders.sql"): `-- @import(staging.users)
SELECT * FROM raw.orders`,
		filepath.Join(tmpDir, "summary.sql"): `-- @import(staging.users, staging.orders)
SELECT COUNT(*) FROM staging.users`,
	}

	for path, content := range files {
		require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	}

	// Scan directory
	scanner := NewScanner(tmpDir, testDialect(t))
	models, err := scanner.ScanDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, models, 3)

	// Verify models were parsed correctly
	modelsByPath := make(map[string]*core.Model)
	for _, m := range models {
		modelsByPath[m.Path] = m
	}

	m, ok := modelsByPath["staging.users"]
	require.True(t, ok, "missing staging.users model")
	assert.Equal(t, "table", m.Materialized)

	m, ok = modelsByPath["staging.orders"]
	require.True(t, ok, "missing staging.orders model")
	assert.Equal(t, []string{"staging.users"}, m.Imports)

	m, ok = modelsByPath["summary"]
	require.True(t, ok, "missing summary model")
	assert.Len(t, m.Imports, 2)
}

func TestScanner_ScanDir_SkipsHiddenFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "parser-test-hidden")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create regular file and hidden file
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "users.sql"), []byte("SELECT 1"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, ".hidden.sql"), []byte("SELECT 1"), 0600))

	scanner := NewScanner(tmpDir, nil) // dialect not needed for hidden file test
	models, err := scanner.ScanDir(tmpDir)
	require.NoError(t, err)
	assert.Len(t, models, 1, "expected 1 model (skipping hidden)")
}

func TestParser_ParseContent_ColumnLineage(t *testing.T) {
	p := NewLoader("/models", testDialect(t))

	content := `SELECT 
		c.customer_id,
		c.customer_name,
		SUM(o.amount) as total_amount
	FROM customers c
	JOIN orders o ON c.customer_id = o.customer_id
	GROUP BY c.customer_id, c.customer_name`

	config, err := p.ParseContent("/models/summary.sql", content)
	require.NoError(t, err)

	// Should have column lineage extracted
	require.Len(t, config.Columns, 3)

	// Find columns by name
	columnsByName := make(map[string]core.ColumnInfo)
	for _, col := range config.Columns {
		columnsByName[col.Name] = col
	}

	// customer_id should be direct from customers
	col, ok := columnsByName["customer_id"]
	require.True(t, ok, "missing customer_id column")
	assert.Empty(t, col.TransformType, "customer_id should be direct (no transform)")
	assert.NotEmpty(t, col.Sources, "customer_id should have at least one source")

	// total_amount should be an expression (SUM)
	col, ok = columnsByName["total_amount"]
	require.True(t, ok, "missing total_amount column")
	assert.Equal(t, "EXPR", col.TransformType)
	assert.Equal(t, "sum", col.Function)
}
