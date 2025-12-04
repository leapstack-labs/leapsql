package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParser_ParseContent_BasicModel(t *testing.T) {
	p := NewParser("/models")

	content := `-- Basic model with no pragmas
SELECT id, name, email
FROM raw.users
WHERE active = true`

	config, err := p.ParseContent("/models/staging/users.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if config.Name != "users" {
		t.Errorf("expected name 'users', got %q", config.Name)
	}
	if config.Path != "staging.users" {
		t.Errorf("expected path 'staging.users', got %q", config.Path)
	}
	if config.Materialized != "table" {
		t.Errorf("expected materialized 'table', got %q", config.Materialized)
	}
	if len(config.Imports) != 0 {
		t.Errorf("expected 0 imports, got %d", len(config.Imports))
	}
}

func TestParser_ParseContent_WithConfig(t *testing.T) {
	p := NewParser("/models")

	content := `-- @config(materialized='view')
SELECT id, name
FROM staging.users`

	config, err := p.ParseContent("/models/marts/user_summary.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if config.Materialized != "view" {
		t.Errorf("expected materialized 'view', got %q", config.Materialized)
	}
}

func TestParser_ParseContent_WithUniqueKey(t *testing.T) {
	p := NewParser("/models")

	content := `-- @config(materialized='incremental', unique_key='id')
SELECT id, name, updated_at
FROM staging.users
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})`

	config, err := p.ParseContent("/models/marts/users.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if config.Materialized != "incremental" {
		t.Errorf("expected materialized 'incremental', got %q", config.Materialized)
	}
	if config.UniqueKey != "id" {
		t.Errorf("expected unique_key 'id', got %q", config.UniqueKey)
	}
}

func TestParser_ParseContent_WithImports(t *testing.T) {
	p := NewParser("/models")

	content := `-- @import(staging.orders)
-- @import(staging.customers)
SELECT 
    o.id,
    c.name
FROM staging.orders o
JOIN staging.customers c ON o.customer_id = c.id`

	config, err := p.ParseContent("/models/marts/order_summary.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Imports) != 2 {
		t.Fatalf("expected 2 imports, got %d", len(config.Imports))
	}
	if config.Imports[0] != "staging.orders" {
		t.Errorf("expected first import 'staging.orders', got %q", config.Imports[0])
	}
	if config.Imports[1] != "staging.customers" {
		t.Errorf("expected second import 'staging.customers', got %q", config.Imports[1])
	}
}

func TestParser_ParseContent_WithMultipleImports(t *testing.T) {
	p := NewParser("/models")

	content := `-- @import(staging.orders, staging.customers, staging.products)
SELECT * FROM staging.orders`

	config, err := p.ParseContent("/models/marts/summary.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Imports) != 3 {
		t.Fatalf("expected 3 imports, got %d", len(config.Imports))
	}
}

func TestParser_ParseContent_WithConditional(t *testing.T) {
	p := NewParser("/models")

	content := `-- @config(materialized='table')
SELECT id, name
FROM staging.users
-- #if env == 'prod'
WHERE created_at > '2024-01-01'
-- #endif`

	config, err := p.ParseContent("/models/marts/users.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Conditionals) != 1 {
		t.Fatalf("expected 1 conditional, got %d", len(config.Conditionals))
	}
	if config.Conditionals[0].Condition != "env == 'prod'" {
		t.Errorf("expected condition \"env == 'prod'\", got %q", config.Conditionals[0].Condition)
	}
	if config.Conditionals[0].Content != "WHERE created_at > '2024-01-01'\n" {
		t.Errorf("unexpected conditional content: %q", config.Conditionals[0].Content)
	}
}

func TestParser_ParseContent_SQLWithoutPragmas(t *testing.T) {
	p := NewParser("/models")

	content := `-- @config(materialized='view')
-- @import(staging.users)
SELECT id, name
FROM staging.users
WHERE active = true`

	config, err := p.ParseContent("/models/active_users.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	expectedSQL := `SELECT id, name
FROM staging.users
WHERE active = true`

	if config.SQL != expectedSQL {
		t.Errorf("expected SQL:\n%s\ngot:\n%s", expectedSQL, config.SQL)
	}
}

func TestParser_filePathToModelPath(t *testing.T) {
	tests := []struct {
		baseDir  string
		filePath string
		expected string
	}{
		{"/models", "/models/staging/users.sql", "staging.users"},
		{"/models", "/models/marts/core/orders.sql", "marts.core.orders"},
		{"/models", "/models/users.sql", "users"},
		{"/app/models", "/app/models/staging/customers.sql", "staging.customers"},
	}

	for _, tc := range tests {
		p := NewParser(tc.baseDir)
		result := p.filePathToModelPath(tc.filePath)
		if result != tc.expected {
			t.Errorf("filePathToModelPath(%q, %q) = %q, expected %q",
				tc.baseDir, tc.filePath, result, tc.expected)
		}
	}
}

func TestExtractReferences(t *testing.T) {
	tests := []struct {
		sql      string
		expected []string
	}{
		{
			sql:      `SELECT * FROM {{ ref('staging.orders') }}`,
			expected: []string{"staging.orders"},
		},
		{
			sql:      `SELECT * FROM {{ ref("staging.orders") }}`,
			expected: []string{"staging.orders"},
		},
		{
			sql: `SELECT o.id, c.name 
FROM {{ ref('staging.orders') }} o
JOIN {{ ref('staging.customers') }} c ON o.customer_id = c.id`,
			expected: []string{"staging.orders", "staging.customers"},
		},
		{
			sql:      `SELECT * FROM raw_data`,
			expected: nil,
		},
		{
			// Duplicate refs should be deduplicated
			sql: `SELECT * FROM {{ ref('staging.orders') }}
UNION ALL
SELECT * FROM {{ ref('staging.orders') }}`,
			expected: []string{"staging.orders"},
		},
	}

	for i, tc := range tests {
		refs := ExtractReferences(tc.sql)
		if len(refs) != len(tc.expected) {
			t.Errorf("test %d: expected %d refs, got %d", i, len(tc.expected), len(refs))
			continue
		}
		for j, ref := range refs {
			if ref != tc.expected[j] {
				t.Errorf("test %d: ref[%d] = %q, expected %q", i, j, ref, tc.expected[j])
			}
		}
	}
}

func TestScanner_ScanDir(t *testing.T) {
	// Create temp directory with test models
	tmpDir, err := os.MkdirTemp("", "parser-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create subdirectory
	stagingDir := filepath.Join(tmpDir, "staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		t.Fatalf("failed to create staging dir: %v", err)
	}

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
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write file %s: %v", path, err)
		}
	}

	// Scan directory
	scanner := NewScanner(tmpDir)
	models, err := scanner.ScanDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to scan dir: %v", err)
	}

	if len(models) != 3 {
		t.Fatalf("expected 3 models, got %d", len(models))
	}

	// Verify models were parsed correctly
	modelsByPath := make(map[string]*ModelConfig)
	for _, m := range models {
		modelsByPath[m.Path] = m
	}

	if m, ok := modelsByPath["staging.users"]; !ok {
		t.Error("missing staging.users model")
	} else if m.Materialized != "table" {
		t.Errorf("staging.users materialized = %q, expected 'table'", m.Materialized)
	}

	if m, ok := modelsByPath["staging.orders"]; !ok {
		t.Error("missing staging.orders model")
	} else if len(m.Imports) != 1 || m.Imports[0] != "staging.users" {
		t.Errorf("staging.orders imports = %v, expected [staging.users]", m.Imports)
	}

	if m, ok := modelsByPath["summary"]; !ok {
		t.Error("missing summary model")
	} else if len(m.Imports) != 2 {
		t.Errorf("summary imports = %v, expected 2 imports", m.Imports)
	}
}

func TestScanner_ScanDir_SkipsHiddenFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "parser-test-hidden")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create regular file and hidden file
	os.WriteFile(filepath.Join(tmpDir, "users.sql"), []byte("SELECT 1"), 0644)
	os.WriteFile(filepath.Join(tmpDir, ".hidden.sql"), []byte("SELECT 1"), 0644)

	scanner := NewScanner(tmpDir)
	models, err := scanner.ScanDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to scan dir: %v", err)
	}

	if len(models) != 1 {
		t.Errorf("expected 1 model (skipping hidden), got %d", len(models))
	}
}

func TestParser_ParseContent_AutoDetectSources(t *testing.T) {
	p := NewParser("/models")

	// Test: simple FROM clause
	content := `SELECT id, name FROM customers`
	config, err := p.ParseContent("/models/simple.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Sources) != 1 {
		t.Fatalf("expected 1 source, got %d: %v", len(config.Sources), config.Sources)
	}
	if config.Sources[0] != "customers" {
		t.Errorf("expected source 'customers', got %q", config.Sources[0])
	}
}

func TestParser_ParseContent_AutoDetectQualifiedSources(t *testing.T) {
	p := NewParser("/models")

	// Test: qualified table names
	content := `SELECT id, name FROM staging.stg_customers`
	config, err := p.ParseContent("/models/qualified.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Sources) != 1 {
		t.Fatalf("expected 1 source, got %d: %v", len(config.Sources), config.Sources)
	}
	if config.Sources[0] != "staging.stg_customers" {
		t.Errorf("expected source 'staging.stg_customers', got %q", config.Sources[0])
	}
}

func TestParser_ParseContent_AutoDetectJoinSources(t *testing.T) {
	p := NewParser("/models")

	// Test: JOIN with multiple sources
	content := `SELECT 
		c.customer_id,
		o.order_id
	FROM staging.stg_customers c
	LEFT JOIN staging.stg_orders o ON c.customer_id = o.customer_id`
	config, err := p.ParseContent("/models/join.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d: %v", len(config.Sources), config.Sources)
	}

	// Check both sources are present (order may vary)
	sourcesMap := make(map[string]bool)
	for _, s := range config.Sources {
		sourcesMap[s] = true
	}
	if !sourcesMap["staging.stg_customers"] {
		t.Error("expected 'staging.stg_customers' in sources")
	}
	if !sourcesMap["staging.stg_orders"] {
		t.Error("expected 'staging.stg_orders' in sources")
	}
}

func TestParser_ParseContent_AutoDetectWithPragmas(t *testing.T) {
	p := NewParser("/models")

	// Test: SQL with pragmas - should still detect sources from SQL
	content := `-- @config(materialized='table')
-- @import(staging.stg_customers)
-- Comment line
SELECT 
	customer_id,
	customer_name
FROM staging.stg_customers`

	config, err := p.ParseContent("/models/with_pragmas.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	// Should have explicit import from pragma
	if len(config.Imports) != 1 {
		t.Errorf("expected 1 import, got %d", len(config.Imports))
	}

	// Should also have auto-detected source
	if len(config.Sources) != 1 {
		t.Fatalf("expected 1 auto-detected source, got %d: %v", len(config.Sources), config.Sources)
	}
	if config.Sources[0] != "staging.stg_customers" {
		t.Errorf("expected source 'staging.stg_customers', got %q", config.Sources[0])
	}
}

func TestParser_ParseContent_AutoDetectSubquery(t *testing.T) {
	p := NewParser("/models")

	// Test: subquery with inner table reference
	content := `SELECT * FROM (
		SELECT id, name FROM raw_customers
	) subq`

	config, err := p.ParseContent("/models/subquery.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	if len(config.Sources) != 1 {
		t.Fatalf("expected 1 source, got %d: %v", len(config.Sources), config.Sources)
	}
	if config.Sources[0] != "raw_customers" {
		t.Errorf("expected source 'raw_customers', got %q", config.Sources[0])
	}
}

func TestParser_ParseContent_AutoDetectCTE(t *testing.T) {
	p := NewParser("/models")

	// Test: CTE - should only include real table sources, not the CTE itself
	content := `WITH customer_orders AS (
		SELECT customer_id, COUNT(*) as order_count
		FROM raw_orders
		GROUP BY customer_id
	)
	SELECT c.*, co.order_count
	FROM raw_customers c
	JOIN customer_orders co ON c.id = co.customer_id`

	config, err := p.ParseContent("/models/cte.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	// Should have 2 sources (raw_orders and raw_customers), not the CTE (customer_orders)
	sourcesMap := make(map[string]bool)
	for _, s := range config.Sources {
		sourcesMap[s] = true
	}

	if !sourcesMap["raw_orders"] {
		t.Errorf("expected 'raw_orders' in sources, got %v", config.Sources)
	}
	if !sourcesMap["raw_customers"] {
		t.Errorf("expected 'raw_customers' in sources, got %v", config.Sources)
	}
	if sourcesMap["customer_orders"] {
		t.Error("CTE 'customer_orders' should NOT be in sources")
	}
}

func TestParser_ParseContent_ColumnLineage(t *testing.T) {
	p := NewParser("/models")

	content := `SELECT 
		c.customer_id,
		c.customer_name,
		SUM(o.amount) as total_amount
	FROM customers c
	JOIN orders o ON c.customer_id = o.customer_id
	GROUP BY c.customer_id, c.customer_name`

	config, err := p.ParseContent("/models/summary.sql", content)
	if err != nil {
		t.Fatalf("failed to parse content: %v", err)
	}

	// Should have column lineage extracted
	if len(config.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(config.Columns))
	}

	// Find columns by name
	columnsByName := make(map[string]ColumnInfo)
	for _, col := range config.Columns {
		columnsByName[col.Name] = col
	}

	// customer_id should be direct from customers
	if col, ok := columnsByName["customer_id"]; !ok {
		t.Error("missing customer_id column")
	} else {
		if col.TransformType != "" {
			t.Errorf("customer_id should be direct (no transform), got %q", col.TransformType)
		}
		if len(col.Sources) < 1 {
			t.Error("customer_id should have at least one source")
		}
	}

	// total_amount should be an expression (SUM)
	if col, ok := columnsByName["total_amount"]; !ok {
		t.Error("missing total_amount column")
	} else {
		if col.TransformType != "EXPR" {
			t.Errorf("total_amount should be EXPR transform, got %q", col.TransformType)
		}
		if col.Function != "sum" {
			t.Errorf("total_amount should have function 'sum', got %q", col.Function)
		}
	}
}
