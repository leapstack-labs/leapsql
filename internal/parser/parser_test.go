package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParser_ParseContent(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantName      string
		wantPath      string
		wantMatl      string
		wantUniqueKey string
		wantImports   []string
		checkFunc     func(t *testing.T, config *ModelConfig)
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
			checkFunc: func(t *testing.T, config *ModelConfig) {
				if len(config.Conditionals) != 1 {
					t.Fatalf("expected 1 conditional, got %d", len(config.Conditionals))
				}
				if config.Conditionals[0].Condition != "env == 'prod'" {
					t.Errorf("expected condition \"env == 'prod'\", got %q", config.Conditionals[0].Condition)
				}
				if config.Conditionals[0].Content != "WHERE created_at > '2024-01-01'\n" {
					t.Errorf("unexpected conditional content: %q", config.Conditionals[0].Content)
				}
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
			checkFunc: func(t *testing.T, config *ModelConfig) {
				expectedSQL := `SELECT id, name
FROM staging.users
WHERE active = true`
				if config.SQL != expectedSQL {
					t.Errorf("expected SQL:\n%s\ngot:\n%s", expectedSQL, config.SQL)
				}
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
			p := NewParser("/models")
			filePath := filePaths[tt.name]
			config, err := p.ParseContent(filePath, tt.sql)
			if err != nil {
				t.Fatalf("failed to parse content: %v", err)
			}

			if tt.wantName != "" && config.Name != tt.wantName {
				t.Errorf("expected name %q, got %q", tt.wantName, config.Name)
			}
			if tt.wantPath != "" && config.Path != tt.wantPath {
				t.Errorf("expected path %q, got %q", tt.wantPath, config.Path)
			}
			if tt.wantMatl != "" && config.Materialized != tt.wantMatl {
				t.Errorf("expected materialized %q, got %q", tt.wantMatl, config.Materialized)
			}
			if tt.wantUniqueKey != "" && config.UniqueKey != tt.wantUniqueKey {
				t.Errorf("expected unique_key %q, got %q", tt.wantUniqueKey, config.UniqueKey)
			}
			if tt.wantImports != nil {
				if len(config.Imports) != len(tt.wantImports) {
					t.Fatalf("expected %d imports, got %d", len(tt.wantImports), len(config.Imports))
				}
				for i, imp := range tt.wantImports {
					if config.Imports[i] != imp {
						t.Errorf("import[%d]: expected %q, got %q", i, imp, config.Imports[i])
					}
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
			p := NewParser("/models")
			config, err := p.ParseContent("/models/test.sql", tt.sql)
			if err != nil {
				t.Fatalf("failed to parse content: %v", err)
			}

			sourcesMap := make(map[string]bool)
			for _, s := range config.Sources {
				sourcesMap[s] = true
			}

			for _, want := range tt.wantSources {
				if !sourcesMap[want] {
					t.Errorf("expected source %q in sources %v", want, config.Sources)
				}
			}

			// Special check for CTE test: CTE names should NOT be in sources
			if tt.name == "CTE" {
				if sourcesMap["customer_orders"] {
					t.Error("CTE 'customer_orders' should NOT be in sources")
				}
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
			p := NewParser(tt.baseDir)
			result := p.filePathToModelPath(tt.filePath)
			if result != tt.expected {
				t.Errorf("filePathToModelPath(%q, %q) = %q, expected %q",
					tt.baseDir, tt.filePath, result, tt.expected)
			}
		})
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
