package parser

import (
	"errors"
	"testing"
)

func TestExtractFrontmatter_ValidBasic(t *testing.T) {
	content := `/*---
name: monthly_revenue
materialized: table
owner: finance
---*/

SELECT * FROM orders`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.HasYAML {
		t.Error("expected HasYAML to be true")
	}

	if result.Config.Name != "monthly_revenue" {
		t.Errorf("expected name 'monthly_revenue', got %q", result.Config.Name)
	}

	if result.Config.Materialized != "table" {
		t.Errorf("expected materialized 'table', got %q", result.Config.Materialized)
	}

	if result.Config.Owner != "finance" {
		t.Errorf("expected owner 'finance', got %q", result.Config.Owner)
	}

	expectedSQL := "SELECT * FROM orders"
	if result.SQL != expectedSQL {
		t.Errorf("expected SQL %q, got %q", expectedSQL, result.SQL)
	}
}

func TestExtractFrontmatter_AllFields(t *testing.T) {
	content := `/*---
name: user_metrics
materialized: incremental
unique_key: user_id
owner: data-team
schema: analytics
tags:
  - users
  - metrics
tests:
  - unique: [user_id]
  - not_null: [user_id, created_at]
meta:
  priority: high
  team: growth
---*/

SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg := result.Config

	if cfg.Name != "user_metrics" {
		t.Errorf("expected name 'user_metrics', got %q", cfg.Name)
	}

	if cfg.Materialized != "incremental" {
		t.Errorf("expected materialized 'incremental', got %q", cfg.Materialized)
	}

	if cfg.UniqueKey != "user_id" {
		t.Errorf("expected unique_key 'user_id', got %q", cfg.UniqueKey)
	}

	if cfg.Owner != "data-team" {
		t.Errorf("expected owner 'data-team', got %q", cfg.Owner)
	}

	if cfg.Schema != "analytics" {
		t.Errorf("expected schema 'analytics', got %q", cfg.Schema)
	}

	if len(cfg.Tags) != 2 || cfg.Tags[0] != "users" || cfg.Tags[1] != "metrics" {
		t.Errorf("expected tags ['users', 'metrics'], got %v", cfg.Tags)
	}

	if len(cfg.Tests) != 2 {
		t.Errorf("expected 2 test configs, got %d", len(cfg.Tests))
	} else {
		if len(cfg.Tests[0].Unique) != 1 || cfg.Tests[0].Unique[0] != "user_id" {
			t.Errorf("expected unique test [user_id], got %v", cfg.Tests[0].Unique)
		}
		if len(cfg.Tests[1].NotNull) != 2 {
			t.Errorf("expected not_null test with 2 columns, got %v", cfg.Tests[1].NotNull)
		}
	}

	if cfg.Meta["priority"] != "high" {
		t.Errorf("expected meta.priority 'high', got %v", cfg.Meta["priority"])
	}

	if cfg.Meta["team"] != "growth" {
		t.Errorf("expected meta.team 'growth', got %v", cfg.Meta["team"])
	}
}

func TestExtractFrontmatter_AcceptedValuesTest(t *testing.T) {
	content := `/*---
name: orders
materialized: table
tests:
  - accepted_values:
      column: status
      values: [pending, completed, cancelled]
---*/

SELECT * FROM raw_orders`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Config.Tests) != 1 {
		t.Fatalf("expected 1 test config, got %d", len(result.Config.Tests))
	}

	av := result.Config.Tests[0].AcceptedValues
	if av == nil {
		t.Fatal("expected accepted_values test, got nil")
	}

	if av.Column != "status" {
		t.Errorf("expected column 'status', got %q", av.Column)
	}

	if len(av.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(av.Values))
	}
}

func TestExtractFrontmatter_NoFrontmatter(t *testing.T) {
	content := `SELECT * FROM orders WHERE amount > 100`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.HasYAML {
		t.Error("expected HasYAML to be false")
	}

	if result.SQL != content {
		t.Errorf("expected SQL to be unchanged, got %q", result.SQL)
	}
}

func TestExtractFrontmatter_EmptyFrontmatter(t *testing.T) {
	content := `/*---
---*/

SELECT 1`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.HasYAML {
		t.Error("expected HasYAML to be true")
	}

	// Empty frontmatter should result in empty config
	if result.Config.Name != "" {
		t.Errorf("expected empty name, got %q", result.Config.Name)
	}
}

func TestExtractFrontmatter_UnknownFieldError(t *testing.T) {
	content := `/*---
name: test_model
unknown_field: value
---*/

SELECT 1`

	_, err := ExtractFrontmatter(content)
	if err == nil {
		t.Fatal("expected error for unknown field")
	}

	var unknownErr *UnknownFieldError
	if !errors.As(err, &unknownErr) {
		t.Fatalf("expected UnknownFieldError, got %T: %v", err, err)
	}

	if unknownErr.Field != "unknown_field" {
		t.Errorf("expected field 'unknown_field', got %q", unknownErr.Field)
	}
}

func TestExtractFrontmatter_InvalidMaterialized(t *testing.T) {
	content := `/*---
name: test_model
materialized: invalid_type
---*/

SELECT 1`

	_, err := ExtractFrontmatter(content)
	if err == nil {
		t.Fatal("expected error for invalid materialized value")
	}

	var parseErr *FrontmatterParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected FrontmatterParseError, got %T: %v", err, err)
	}

	if parseErr.Message == "" {
		t.Error("expected error message to be non-empty")
	}
}

func TestExtractFrontmatter_InvalidYAML(t *testing.T) {
	content := `/*---
name: test_model
invalid yaml: [
---*/

SELECT 1`

	_, err := ExtractFrontmatter(content)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}

	var parseErr *FrontmatterParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected FrontmatterParseError, got %T: %v", err, err)
	}

	if parseErr.Message == "" {
		t.Error("expected error message to be non-empty")
	}
}

func TestExtractFrontmatter_ViewMaterialized(t *testing.T) {
	content := `/*---
materialized: view
---*/

SELECT 1`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Config.Materialized != "view" {
		t.Errorf("expected materialized 'view', got %q", result.Config.Materialized)
	}
}

func TestExtractFrontmatter_IncrementalMaterialized(t *testing.T) {
	content := `/*---
materialized: incremental
unique_key: id
---*/

SELECT 1`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Config.Materialized != "incremental" {
		t.Errorf("expected materialized 'incremental', got %q", result.Config.Materialized)
	}

	if result.Config.UniqueKey != "id" {
		t.Errorf("expected unique_key 'id', got %q", result.Config.UniqueKey)
	}
}

func TestApplyDefaults(t *testing.T) {
	tests := []struct {
		name       string
		config     FrontmatterConfig
		filename   string
		dirPath    string
		wantName   string
		wantMatl   string
		wantSchema string
	}{
		{
			name:       "empty config gets defaults",
			config:     FrontmatterConfig{},
			filename:   "orders.sql",
			dirPath:    "staging",
			wantName:   "orders",
			wantMatl:   "table",
			wantSchema: "staging",
		},
		{
			name:       "explicit values are preserved",
			config:     FrontmatterConfig{Name: "my_orders", Materialized: "view", Schema: "analytics"},
			filename:   "orders.sql",
			dirPath:    "staging",
			wantName:   "my_orders",
			wantMatl:   "view",
			wantSchema: "analytics",
		},
		{
			name:       "partial defaults",
			config:     FrontmatterConfig{Name: "custom_name"},
			filename:   "orders.sql",
			dirPath:    "marts",
			wantName:   "custom_name",
			wantMatl:   "table",
			wantSchema: "marts",
		},
		{
			name:       "no dirPath",
			config:     FrontmatterConfig{},
			filename:   "orders.sql",
			dirPath:    "",
			wantName:   "orders",
			wantMatl:   "table",
			wantSchema: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config
			cfg.ApplyDefaults(tt.filename, tt.dirPath)

			if cfg.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", cfg.Name, tt.wantName)
			}
			if cfg.Materialized != tt.wantMatl {
				t.Errorf("Materialized = %q, want %q", cfg.Materialized, tt.wantMatl)
			}
			if cfg.Schema != tt.wantSchema {
				t.Errorf("Schema = %q, want %q", cfg.Schema, tt.wantSchema)
			}
		})
	}
}

func TestFrontmatterParseError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  FrontmatterParseError
		want string
	}{
		{
			name: "with file and line",
			err:  FrontmatterParseError{File: "models/orders.sql", Line: 5, Message: "invalid YAML"},
			want: "models/orders.sql:5: invalid YAML",
		},
		{
			name: "with file only",
			err:  FrontmatterParseError{File: "models/orders.sql", Message: "invalid YAML"},
			want: "models/orders.sql: invalid YAML",
		},
		{
			name: "message only",
			err:  FrontmatterParseError{Message: "invalid YAML"},
			want: "invalid YAML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUnknownFieldError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  UnknownFieldError
		want string
	}{
		{
			name: "with file",
			err:  UnknownFieldError{File: "models/orders.sql", Field: "custom"},
			want: `models/orders.sql: unknown field "custom" in frontmatter, use "meta" field for custom fields`,
		},
		{
			name: "without file",
			err:  UnknownFieldError{Field: "custom"},
			want: `unknown field "custom" in frontmatter, use "meta" field for custom fields`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractFrontmatter_WhitespaceHandling(t *testing.T) {
	// Test with various whitespace before frontmatter
	content := `   /*---
name: test
---*/
SELECT 1`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.HasYAML {
		t.Error("expected HasYAML to be true")
	}

	if result.Config.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Config.Name)
	}
}

func TestExtractFrontmatter_MultilineSQL(t *testing.T) {
	content := `/*---
name: complex_query
materialized: table
---*/

WITH base AS (
    SELECT * FROM orders
    WHERE created_at > '2024-01-01'
),
aggregated AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM base
    GROUP BY customer_id
)
SELECT * FROM aggregated`

	result, err := ExtractFrontmatter(content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Config.Name != "complex_query" {
		t.Errorf("expected name 'complex_query', got %q", result.Config.Name)
	}

	// Check that the SQL is properly extracted
	if len(result.SQL) < 100 {
		t.Error("expected SQL to contain the full query")
	}
}
