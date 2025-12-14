package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Import adapter package to ensure duckdb is registered via init()
	_ "github.com/leapstack-labs/leapsql/internal/adapter"
)

// TestTargetConfig_Validate tests the Validate method of TargetConfig.
func TestTargetConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		target    TargetConfig
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "empty type",
			target:    TargetConfig{Type: ""},
			wantErr:   true,
			errSubstr: "target type is required",
		},
		{
			name:      "valid duckdb",
			target:    TargetConfig{Type: "duckdb"},
			wantErr:   false,
			errSubstr: "",
		},
		{
			name:      "valid duckdb uppercase",
			target:    TargetConfig{Type: "DuckDB"},
			wantErr:   false,
			errSubstr: "",
		},
		{
			name:      "unknown type mysql",
			target:    TargetConfig{Type: "mysql"},
			wantErr:   true,
			errSubstr: "unknown adapter type",
		},
		{
			name:      "unknown type postgres (not yet implemented)",
			target:    TargetConfig{Type: "postgres"},
			wantErr:   true,
			errSubstr: "unknown adapter type",
		},
		{
			name:      "unknown type snowflake (not yet implemented)",
			target:    TargetConfig{Type: "snowflake"},
			wantErr:   true,
			errSubstr: "unknown adapter type",
		},
		{
			name:      "unknown type bigquery (not yet implemented)",
			target:    TargetConfig{Type: "bigquery"},
			wantErr:   true,
			errSubstr: "unknown adapter type",
		},
		{
			name:      "unknown type redshift",
			target:    TargetConfig{Type: "redshift"},
			wantErr:   true,
			errSubstr: "unknown adapter type",
		},
		{
			name:      "unknown type oracle",
			target:    TargetConfig{Type: "oracle"},
			wantErr:   true,
			errSubstr: "unknown adapter type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.target.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errSubstr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestTargetConfig_Validate_ErrorContainsAvailable verifies that validation errors
// include the list of available adapters.
func TestTargetConfig_Validate_ErrorContainsAvailable(t *testing.T) {
	target := TargetConfig{Type: "invalid_db"}
	err := target.Validate()
	if err == nil {
		t.Fatal("expected error for invalid type")
	}

	errStr := err.Error()
	// Should mention available adapters
	if !strings.Contains(errStr, "duckdb") {
		t.Errorf("error should list available adapters, got: %s", errStr)
	}
	// Should mention the config file
	if !strings.Contains(errStr, "leapsql.yaml") {
		t.Errorf("error should mention config file, got: %s", errStr)
	}
}

// TestDefaultSchemaForType tests the DefaultSchemaForType function.
func TestDefaultSchemaForType(t *testing.T) {
	tests := []struct {
		dbType   string
		expected string
	}{
		{"duckdb", "main"},
		{"DuckDB", "main"},
		{"DUCKDB", "main"},
		{"postgres", "public"},
		{"postgresql", "public"},
		{"snowflake", "PUBLIC"},
		{"bigquery", ""},
		{"unknown", "main"}, // Default fallback
		{"", "main"},        // Empty string fallback
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			got := DefaultSchemaForType(tt.dbType)
			if got != tt.expected {
				t.Errorf("DefaultSchemaForType(%q) = %q, want %q", tt.dbType, got, tt.expected)
			}
		})
	}
}

// TestExpandEnvVars tests the expandEnvVars function.
func TestExpandEnvVars(t *testing.T) {
	// Set test environment variables
	os.Setenv("TEST_VAR_ONE", "value_one")
	os.Setenv("TEST_VAR_TWO", "value_two")
	defer func() {
		os.Unsetenv("TEST_VAR_ONE")
		os.Unsetenv("TEST_VAR_TWO")
	}()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single variable",
			input:    "${TEST_VAR_ONE}",
			expected: "value_one",
		},
		{
			name:     "multiple variables",
			input:    "${TEST_VAR_ONE}/${TEST_VAR_TWO}",
			expected: "value_one/value_two",
		},
		{
			name:     "variable in path",
			input:    "/path/to/${TEST_VAR_ONE}/file",
			expected: "/path/to/value_one/file",
		},
		{
			name:     "unset variable stays as-is",
			input:    "${UNSET_VARIABLE}",
			expected: "${UNSET_VARIABLE}",
		},
		{
			name:     "no variables",
			input:    "plain string",
			expected: "plain string",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "mixed set and unset",
			input:    "${TEST_VAR_ONE}:${UNSET_VAR}",
			expected: "value_one:${UNSET_VAR}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := expandEnvVars(tt.input)
			if got != tt.expected {
				t.Errorf("expandEnvVars(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// TestMergeTargetConfig tests the mergeTargetConfig function.
func TestMergeTargetConfig(t *testing.T) {
	t.Run("nil base returns override", func(t *testing.T) {
		override := &TargetConfig{Type: "duckdb", Database: "test.db"}
		result := mergeTargetConfig(nil, override)
		if result != override {
			t.Error("nil base should return override")
		}
	})

	t.Run("nil override returns base", func(t *testing.T) {
		base := &TargetConfig{Type: "duckdb", Database: "test.db"}
		result := mergeTargetConfig(base, nil)
		if result != base {
			t.Error("nil override should return base")
		}
	})

	t.Run("both nil returns nil", func(t *testing.T) {
		result := mergeTargetConfig(nil, nil)
		if result != nil {
			t.Error("both nil should return nil")
		}
	})

	t.Run("override replaces base fields", func(t *testing.T) {
		base := &TargetConfig{
			Type:     "duckdb",
			Database: "base.db",
			Schema:   "main",
			Host:     "localhost",
		}
		override := &TargetConfig{
			Database: "override.db",
			Schema:   "custom",
		}

		result := mergeTargetConfig(base, override)

		if result.Type != "duckdb" {
			t.Errorf("Type should be inherited from base, got %q", result.Type)
		}
		if result.Database != "override.db" {
			t.Errorf("Database should be from override, got %q", result.Database)
		}
		if result.Schema != "custom" {
			t.Errorf("Schema should be from override, got %q", result.Schema)
		}
		if result.Host != "localhost" {
			t.Errorf("Host should be inherited from base, got %q", result.Host)
		}
	})

	t.Run("options are merged", func(t *testing.T) {
		base := &TargetConfig{
			Type: "duckdb",
			Options: map[string]string{
				"key1": "base_value1",
				"key2": "base_value2",
			},
		}
		override := &TargetConfig{
			Options: map[string]string{
				"key2": "override_value2",
				"key3": "override_value3",
			},
		}

		result := mergeTargetConfig(base, override)

		if result.Options["key1"] != "base_value1" {
			t.Errorf("key1 should be from base, got %q", result.Options["key1"])
		}
		if result.Options["key2"] != "override_value2" {
			t.Errorf("key2 should be from override, got %q", result.Options["key2"])
		}
		if result.Options["key3"] != "override_value3" {
			t.Errorf("key3 should be from override, got %q", result.Options["key3"])
		}
	})
}

// TestTargetConfig_ApplyDefaults tests the ApplyDefaults method.
func TestTargetConfig_ApplyDefaults(t *testing.T) {
	t.Run("sets default schema for duckdb", func(t *testing.T) {
		target := &TargetConfig{Type: "duckdb"}
		target.ApplyDefaults()
		if target.Schema != "main" {
			t.Errorf("expected schema 'main', got %q", target.Schema)
		}
	})

	t.Run("preserves existing schema", func(t *testing.T) {
		target := &TargetConfig{Type: "duckdb", Schema: "custom"}
		target.ApplyDefaults()
		if target.Schema != "custom" {
			t.Errorf("expected schema 'custom' to be preserved, got %q", target.Schema)
		}
	})
}

// TestLoadConfigWithTarget_Fixtures tests LoadConfigWithTarget using fixture files.
func TestLoadConfigWithTarget_Fixtures(t *testing.T) {
	testdataDir := filepath.Join("testdata")

	t.Run("valid duckdb config", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_duckdb.yaml")
		cfg, err := LoadConfigWithTarget(cfgPath, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Target.Type != "duckdb" {
			t.Errorf("expected target type 'duckdb', got %q", cfg.Target.Type)
		}
		if cfg.Target.Database != ":memory:" {
			t.Errorf("expected database ':memory:', got %q", cfg.Target.Database)
		}
		if cfg.Target.Schema != "main" {
			t.Errorf("expected schema 'main', got %q", cfg.Target.Schema)
		}
	})

	t.Run("valid config with environments", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		// Load with default environment (dev)
		cfg, err := LoadConfigWithTarget(cfgPath, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Target.Database != "dev.duckdb" {
			t.Errorf("expected database 'dev.duckdb', got %q", cfg.Target.Database)
		}
	})

	t.Run("config with target override to staging", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		cfg, err := LoadConfigWithTarget(cfgPath, "staging")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Target.Database != "staging.duckdb" {
			t.Errorf("expected database 'staging.duckdb', got %q", cfg.Target.Database)
		}
		if cfg.Target.Schema != "staging" {
			t.Errorf("expected schema 'staging', got %q", cfg.Target.Schema)
		}
	})

	t.Run("config with target override to prod", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		cfg, err := LoadConfigWithTarget(cfgPath, "prod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Target.Database != "prod.duckdb" {
			t.Errorf("expected database 'prod.duckdb', got %q", cfg.Target.Database)
		}
		if cfg.Target.Schema != "prod" {
			t.Errorf("expected schema 'prod', got %q", cfg.Target.Schema)
		}
	})

	t.Run("invalid unknown type", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "invalid_unknown_type.yaml")
		_, err := LoadConfigWithTarget(cfgPath, "")
		if err == nil {
			t.Fatal("expected error for unknown type, got nil")
		}

		if !strings.Contains(err.Error(), "invalid target configuration") {
			t.Errorf("error should mention invalid target configuration, got: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "mysql") {
			t.Errorf("error should mention the invalid type 'mysql', got: %s", err.Error())
		}
	})

	t.Run("invalid empty type", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "invalid_empty_type.yaml")
		_, err := LoadConfigWithTarget(cfgPath, "")
		if err == nil {
			t.Fatal("expected error for empty type, got nil")
		}

		if !strings.Contains(err.Error(), "target type is required") {
			t.Errorf("error should mention type is required, got: %s", err.Error())
		}
	})

	t.Run("config with env vars", func(t *testing.T) {
		// Set test env vars
		os.Setenv("TEST_DB_PATH", "/path/to/test.db")
		os.Setenv("TEST_DB_USER", "testuser")
		os.Setenv("TEST_DB_PASSWORD", "secret123")
		defer func() {
			os.Unsetenv("TEST_DB_PATH")
			os.Unsetenv("TEST_DB_USER")
			os.Unsetenv("TEST_DB_PASSWORD")
		}()

		cfgPath := filepath.Join(testdataDir, "valid_env_vars.yaml")
		cfg, err := LoadConfigWithTarget(cfgPath, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Target.Database != "/path/to/test.db" {
			t.Errorf("expected database '/path/to/test.db', got %q", cfg.Target.Database)
		}
		if cfg.Target.User != "testuser" {
			t.Errorf("expected user 'testuser', got %q", cfg.Target.User)
		}
		if cfg.Target.Password != "secret123" {
			t.Errorf("expected password 'secret123', got %q", cfg.Target.Password)
		}
	})
}

// TestLoadConfigWithTarget_NonexistentEnvironment tests loading with a non-existent environment.
func TestLoadConfigWithTarget_NonexistentEnvironment(t *testing.T) {
	testdataDir := filepath.Join("testdata")
	cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

	// Load with non-existent environment - should still work, using base target
	cfg, err := LoadConfigWithTarget(cfgPath, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should fall back to the base target config
	if cfg.Target.Type != "duckdb" {
		t.Errorf("expected type 'duckdb', got %q", cfg.Target.Type)
	}
}

// TestConfig_Validate tests the Config.Validate method.
func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{ModelsDir: "models"}
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("empty models_dir", func(t *testing.T) {
		cfg := &Config{ModelsDir: ""}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected error for empty models_dir")
		}
		if !strings.Contains(err.Error(), "models_dir is required") {
			t.Errorf("error should mention models_dir, got: %s", err.Error())
		}
	})
}
