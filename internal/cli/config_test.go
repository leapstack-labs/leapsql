package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
				require.Error(t, err, "expected error but got nil")
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestTargetConfig_Validate_ErrorContainsAvailable verifies that validation errors
// include the list of available adapters.
func TestTargetConfig_Validate_ErrorContainsAvailable(t *testing.T) {
	target := TargetConfig{Type: "invalid_db"}
	err := target.Validate()
	require.Error(t, err, "expected error for invalid type")

	errStr := err.Error()
	// Should mention available adapters
	assert.Contains(t, errStr, "duckdb", "error should list available adapters")
	// Should mention the config file
	assert.Contains(t, errStr, "leapsql.yaml", "error should mention config file")
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
			assert.Equal(t, tt.expected, got)
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
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestMergeTargetConfig tests the mergeTargetConfig function.
func TestMergeTargetConfig(t *testing.T) {
	t.Run("nil base returns override", func(t *testing.T) {
		override := &TargetConfig{Type: "duckdb", Database: "test.db"}
		result := mergeTargetConfig(nil, override)
		assert.Equal(t, override, result, "nil base should return override")
	})

	t.Run("nil override returns base", func(t *testing.T) {
		base := &TargetConfig{Type: "duckdb", Database: "test.db"}
		result := mergeTargetConfig(base, nil)
		assert.Equal(t, base, result, "nil override should return base")
	})

	t.Run("both nil returns nil", func(t *testing.T) {
		result := mergeTargetConfig(nil, nil)
		assert.Nil(t, result, "both nil should return nil")
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

		assert.Equal(t, "duckdb", result.Type, "Type should be inherited from base")
		assert.Equal(t, "override.db", result.Database, "Database should be from override")
		assert.Equal(t, "custom", result.Schema, "Schema should be from override")
		assert.Equal(t, "localhost", result.Host, "Host should be inherited from base")
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

		assert.Equal(t, "base_value1", result.Options["key1"], "key1 should be from base")
		assert.Equal(t, "override_value2", result.Options["key2"], "key2 should be from override")
		assert.Equal(t, "override_value3", result.Options["key3"], "key3 should be from override")
	})
}

// TestTargetConfig_ApplyDefaults tests the ApplyDefaults method.
func TestTargetConfig_ApplyDefaults(t *testing.T) {
	t.Run("sets default schema for duckdb", func(t *testing.T) {
		target := &TargetConfig{Type: "duckdb"}
		target.ApplyDefaults()
		assert.Equal(t, "main", target.Schema)
	})

	t.Run("preserves existing schema", func(t *testing.T) {
		target := &TargetConfig{Type: "duckdb", Schema: "custom"}
		target.ApplyDefaults()
		assert.Equal(t, "custom", target.Schema)
	})
}

// TestLoadConfigWithTarget_Fixtures tests LoadConfigWithTarget using fixture files.
func TestLoadConfigWithTarget_Fixtures(t *testing.T) {
	testdataDir := filepath.Join("testdata")

	t.Run("valid duckdb config", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_duckdb.yaml")
		cfg, err := LoadConfigWithTarget(cfgPath, "")
		require.NoError(t, err)

		assert.Equal(t, "duckdb", cfg.Target.Type)
		assert.Equal(t, ":memory:", cfg.Target.Database)
		assert.Equal(t, "main", cfg.Target.Schema)
	})

	t.Run("valid config with environments", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		// Load with default environment (dev)
		cfg, err := LoadConfigWithTarget(cfgPath, "")
		require.NoError(t, err)

		assert.Equal(t, "dev.duckdb", cfg.Target.Database)
	})

	t.Run("config with target override to staging", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		cfg, err := LoadConfigWithTarget(cfgPath, "staging")
		require.NoError(t, err)

		assert.Equal(t, "staging.duckdb", cfg.Target.Database)
		assert.Equal(t, "staging", cfg.Target.Schema)
	})

	t.Run("config with target override to prod", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		cfg, err := LoadConfigWithTarget(cfgPath, "prod")
		require.NoError(t, err)

		assert.Equal(t, "prod.duckdb", cfg.Target.Database)
		assert.Equal(t, "prod", cfg.Target.Schema)
	})

	t.Run("invalid unknown type", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "invalid_unknown_type.yaml")
		_, err := LoadConfigWithTarget(cfgPath, "")
		require.Error(t, err, "expected error for unknown type")

		assert.Contains(t, err.Error(), "invalid target configuration")
		assert.Contains(t, err.Error(), "mysql")
	})

	t.Run("invalid empty type", func(t *testing.T) {
		cfgPath := filepath.Join(testdataDir, "invalid_empty_type.yaml")
		_, err := LoadConfigWithTarget(cfgPath, "")
		require.Error(t, err, "expected error for empty type")

		assert.Contains(t, err.Error(), "target type is required")
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
		require.NoError(t, err)

		assert.Equal(t, "/path/to/test.db", cfg.Target.Database)
		assert.Equal(t, "testuser", cfg.Target.User)
		assert.Equal(t, "secret123", cfg.Target.Password)
	})
}

// TestLoadConfigWithTarget_NonexistentEnvironment tests loading with a non-existent environment.
func TestLoadConfigWithTarget_NonexistentEnvironment(t *testing.T) {
	testdataDir := filepath.Join("testdata")
	cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

	// Load with non-existent environment - should still work, using base target
	cfg, err := LoadConfigWithTarget(cfgPath, "nonexistent")
	require.NoError(t, err)

	// Should fall back to the base target config
	assert.Equal(t, "duckdb", cfg.Target.Type)
}

// TestConfig_Validate tests the Config.Validate method.
func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{ModelsDir: "models"}
		assert.NoError(t, cfg.Validate())
	})

	t.Run("empty models_dir", func(t *testing.T) {
		cfg := &Config{ModelsDir: ""}
		err := cfg.Validate()
		require.Error(t, err, "expected error for empty models_dir")
		assert.True(t, strings.Contains(err.Error(), "models_dir is required"), "error should mention models_dir")
	})
}
