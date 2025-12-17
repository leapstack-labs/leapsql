package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import adapter packages to ensure adapters are registered via init()
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb"
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/postgres"
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
			name:      "valid postgres",
			target:    TargetConfig{Type: "postgres"},
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
		// Note: "postgresql" is not a registered dialect name, so it falls back to "main"
		// The registered dialect name is "postgres"
		{"postgresql", "main"},
		// Unregistered dialects fall back to "main"
		{"snowflake", "main"},
		{"bigquery", "main"},
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
	require.NoError(t, os.Setenv("TEST_VAR_ONE", "value_one"))
	require.NoError(t, os.Setenv("TEST_VAR_TWO", "value_two"))
	defer func() {
		_ = os.Unsetenv("TEST_VAR_ONE")
		_ = os.Unsetenv("TEST_VAR_TWO")
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

// TestMergeTargetConfig tests the MergeTargetConfig function.
func TestMergeTargetConfig(t *testing.T) {
	t.Run("nil base returns override", func(t *testing.T) {
		override := &TargetConfig{Type: "duckdb", Database: "test.db"}
		result := MergeTargetConfig(nil, override)
		assert.Equal(t, override, result, "nil base should return override")
	})

	t.Run("nil override returns base", func(t *testing.T) {
		base := &TargetConfig{Type: "duckdb", Database: "test.db"}
		result := MergeTargetConfig(base, nil)
		assert.Equal(t, base, result, "nil override should return base")
	})

	t.Run("both nil returns nil", func(t *testing.T) {
		result := MergeTargetConfig(nil, nil)
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

		result := MergeTargetConfig(base, override)

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

		result := MergeTargetConfig(base, override)

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
	// Reset config before each test
	ResetConfig()

	testdataDir := "../testdata"

	t.Run("valid duckdb config", func(t *testing.T) {
		ResetConfig()
		cfgPath := filepath.Join(testdataDir, "valid_duckdb.yaml")
		cfg, err := LoadConfigWithTarget(cfgPath, "", nil)
		require.NoError(t, err)

		assert.Equal(t, "duckdb", cfg.Target.Type)
		assert.Equal(t, ":memory:", cfg.Target.Database)
		assert.Equal(t, "main", cfg.Target.Schema)
	})

	t.Run("valid config with environments", func(t *testing.T) {
		ResetConfig()
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		// Load with default environment (dev)
		cfg, err := LoadConfigWithTarget(cfgPath, "", nil)
		require.NoError(t, err)

		assert.Equal(t, "dev.duckdb", cfg.Target.Database)
	})

	t.Run("config with target override to staging", func(t *testing.T) {
		ResetConfig()
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		cfg, err := LoadConfigWithTarget(cfgPath, "staging", nil)
		require.NoError(t, err)

		assert.Equal(t, "staging.duckdb", cfg.Target.Database)
		assert.Equal(t, "staging", cfg.Target.Schema)
	})

	t.Run("config with target override to prod", func(t *testing.T) {
		ResetConfig()
		cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

		cfg, err := LoadConfigWithTarget(cfgPath, "prod", nil)
		require.NoError(t, err)

		assert.Equal(t, "prod.duckdb", cfg.Target.Database)
		assert.Equal(t, "prod", cfg.Target.Schema)
	})

	t.Run("invalid unknown type", func(t *testing.T) {
		ResetConfig()
		cfgPath := filepath.Join(testdataDir, "invalid_unknown_type.yaml")
		_, err := LoadConfigWithTarget(cfgPath, "", nil)
		require.Error(t, err, "expected error for unknown type")

		assert.Contains(t, err.Error(), "invalid target configuration")
		assert.Contains(t, err.Error(), "mysql")
	})

	t.Run("invalid empty type", func(t *testing.T) {
		ResetConfig()
		cfgPath := filepath.Join(testdataDir, "invalid_empty_type.yaml")
		_, err := LoadConfigWithTarget(cfgPath, "", nil)
		require.Error(t, err, "expected error for empty type")

		assert.Contains(t, err.Error(), "target type is required")
	})

	t.Run("config with env vars", func(t *testing.T) {
		ResetConfig()
		// Set test env vars
		require.NoError(t, os.Setenv("TEST_DB_PATH", "/path/to/test.db"))
		require.NoError(t, os.Setenv("TEST_DB_USER", "testuser"))
		require.NoError(t, os.Setenv("TEST_DB_PASSWORD", "secret123"))
		defer func() {
			_ = os.Unsetenv("TEST_DB_PATH")
			_ = os.Unsetenv("TEST_DB_USER")
			_ = os.Unsetenv("TEST_DB_PASSWORD")
		}()

		cfgPath := filepath.Join(testdataDir, "valid_env_vars.yaml")
		cfg, err := LoadConfigWithTarget(cfgPath, "", nil)
		require.NoError(t, err)

		assert.Equal(t, "/path/to/test.db", cfg.Target.Database)
		assert.Equal(t, "testuser", cfg.Target.User)
		assert.Equal(t, "secret123", cfg.Target.Password)
	})
}

// TestLoadConfigWithTarget_NonexistentEnvironment tests loading with a non-existent environment.
func TestLoadConfigWithTarget_NonexistentEnvironment(t *testing.T) {
	ResetConfig()
	testdataDir := "../testdata"
	cfgPath := filepath.Join(testdataDir, "valid_with_envs.yaml")

	// Load with non-existent environment - should still work, using base target
	cfg, err := LoadConfigWithTarget(cfgPath, "nonexistent", nil)
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
		assert.Contains(t, err.Error(), "models_dir is required")
	})
}

// TestLoadConfigWithTarget_FlagPrecedence tests that flags override env vars and config file.
func TestLoadConfigWithTarget_FlagPrecedence(t *testing.T) {
	ResetConfig()

	// Create a temp config file with models_dir = "from_file"
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "leapsql.yaml")
	cfgContent := `models_dir: from_file
target:
  type: duckdb
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfgContent), 0600))

	// Set env var with different value
	require.NoError(t, os.Setenv("LEAPSQL_MODELS_DIR", "from_env"))
	defer func() { _ = os.Unsetenv("LEAPSQL_MODELS_DIR") }()

	// Create flag set with yet another value
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("models-dir", "", "models directory")
	require.NoError(t, flags.Set("models-dir", "from_flag"))

	// Load config
	cfg, err := LoadConfigWithTarget(cfgPath, "", flags)
	require.NoError(t, err)

	// Flag should win
	assert.Equal(t, "from_flag", cfg.ModelsDir, "flag value should override config file and env var")
}

// TestLoadConfigWithTarget_EnvPrecedenceOverFile tests that env vars override config file.
func TestLoadConfigWithTarget_EnvPrecedenceOverFile(t *testing.T) {
	ResetConfig()

	// Create a temp config file
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "leapsql.yaml")
	cfgContent := `models_dir: from_file
target:
  type: duckdb
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfgContent), 0600))

	// Set env var
	require.NoError(t, os.Setenv("LEAPSQL_MODELS_DIR", "from_env"))
	defer func() { _ = os.Unsetenv("LEAPSQL_MODELS_DIR") }()

	// Load config with nil flags
	cfg, err := LoadConfigWithTarget(cfgPath, "", nil)
	require.NoError(t, err)

	// Env should win over file
	assert.Equal(t, "from_env", cfg.ModelsDir, "env var should override config file")
}

// TestLoadConfigWithTarget_FlagNotSetUsesEnv tests that unset flags fall back to env vars.
func TestLoadConfigWithTarget_FlagNotSetUsesEnv(t *testing.T) {
	ResetConfig()

	// Create a temp config file
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "leapsql.yaml")
	cfgContent := `models_dir: from_file
target:
  type: duckdb
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfgContent), 0600))

	// Set env var
	require.NoError(t, os.Setenv("LEAPSQL_MODELS_DIR", "from_env"))
	defer func() { _ = os.Unsetenv("LEAPSQL_MODELS_DIR") }()

	// Create flag set but don't set the flag (Changed will be false)
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("models-dir", "", "models directory")
	// Note: not calling flags.Set(), so Changed is false

	// Load config
	cfg, err := LoadConfigWithTarget(cfgPath, "", flags)
	require.NoError(t, err)

	// Env should win since flag wasn't explicitly set
	assert.Equal(t, "from_env", cfg.ModelsDir, "env var should be used when flag is not set")
}
