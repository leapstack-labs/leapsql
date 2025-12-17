package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInitCommand(t *testing.T) {
	tests := []struct {
		name      string
		setupDir  func(t *testing.T, dir string) // setup before running
		args      []string
		wantErr   bool
		wantFiles []string
	}{
		{
			name:    "init empty directory",
			args:    []string{},
			wantErr: false,
			wantFiles: []string{
				"leapsql.yaml",
				"models",
				"seeds",
				"macros",
				"models/staging",
				"models/staging/stg_example.sql",
				".gitignore",
			},
		},
		{
			name: "init existing config without force",
			setupDir: func(_ *testing.T, dir string) {
				_ = os.WriteFile(filepath.Join(dir, "leapsql.yaml"), []byte("existing"), 0600)
			},
			args:    []string{},
			wantErr: true,
		},
		{
			name: "init existing config with force",
			setupDir: func(_ *testing.T, dir string) {
				_ = os.WriteFile(filepath.Join(dir, "leapsql.yaml"), []byte("existing"), 0600)
			},
			args:    []string{"--force"},
			wantErr: false,
			wantFiles: []string{
				"leapsql.yaml",
				"models",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory and change to it
			tmpDir := t.TempDir()
			oldWd, _ := os.Getwd()
			require.NoError(t, os.Chdir(tmpDir))
			defer func() { _ = os.Chdir(oldWd) }()

			// Run setup if provided
			if tt.setupDir != nil {
				tt.setupDir(t, tmpDir)
			}

			cmd := NewInitCommand()
			buf := new(bytes.Buffer)
			cmd.SetOut(buf)
			cmd.SetErr(buf)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Check expected files exist
			for _, f := range tt.wantFiles {
				path := filepath.Join(tmpDir, f)
				_, err := os.Stat(path)
				assert.False(t, os.IsNotExist(err), "expected file/dir %q to exist", f)
			}
		})
	}
}

func TestInitCommandMetadata(t *testing.T) {
	cmd := NewInitCommand()

	assert.Equal(t, "init [directory]", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")
	assert.NotNil(t, cmd.Flags().Lookup("force"), "--force flag should exist")
}

func TestInitCreatesValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer func() { _ = os.Chdir(oldWd) }()

	cmd := NewInitCommand()
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	err := cmd.Execute()
	require.NoError(t, err)

	// Read and verify config content
	content, err := os.ReadFile("leapsql.yaml")
	require.NoError(t, err, "failed to read leapsql.yaml")

	expectedContents := []string{
		"models_dir: models",
		"seeds_dir: seeds",
		"macros_dir: macros",
		"state_path:",
	}

	for _, expected := range expectedContents {
		assert.Contains(t, string(content), expected, "config should contain %q", expected)
	}
}

func TestInitExample(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer func() { _ = os.Chdir(oldWd) }()

	cmd := NewInitCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--example"})

	err := cmd.Execute()
	require.NoError(t, err)

	// Check all expected files exist
	expectedFiles := []string{
		"leapsql.yaml",
		".gitignore",
		"README.md",
		"seeds/raw_customers.csv",
		"seeds/raw_orders.csv",
		"seeds/raw_products.csv",
		"models/staging/stg_customers.sql",
		"models/staging/stg_orders.sql",
		"models/staging/stg_products.sql",
		"models/marts/dim_customers.sql",
		"models/marts/fct_orders.sql",
		"macros/utils.star",
	}

	for _, f := range expectedFiles {
		path := filepath.Join(tmpDir, f)
		_, err := os.Stat(path)
		assert.False(t, os.IsNotExist(err), "expected file %q to exist", f)
	}

	// Verify config content
	content, err := os.ReadFile("leapsql.yaml")
	require.NoError(t, err)
	assert.Contains(t, string(content), "warehouse.duckdb", "example config should use warehouse.duckdb")
}

func TestTemplateFS(t *testing.T) {
	t.Run("minimal template exists", func(t *testing.T) {
		files, err := listTemplateFiles("minimal")
		require.NoError(t, err)
		assert.NotEmpty(t, files, "minimal template should have files")

		// Should contain these files
		hasConfig := false
		hasModel := false
		hasGitignore := false
		for _, f := range files {
			switch f {
			case "leapsql.yaml":
				hasConfig = true
			case filepath.Join("models", "staging", "stg_example.sql"):
				hasModel = true
			case ".gitignore":
				hasGitignore = true
			}
		}
		assert.True(t, hasConfig, "minimal template should have leapsql.yaml")
		assert.True(t, hasModel, "minimal template should have models/staging/stg_example.sql")
		assert.True(t, hasGitignore, "minimal template should have .gitignore")
	})

	t.Run("example template exists", func(t *testing.T) {
		files, err := listTemplateFiles("example")
		require.NoError(t, err)
		assert.NotEmpty(t, files, "example template should have files")

		// Should have more files than minimal
		minFiles, _ := listTemplateFiles("minimal")
		assert.Greater(t, len(files), len(minFiles), "example should have more files than minimal")
	})
}

func TestRenameSpecialFiles(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"gitignore", ".gitignore"},
		{"README.md", "README.md"},
		{filepath.Join("some", "dir", "gitignore"), filepath.Join("some", "dir", ".gitignore")},
		{filepath.Join("models", "staging", "stg_example.sql"), filepath.Join("models", "staging", "stg_example.sql")},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := renameSpecialFiles(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGroupTemplateFiles(t *testing.T) {
	files := []string{
		"leapsql.yaml",
		".gitignore",
		"README.md",
		"seeds/raw_customers.csv",
		"seeds/raw_orders.csv",
		"models/staging/stg_customers.sql",
		"models/marts/dim_customers.sql",
		"macros/utils.star",
	}

	groups := groupTemplateFiles(files)

	assert.Len(t, groups["config"], 3, "should have 3 config files")
	assert.Len(t, groups["seeds"], 2, "should have 2 seed files")
	assert.Len(t, groups["models"], 2, "should have 2 model files")
	assert.Len(t, groups["macros"], 1, "should have 1 macro file")
}

func TestCopyTemplate(t *testing.T) {
	t.Run("copies minimal template", func(t *testing.T) {
		tmpDir := t.TempDir()

		err := copyTemplate("minimal", tmpDir, false)
		require.NoError(t, err)

		// Verify files were created
		_, err = os.Stat(filepath.Join(tmpDir, "leapsql.yaml"))
		require.NoError(t, err, "leapsql.yaml should exist")

		_, err = os.Stat(filepath.Join(tmpDir, ".gitignore"))
		require.NoError(t, err, ".gitignore should exist (renamed from gitignore)")

		_, err = os.Stat(filepath.Join(tmpDir, "models", "staging", "stg_example.sql"))
		require.NoError(t, err, "stg_example.sql should exist")
	})

	t.Run("skips existing files without force", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create existing file with different content
		configPath := filepath.Join(tmpDir, "leapsql.yaml")
		err := os.WriteFile(configPath, []byte("original content"), 0600)
		require.NoError(t, err)

		err = copyTemplate("minimal", tmpDir, false)
		require.NoError(t, err)

		// Verify original content preserved
		content, err := os.ReadFile(configPath)
		require.NoError(t, err)
		assert.Equal(t, "original content", string(content), "existing file should not be overwritten")
	})

	t.Run("overwrites existing files with force", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create existing file with different content
		configPath := filepath.Join(tmpDir, "leapsql.yaml")
		err := os.WriteFile(configPath, []byte("original content"), 0600)
		require.NoError(t, err)

		err = copyTemplate("minimal", tmpDir, true)
		require.NoError(t, err)

		// Verify content was overwritten
		content, err := os.ReadFile(configPath)
		require.NoError(t, err)
		assert.Contains(t, string(content), "models_dir:", "file should be overwritten with template content")
	})
}
