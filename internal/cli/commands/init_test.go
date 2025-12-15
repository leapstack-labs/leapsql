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
				"models/marts",
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
