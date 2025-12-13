package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
			setupDir: func(t *testing.T, dir string) {
				os.WriteFile(filepath.Join(dir, "leapsql.yaml"), []byte("existing"), 0644)
			},
			args:    []string{},
			wantErr: true,
		},
		{
			name: "init existing config with force",
			setupDir: func(t *testing.T, dir string) {
				os.WriteFile(filepath.Join(dir, "leapsql.yaml"), []byte("existing"), 0644)
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
			os.Chdir(tmpDir)
			defer os.Chdir(oldWd)

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
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check expected files exist
			for _, f := range tt.wantFiles {
				path := filepath.Join(tmpDir, f)
				if _, err := os.Stat(path); os.IsNotExist(err) {
					t.Errorf("expected file/dir %q to exist", f)
				}
			}
		})
	}
}

func TestInitCommandMetadata(t *testing.T) {
	cmd := NewInitCommand()

	if cmd.Use != "init [directory]" {
		t.Errorf("Use = %q, want %q", cmd.Use, "init [directory]")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	// Verify --force flag exists
	if cmd.Flags().Lookup("force") == nil {
		t.Error("--force flag should exist")
	}
}

func TestInitCreatesValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cmd := NewInitCommand()
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Read and verify config content
	content, err := os.ReadFile("leapsql.yaml")
	if err != nil {
		t.Fatalf("failed to read leapsql.yaml: %v", err)
	}

	expectedContents := []string{
		"models_dir: models",
		"seeds_dir: seeds",
		"macros_dir: macros",
		"state_path:",
	}

	for _, expected := range expectedContents {
		if !strings.Contains(string(content), expected) {
			t.Errorf("config should contain %q", expected)
		}
	}
}
