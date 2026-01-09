// Package main provides tests for the LeapSQL CLI.
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/cli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testdataDir(t *testing.T) string {
	t.Helper()
	// Get the absolute path to testdata directory
	wd, err := os.Getwd()
	require.NoError(t, err, "failed to get working directory")
	return filepath.Join(wd, "..", "..", "testdata")
}

func TestVersionCommand(t *testing.T) {
	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"version"})

	err := cmd.Execute()
	require.NoError(t, err, "version command error")

	output := buf.String()
	assert.Contains(t, output, "LeapSQL", "version output should contain 'LeapSQL'")
}

func TestHelpCommand(t *testing.T) {
	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()
	require.NoError(t, err, "help command error")

	output := buf.String()
	expectedCommands := []string{"run", "list", "dag", "seed", "lineage", "render"}
	for _, expected := range expectedCommands {
		assert.Contains(t, output, expected, "help output should contain '%s'", expected)
	}
}

func TestListCommand(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"list",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	require.NoError(t, err, "list command error")

	output := buf.String()
	assert.Contains(t, output, "Models", "list output should contain 'Models'")
}

func TestListCommandJSON(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"list",
		"--output", "json",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	assert.NoError(t, err, "list --json command error")

	// Validate JSON structure
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result), "output is not valid JSON")

	// Verify expected top-level keys exist
	expectedKeys := []string{"models", "macros", "summary"}
	for _, key := range expectedKeys {
		assert.Contains(t, result, key, "JSON output should contain key %q", key)
	}
}

func TestDAGCommand(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"dag",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	assert.NoError(t, err, "dag command error")
}

func TestSeedCommand(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"seed",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	assert.NoError(t, err, "seed command error")
}

func TestRunCommand(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
		"--env", "test",
	})

	err := cmd.Execute()
	assert.NoError(t, err, "run command error")
}

func TestRunCommandSelect(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"run",
		"--select", "staging.stg_customers",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
		"--env", "test",
	})

	err := cmd.Execute()
	assert.NoError(t, err, "run --select command error")
}

func TestRunCommandSelectWithDownstream(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// First run all models to create base tables
	cmd := cli.NewRootCmd()
	cmd.SetArgs([]string{
		"run",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
		"--database", dbPath,
		"--env", "test",
	})

	err := cmd.Execute()
	require.NoError(t, err, "initial run command error")

	// Now test select with downstream
	cmd2 := cli.NewRootCmd()
	cmd2.SetArgs([]string{
		"run",
		"--select", "staging.stg_customers",
		"--downstream",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
		"--database", dbPath,
		"--env", "test",
	})

	err = cmd2.Execute()
	assert.NoError(t, err, "run --select --downstream command error")
}

func TestLineageCommand(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"lineage", "staging.stg_customers",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	assert.NoError(t, err, "lineage command error")
}

func TestLineageCommandInvalidModel(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"lineage", "nonexistent.model",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	require.Error(t, err, "lineage with invalid model should return an error")
	assert.Contains(t, err.Error(), "not found", "error should mention 'not found'")
}

func TestRenderCommand(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"render", "staging.stg_customers",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	assert.NoError(t, err, "render command error")
}

func TestRenderCommandInvalidModel(t *testing.T) {
	td := testdataDir(t)
	tmpDir := t.TempDir()

	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"render", "nonexistent.model",
		"--models-dir", filepath.Join(td, "models"),
		"--seeds-dir", filepath.Join(td, "seeds"),
		"--macros-dir", filepath.Join(td, "macros"),
		"--state", filepath.Join(tmpDir, "state.db"),
	})

	err := cmd.Execute()
	assert.Error(t, err, "render with invalid model should return an error")
}

func TestCompletionCommand(t *testing.T) {
	shells := []string{"bash", "zsh", "fish", "powershell"}

	for _, shell := range shells {
		t.Run(shell, func(t *testing.T) {
			cmd := cli.NewRootCmd()
			buf := new(bytes.Buffer)
			cmd.SetOut(buf)
			cmd.SetErr(buf)
			cmd.SetArgs([]string{"completion", shell})

			err := cmd.Execute()
			assert.NoError(t, err, "completion %s command error", shell)
		})
	}
}

func TestUnknownCommand(t *testing.T) {
	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"unknown-command"})

	err := cmd.Execute()
	assert.Error(t, err, "unknown command should return an error")
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
