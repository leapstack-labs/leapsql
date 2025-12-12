// Package main provides tests for the LeapSQL CLI.
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/cli"
)

func testdataDir(t *testing.T) string {
	t.Helper()
	// Get the absolute path to testdata directory
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	return filepath.Join(wd, "..", "..", "testdata")
}

func TestVersionCommand(t *testing.T) {
	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"version"})

	err := cmd.Execute()
	if err != nil {
		t.Errorf("version command error = %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "LeapSQL") {
		t.Errorf("version output should contain 'LeapSQL', got: %s", output)
	}
}

func TestHelpCommand(t *testing.T) {
	cmd := cli.NewRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()
	if err != nil {
		t.Errorf("help command error = %v", err)
	}

	output := buf.String()
	expectedCommands := []string{"run", "list", "dag", "seed", "lineage", "render", "docs"}
	for _, expected := range expectedCommands {
		if !strings.Contains(output, expected) {
			t.Errorf("help output should contain '%s', got: %s", expected, output)
		}
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
	if err != nil {
		t.Errorf("list command error = %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "Models") {
		t.Errorf("list output should contain 'Models', got: %s", output)
	}
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
	if err != nil {
		t.Errorf("list --json command error = %v", err)
	}

	// JSON output goes to stdout, not the buffer
	// So we just verify no error occurred
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
	if err != nil {
		t.Errorf("dag command error = %v", err)
	}
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
	if err != nil {
		t.Errorf("seed command error = %v", err)
	}
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
	if err != nil {
		t.Errorf("run command error = %v", err)
	}
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
	if err != nil {
		t.Errorf("run --select command error = %v", err)
	}
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
	if err != nil {
		t.Fatalf("initial run command error = %v", err)
	}

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
	if err != nil {
		t.Errorf("run --select --downstream command error = %v", err)
	}
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
	if err != nil {
		t.Errorf("lineage command error = %v", err)
	}
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
	if err != nil {
		t.Errorf("render command error = %v", err)
	}
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
			if err != nil {
				t.Errorf("completion %s command error = %v", shell, err)
			}
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
	if err == nil {
		t.Error("unknown command should return an error")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
