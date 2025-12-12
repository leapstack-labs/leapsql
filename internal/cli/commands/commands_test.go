package commands

import (
	"testing"
)

func TestNewLineageCommand(t *testing.T) {
	cmd := NewLineageCommand()

	if cmd.Use != "lineage <model>" {
		t.Errorf("Use = %q, want %q", cmd.Use, "lineage <model>")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	// Verify flags exist
	flags := []string{"output", "upstream", "downstream", "depth"}
	for _, flag := range flags {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("flag %q should exist", flag)
		}
	}
}

func TestNewRenderCommand(t *testing.T) {
	cmd := NewRenderCommand()

	if cmd.Use != "render <model>" {
		t.Errorf("Use = %q, want %q", cmd.Use, "render <model>")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	if cmd.Example == "" {
		t.Error("Example should not be empty")
	}
}

func TestNewListCommand(t *testing.T) {
	cmd := NewListCommand()

	if cmd.Use != "list" {
		t.Errorf("Use = %q, want %q", cmd.Use, "list")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	// Verify output flag exists
	if cmd.Flags().Lookup("output") == nil {
		t.Error("--output flag should exist")
	}
}

func TestNewRunCommand(t *testing.T) {
	cmd := NewRunCommand()

	if cmd.Use != "run" {
		t.Errorf("Use = %q, want %q", cmd.Use, "run")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	// Verify flags exist
	flags := []string{"select", "downstream", "json"}
	for _, flag := range flags {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("flag %q should exist", flag)
		}
	}

	// Verify alias exists
	if len(cmd.Aliases) == 0 || cmd.Aliases[0] != "build" {
		t.Error("run command should have 'build' alias")
	}
}

func TestNewSeedCommand(t *testing.T) {
	cmd := NewSeedCommand()

	if cmd.Use != "seed" {
		t.Errorf("Use = %q, want %q", cmd.Use, "seed")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	if cmd.Example == "" {
		t.Error("Example should not be empty")
	}
}
