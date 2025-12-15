// Package commands_test provides tests for CLI command creation.
package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLineageCommand(t *testing.T) {
	cmd := NewLineageCommand()

	assert.Equal(t, "lineage <model>", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")

	// Verify flags exist (output is a global flag on root, not local)
	flags := []string{"upstream", "downstream", "depth"}
	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %q should exist", flag)
	}
}

func TestNewRenderCommand(t *testing.T) {
	cmd := NewRenderCommand()

	assert.Equal(t, "render <model>", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")
	assert.NotEmpty(t, cmd.Example, "Example should not be empty")
}

func TestNewListCommand(t *testing.T) {
	cmd := NewListCommand()

	assert.Equal(t, "list", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")

	// Note: --output flag is a global persistent flag on root command, not local to list
}

func TestNewRunCommand(t *testing.T) {
	cmd := NewRunCommand()

	assert.Equal(t, "run", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")

	// Verify flags exist
	flags := []string{"select", "downstream", "json"}
	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %q should exist", flag)
	}

	// Verify alias exists
	assert.NotEmpty(t, cmd.Aliases, "run command should have aliases")
	assert.Equal(t, "build", cmd.Aliases[0], "run command should have 'build' alias")
}

func TestNewSeedCommand(t *testing.T) {
	cmd := NewSeedCommand()

	assert.Equal(t, "seed", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")
	assert.NotEmpty(t, cmd.Example, "Example should not be empty")
}
