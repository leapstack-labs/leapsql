package commands

import (
	"testing"
)

func TestNewDAGCommand(t *testing.T) {
	cmd := NewDAGCommand()

	if cmd.Use != "dag" {
		t.Errorf("Use = %q, want %q", cmd.Use, "dag")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	if cmd.Long == "" {
		t.Error("Long should not be empty")
	}

	if cmd.Example == "" {
		t.Error("Example should not be empty")
	}
}
