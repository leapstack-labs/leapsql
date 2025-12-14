package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDAGCommand(t *testing.T) {
	cmd := NewDAGCommand()

	assert.Equal(t, "dag", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")
	assert.NotEmpty(t, cmd.Long, "Long should not be empty")
	assert.NotEmpty(t, cmd.Example, "Example should not be empty")
}
