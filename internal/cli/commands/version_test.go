package commands

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewVersionCommand(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantOut []string
		wantErr bool
	}{
		{
			name:    "default version",
			version: "0.1.0",
			wantOut: []string{"LeapSQL v0.1.0", "DuckDB"},
		},
		{
			name:    "custom version",
			version: "1.2.3",
			wantOut: []string{"LeapSQL v1.2.3"},
		},
		{
			name:    "dev version",
			version: "dev",
			wantOut: []string{"LeapSQL vdev"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewVersionCommand(tt.version)
			buf := new(bytes.Buffer)
			cmd.SetOut(buf)
			cmd.SetErr(buf)

			err := cmd.Execute()
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			output := buf.String()
			for _, want := range tt.wantOut {
				assert.Contains(t, output, want, "output should contain %q, got: %s", want, output)
			}
		})
	}
}

func TestVersionCommandMetadata(t *testing.T) {
	cmd := NewVersionCommand("test")

	assert.Equal(t, "version", cmd.Use)
	assert.NotEmpty(t, cmd.Short, "Short should not be empty")
	assert.NotEmpty(t, cmd.Long, "Long should not be empty")
}
