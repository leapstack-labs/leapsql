package commands

import (
	"bytes"
	"strings"
	"testing"
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
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			output := buf.String()
			for _, want := range tt.wantOut {
				if !strings.Contains(output, want) {
					t.Errorf("output should contain %q, got: %s", want, output)
				}
			}
		})
	}
}

func TestVersionCommandMetadata(t *testing.T) {
	cmd := NewVersionCommand("test")

	if cmd.Use != "version" {
		t.Errorf("Use = %q, want %q", cmd.Use, "version")
	}

	if cmd.Short == "" {
		t.Error("Short should not be empty")
	}

	if cmd.Long == "" {
		t.Error("Long should not be empty")
	}
}
