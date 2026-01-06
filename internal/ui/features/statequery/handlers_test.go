package statequery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"nil", nil, "NULL"},
		{"string", "hello", "hello"},
		{"int", 42, "42"},
		{"int64", int64(100), "100"},
		{"float", 3.14, "3.14"},
		{"bytes", []byte("world"), "world"},
		{"bool", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
