package rules_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules" // register rules
)

func TestRF02_QualifyColumns(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "unqualified column with multiple tables",
			sql:      "SELECT id FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: true,
		},
		{
			name:     "qualified columns with multiple tables",
			sql:      "SELECT usr.id FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: false,
		},
		{
			name:     "single table - qualification not required",
			sql:      "SELECT id FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "RF02")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected RF02 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected RF02 diagnostic")
			}
		})
	}
}

func TestRF03_ConsistentQualification(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "mixed qualification",
			sql:      "SELECT usr.id, name FROM users usr",
			wantDiag: true,
		},
		{
			name:     "all qualified",
			sql:      "SELECT usr.id, usr.name FROM users usr",
			wantDiag: false,
		},
		{
			name:     "all unqualified",
			sql:      "SELECT id, name FROM users",
			wantDiag: false,
		},
		{
			name:     "single column",
			sql:      "SELECT id FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "RF03")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected RF03 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected RF03 diagnostic")
			}
		})
	}
}
