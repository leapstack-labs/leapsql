package adapter

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseSQLAdapter_Close(t *testing.T) {
	tests := []struct {
		name      string
		setupDB   bool
		expectErr bool
	}{
		{
			name:      "close with nil DB",
			setupDB:   false,
			expectErr: false,
		},
		{
			name:      "close with open DB",
			setupDB:   true,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &BaseSQLAdapter{}

			if tt.setupDB {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectClose()
				base.DB = db
			}

			err := base.Close()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBaseSQLAdapter_Exec(t *testing.T) {
	tests := []struct {
		name      string
		setupDB   bool
		setupMock func(mock sqlmock.Sqlmock)
		sql       string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "exec without connection",
			setupDB:   false,
			sql:       "SELECT 1",
			expectErr: true,
			errMsg:    "database connection not established",
		},
		{
			name:    "exec success",
			setupDB: true,
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			sql:       "CREATE TABLE users (id INT)",
			expectErr: false,
		},
		{
			name:    "exec with error",
			setupDB: true,
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INVALID SQL").WillReturnError(assert.AnError)
			},
			sql:       "INVALID SQL",
			expectErr: true,
			errMsg:    "failed to execute SQL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			base := &BaseSQLAdapter{}

			if tt.setupDB {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer func() { _ = db.Close() }()

				if tt.setupMock != nil {
					tt.setupMock(mock)
				}
				base.DB = db
			}

			err := base.Exec(ctx, tt.sql)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBaseSQLAdapter_Query(t *testing.T) {
	tests := []struct {
		name      string
		setupDB   bool
		setupMock func(mock sqlmock.Sqlmock)
		sql       string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "query without connection",
			setupDB:   false,
			sql:       "SELECT 1",
			expectErr: true,
			errMsg:    "database connection not established",
		},
		{
			name:    "query success",
			setupDB: true,
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "alice").
					AddRow(2, "bob")
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			sql:       "SELECT id, name FROM users",
			expectErr: false,
		},
		{
			name:    "query with error",
			setupDB: true,
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("INVALID").WillReturnError(assert.AnError)
			},
			sql:       "INVALID SQL",
			expectErr: true,
			errMsg:    "failed to execute query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			base := &BaseSQLAdapter{}

			if tt.setupDB {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer func() { _ = db.Close() }()

				if tt.setupMock != nil {
					tt.setupMock(mock)
				}
				base.DB = db
			}

			rows, err := base.Query(ctx, tt.sql)
			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, rows)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				assert.NotNil(t, rows)
				defer func() { _ = rows.Close() }()
			}
		})
	}
}

func TestBaseSQLAdapter_IsConnected(t *testing.T) {
	tests := []struct {
		name     string
		setupDB  bool
		expected bool
	}{
		{
			name:     "not connected",
			setupDB:  false,
			expected: false,
		},
		{
			name:     "connected",
			setupDB:  true,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &BaseSQLAdapter{}

			if tt.setupDB {
				db, _, err := sqlmock.New()
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				base.DB = db
			}

			assert.Equal(t, tt.expected, base.IsConnected())
		})
	}
}
