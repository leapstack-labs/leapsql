package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseParams(t *testing.T) {
	tests := []struct {
		name    string
		input   map[string]any
		want    *Params
		wantErr bool
	}{
		{
			name:  "nil params returns empty struct",
			input: nil,
			want:  &Params{},
		},
		{
			name:  "empty map returns empty struct",
			input: map[string]any{},
			want:  &Params{},
		},
		{
			name: "extensions only",
			input: map[string]any{
				"extensions": []any{"httpfs", "spatial", "json"},
			},
			want: &Params{
				Extensions: []string{"httpfs", "spatial", "json"},
			},
		},
		{
			name: "settings only",
			input: map[string]any{
				"settings": map[string]any{
					"memory_limit": "4GB",
					"threads":      "4",
				},
			},
			want: &Params{
				Settings: map[string]string{
					"memory_limit": "4GB",
					"threads":      "4",
				},
			},
		},
		{
			name: "secrets with credential_chain",
			input: map[string]any{
				"secrets": []any{
					map[string]any{
						"type":     "s3",
						"provider": "credential_chain",
						"region":   "us-west-2",
					},
				},
			},
			want: &Params{
				Secrets: []SecretConfig{
					{Type: "s3", Provider: "credential_chain", Region: "us-west-2"},
				},
			},
		},
		{
			name: "secrets with scope as string",
			input: map[string]any{
				"secrets": []any{
					map[string]any{
						"type":  "s3",
						"scope": "s3://my-bucket",
					},
				},
			},
			want: &Params{
				Secrets: []SecretConfig{
					{Type: "s3", Scope: "s3://my-bucket"},
				},
			},
		},
		{
			name: "secrets with scope as array",
			input: map[string]any{
				"secrets": []any{
					map[string]any{
						"type":  "s3",
						"scope": []any{"s3://bucket1", "s3://bucket2"},
					},
				},
			},
			want: &Params{
				Secrets: []SecretConfig{
					{Type: "s3", Scope: []any{"s3://bucket1", "s3://bucket2"}},
				},
			},
		},
		{
			name: "full config",
			input: map[string]any{
				"extensions": []any{"httpfs", "spatial"},
				"settings": map[string]any{
					"memory_limit": "4GB",
				},
				"secrets": []any{
					map[string]any{
						"type":     "s3",
						"provider": "credential_chain",
						"region":   "us-west-2",
					},
					map[string]any{
						"type":     "gcs",
						"provider": "service_account",
						"key_id":   "my-key",
					},
				},
			},
			want: &Params{
				Extensions: []string{"httpfs", "spatial"},
				Settings:   map[string]string{"memory_limit": "4GB"},
				Secrets: []SecretConfig{
					{Type: "s3", Provider: "credential_chain", Region: "us-west-2"},
					{Type: "gcs", Provider: "service_account", KeyID: "my-key"},
				},
			},
		},
		{
			name: "secret with all optional fields",
			input: map[string]any{
				"secrets": []any{
					map[string]any{
						"type":      "s3",
						"provider":  "config",
						"region":    "us-east-1",
						"key_id":    "AKIAIOSFODNN7EXAMPLE",
						"secret":    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
						"endpoint":  "http://localhost:9000",
						"url_style": "path",
						"use_ssl":   false,
					},
				},
			},
			want: &Params{
				Secrets: []SecretConfig{
					{
						Type:     "s3",
						Provider: "config",
						Region:   "us-east-1",
						KeyID:    "AKIAIOSFODNN7EXAMPLE",
						Secret:   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
						Endpoint: "http://localhost:9000",
						URLStyle: "path",
						UseSSL:   boolPtr(false),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseParams(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want.Extensions, got.Extensions)
			assert.Equal(t, tt.want.Settings, got.Settings)
			assert.Len(t, got.Secrets, len(tt.want.Secrets))

			for i, wantSecret := range tt.want.Secrets {
				gotSecret := got.Secrets[i]
				assert.Equal(t, wantSecret.Type, gotSecret.Type, "secret %d type", i)
				assert.Equal(t, wantSecret.Provider, gotSecret.Provider, "secret %d provider", i)
				assert.Equal(t, wantSecret.Region, gotSecret.Region, "secret %d region", i)
				assert.Equal(t, wantSecret.KeyID, gotSecret.KeyID, "secret %d key_id", i)
				assert.Equal(t, wantSecret.Secret, gotSecret.Secret, "secret %d secret", i)
				assert.Equal(t, wantSecret.Endpoint, gotSecret.Endpoint, "secret %d endpoint", i)
				assert.Equal(t, wantSecret.URLStyle, gotSecret.URLStyle, "secret %d url_style", i)
				if wantSecret.UseSSL != nil {
					require.NotNil(t, gotSecret.UseSSL, "secret %d use_ssl should not be nil", i)
					assert.Equal(t, *wantSecret.UseSSL, *gotSecret.UseSSL, "secret %d use_ssl", i)
				}
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
