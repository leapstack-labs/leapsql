package duckdb

// Params holds DuckDB-specific configuration.
// Parsed from adapter.Config.Params using mapstructure.
type Params struct {
	// Extensions to install and load (e.g., "httpfs", "spatial", "json")
	Extensions []string `mapstructure:"extensions"`

	// Secrets for cloud storage authentication
	Secrets []SecretConfig `mapstructure:"secrets"`

	// Settings to apply at session level (e.g., memory_limit, threads)
	Settings map[string]string `mapstructure:"settings"`
}

// SecretConfig defines a DuckDB secret for cloud storage.
type SecretConfig struct {
	// Type: "s3", "gcs", "azure", "r2", "huggingface"
	Type string `mapstructure:"type"`

	// Provider: "config", "credential_chain", "service_account", etc.
	Provider string `mapstructure:"provider"`

	// Region for S3 buckets
	Region string `mapstructure:"region,omitempty"`

	// Scope limits the secret to specific paths (string or []string)
	Scope any `mapstructure:"scope,omitempty"`

	// KeyID for explicit credentials (prefer credential_chain)
	KeyID string `mapstructure:"key_id,omitempty"`

	// Secret for explicit credentials (prefer credential_chain)
	Secret string `mapstructure:"secret,omitempty"`

	// Endpoint for S3-compatible services (MinIO, etc.)
	Endpoint string `mapstructure:"endpoint,omitempty"`

	// URLStyle: "vhost" or "path" for S3
	URLStyle string `mapstructure:"url_style,omitempty"`

	// UseSSL: whether to use HTTPS (default true)
	UseSSL *bool `mapstructure:"use_ssl,omitempty"`
}
