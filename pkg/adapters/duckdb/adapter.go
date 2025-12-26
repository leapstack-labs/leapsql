// Package duckdb provides a DuckDB database adapter for LeapSQL.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/leapstack-labs/leapsql/pkg/adapter"
	duckdbdialect "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"

	_ "github.com/marcboeker/go-duckdb" // duckdb driver
)

// Adapter implements the adapter.Adapter interface for DuckDB.
type Adapter struct {
	adapter.BaseSQLAdapter
}

// New creates a new DuckDB adapter instance.
// If logger is nil, a discard logger is used.
func New(logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Adapter{
		BaseSQLAdapter: adapter.BaseSQLAdapter{Logger: logger},
	}
}

// Dialect returns the SQL dialect configuration for this adapter.
func (a *Adapter) Dialect() *dialect.Dialect {
	return duckdbdialect.DuckDB
}

// Connect establishes a connection to DuckDB.
// Use ":memory:" as the path for an in-memory database.
func (a *Adapter) Connect(ctx context.Context, cfg core.AdapterConfig) error {
	path := cfg.Path
	if path == "" {
		path = ":memory:"
	}

	a.Logger.Debug("connecting to duckdb", slog.String("path", path))

	db, err := sql.Open("duckdb", path)
	if err != nil {
		return fmt.Errorf("failed to open duckdb connection: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping duckdb: %w", err)
	}

	a.DB = db
	a.Cfg = cfg

	// Parse and apply adapter-specific params
	params, err := parseParams(cfg.Params)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to parse duckdb params: %w", err)
	}

	if err := a.applyParams(ctx, params); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to apply duckdb params: %w", err)
	}

	return nil
}

// GetTableMetadata retrieves metadata for a specified table.
func (a *Adapter) GetTableMetadata(ctx context.Context, table string) (*core.TableMetadata, error) {
	return a.GetTableMetadataCommon(ctx, table, a.Dialect())
}

// LoadCSV loads data from a CSV file into a table.
// DuckDB will automatically infer the schema from the CSV file.
func (a *Adapter) LoadCSV(ctx context.Context, tableName string, filePath string) error {
	if a.DB == nil {
		return fmt.Errorf("database connection not established")
	}

	// Get absolute path for the file
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Use DuckDB's read_csv_auto to load the CSV with automatic schema detection
	query := fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=true)",
		tableName,
		absPath,
	)

	if err := a.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to load CSV: %w", err)
	}

	return nil
}

// parseParams decodes core.AdapterConfig.Params into Params.
func parseParams(raw map[string]any) (*Params, error) {
	if raw == nil {
		return &Params{}, nil
	}

	var params Params
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           &params,
		WeaklyTypedInput: true,
		TagName:          "mapstructure",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}

	if err := decoder.Decode(raw); err != nil {
		return nil, fmt.Errorf("invalid duckdb params: %w", err)
	}

	return &params, nil
}

// applyParams applies DuckDB-specific configuration after connection.
func (a *Adapter) applyParams(ctx context.Context, params *Params) error {
	// 1. Install and load extensions
	for _, ext := range params.Extensions {
		if err := a.installExtension(ctx, ext); err != nil {
			return fmt.Errorf("failed to install extension %q: %w", ext, err)
		}
	}

	// 2. Create secrets
	for i, secret := range params.Secrets {
		if err := a.createSecret(ctx, secret); err != nil {
			return fmt.Errorf("failed to create secret %d (%s): %w", i, secret.Type, err)
		}
	}

	// 3. Apply settings
	for key, value := range params.Settings {
		if err := a.applySetting(ctx, key, value); err != nil {
			return fmt.Errorf("failed to apply setting %q: %w", key, err)
		}
	}

	return nil
}

// installExtension installs and loads a DuckDB extension.
func (a *Adapter) installExtension(ctx context.Context, name string) error {
	a.Logger.Debug("installing extension", slog.String("extension", name))

	// INSTALL is idempotent - no-op if already installed
	if _, err := a.DB.ExecContext(ctx, fmt.Sprintf("INSTALL %s", name)); err != nil {
		return fmt.Errorf("install failed: %w", err)
	}

	if _, err := a.DB.ExecContext(ctx, fmt.Sprintf("LOAD %s", name)); err != nil {
		return fmt.Errorf("load failed: %w", err)
	}

	return nil
}

// createSecret creates a DuckDB secret for cloud storage.
func (a *Adapter) createSecret(ctx context.Context, cfg SecretConfig) error {
	a.Logger.Debug("creating secret",
		slog.String("type", cfg.Type),
		slog.String("provider", cfg.Provider),
	)

	sql := buildCreateSecretSQL(cfg)

	if _, err := a.DB.ExecContext(ctx, sql); err != nil {
		return err
	}

	return nil
}

// buildCreateSecretSQL constructs the CREATE SECRET statement.
func buildCreateSecretSQL(cfg SecretConfig) string {
	var b strings.Builder
	b.WriteString("CREATE SECRET (")

	// Required: TYPE
	b.WriteString(fmt.Sprintf("\n    TYPE %s", cfg.Type))

	// Optional: PROVIDER
	if cfg.Provider != "" {
		b.WriteString(fmt.Sprintf(",\n    PROVIDER %s", cfg.Provider))
	}

	// Optional: REGION
	if cfg.Region != "" {
		b.WriteString(fmt.Sprintf(",\n    REGION '%s'", cfg.Region))
	}

	// Optional: SCOPE (can be string or []string)
	if cfg.Scope != nil {
		switch v := cfg.Scope.(type) {
		case string:
			b.WriteString(fmt.Sprintf(",\n    SCOPE '%s'", v))
		case []any:
			scopes := make([]string, len(v))
			for i, s := range v {
				scopes[i] = fmt.Sprintf("'%s'", s)
			}
			b.WriteString(fmt.Sprintf(",\n    SCOPE (%s)", strings.Join(scopes, ", ")))
		case []string:
			scopes := make([]string, len(v))
			for i, s := range v {
				scopes[i] = fmt.Sprintf("'%s'", s)
			}
			b.WriteString(fmt.Sprintf(",\n    SCOPE (%s)", strings.Join(scopes, ", ")))
		}
	}

	// Optional: KEY_ID
	if cfg.KeyID != "" {
		b.WriteString(fmt.Sprintf(",\n    KEY_ID '%s'", cfg.KeyID))
	}

	// Optional: SECRET
	if cfg.Secret != "" {
		b.WriteString(fmt.Sprintf(",\n    SECRET '%s'", cfg.Secret))
	}

	// Optional: ENDPOINT
	if cfg.Endpoint != "" {
		b.WriteString(fmt.Sprintf(",\n    ENDPOINT '%s'", cfg.Endpoint))
	}

	// Optional: URL_STYLE
	if cfg.URLStyle != "" {
		b.WriteString(fmt.Sprintf(",\n    URL_STYLE '%s'", cfg.URLStyle))
	}

	// Optional: USE_SSL
	if cfg.UseSSL != nil {
		b.WriteString(fmt.Sprintf(",\n    USE_SSL %t", *cfg.UseSSL))
	}

	b.WriteString("\n)")
	return b.String()
}

// applySetting applies a DuckDB session setting.
func (a *Adapter) applySetting(ctx context.Context, key, value string) error {
	a.Logger.Debug("applying setting",
		slog.String("key", key),
		slog.String("value", value),
	)

	sql := fmt.Sprintf("SET %s = '%s'", key, value)
	_, err := a.DB.ExecContext(ctx, sql)
	return err
}

// Ensure Adapter implements adapter.Adapter interface
var _ adapter.Adapter = (*Adapter)(nil)
