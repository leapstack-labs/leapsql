package commands

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/spf13/cobra"

	// sqlite driver for state database queries.
	_ "modernc.org/sqlite"
)

// resolveStatePath returns the state database path from config or the default.
func resolveStatePath(cfg *config.Config) string {
	if cfg.StatePath != "" {
		return cfg.StatePath
	}
	return config.DefaultStateFile
}

// openStateDBReadOnly opens the state database in read-only mode.
func openStateDBReadOnly(path string) (*sql.DB, error) {
	return sql.Open("sqlite", path+"?mode=ro")
}

// QueryOptions holds options for the query command.
type QueryOptions struct {
	Format string
	Input  string
}

// NewQueryCommand creates the query command.
func NewQueryCommand() *cobra.Command {
	opts := &QueryOptions{}

	cmd := &cobra.Command{
		Use:   "query [SQL]",
		Short: "Query the state database",
		Long: `Query the LeapSQL state database directly.

Execute SQL queries against the state database to inspect runs, models,
dependencies, and other pipeline metadata. Supports multiple output formats
for scripting and integration.

When invoked without arguments, enters interactive REPL mode.`,
		Example: `  # Execute SQL directly
  leapsql query "SELECT * FROM v_runs"
  
  # List available tables
  leapsql query tables
  
  # Show schema for a table
  leapsql query schema models
  
  # Full-text search
  leapsql query search "revenue"
  
  # Output as JSON
  leapsql query "SELECT * FROM v_models" --format json
  
  # Interactive mode
  leapsql query`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runQuery(cmd, args, opts)
		},
	}

	// Flags
	cmd.Flags().StringVarP(&opts.Format, "format", "f", "table", "Output format: table, json, csv, md")
	cmd.Flags().StringVarP(&opts.Input, "input", "i", "", "Read SQL from file")

	// Subcommands
	cmd.AddCommand(newQueryTablesCommand(opts))
	cmd.AddCommand(newQueryViewsCommand(opts))
	cmd.AddCommand(newQuerySchemaCommand(opts))
	cmd.AddCommand(newQuerySearchCommand(opts))

	return cmd
}

func runQuery(cmd *cobra.Command, args []string, opts *QueryOptions) error {
	// Get config for state path
	cmdCtx := NewCommandContextWithoutEngine(cmd)
	statePath := resolveStatePath(cmdCtx.Cfg)

	// Check if state DB exists
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		return fmt.Errorf("state database not found at %s (run 'leapsql run' first)", statePath)
	}

	// Determine SQL source
	var sqlQuery string

	switch {
	case len(args) > 0:
		sqlQuery = strings.Join(args, " ")
	case opts.Input != "":
		content, err := os.ReadFile(opts.Input)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
		sqlQuery = string(content)
	case !isTerminal(os.Stdin):
		// Read from stdin (piped input)
		content, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("failed to read stdin: %w", err)
		}
		sqlQuery = string(content)
	default:
		// No input, TTY detected - enter REPL mode
		return runQueryREPL(cmd, statePath, opts)
	}

	// Execute the query
	return executeAndRender(cmd.Context(), cmd, statePath, sqlQuery, opts.Format)
}

func executeAndRender(ctx context.Context, cmd *cobra.Command, statePath, sqlQuery, format string) error {
	db, err := openStateDBReadOnly(statePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return renderResults(cmd.OutOrStdout(), rows, format)
}

// newQueryTablesCommand creates the tables subcommand.
func newQueryTablesCommand(opts *QueryOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "tables",
		Short: "List all tables and views in the state database",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmdCtx := NewCommandContextWithoutEngine(cmd)
			statePath := resolveStatePath(cmdCtx.Cfg)
			return listTables(cmd, statePath, opts.Format, false)
		},
	}
}

// newQueryViewsCommand creates the views subcommand.
func newQueryViewsCommand(opts *QueryOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "views",
		Short: "List views only",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmdCtx := NewCommandContextWithoutEngine(cmd)
			statePath := resolveStatePath(cmdCtx.Cfg)
			return listTables(cmd, statePath, opts.Format, true)
		},
	}
}

// newQuerySchemaCommand creates the schema subcommand.
func newQuerySchemaCommand(opts *QueryOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "schema <table>",
		Short: "Show schema for a table or view",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmdCtx := NewCommandContextWithoutEngine(cmd)
			statePath := resolveStatePath(cmdCtx.Cfg)
			return showSchema(cmd, statePath, args[0], opts.Format)
		},
	}
}

// newQuerySearchCommand creates the search subcommand.
func newQuerySearchCommand(opts *QueryOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "search <term>",
		Short: "Full-text search across models",
		Long: `Search models using SQLite FTS5 full-text search.

Searches across model names, paths, descriptions, and SQL content.`,
		Example: `  leapsql query search "revenue"
  leapsql query search "customer" --format json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmdCtx := NewCommandContextWithoutEngine(cmd)
			statePath := resolveStatePath(cmdCtx.Cfg)
			return searchModels(cmd, statePath, args[0], opts.Format)
		},
	}
}

func searchModels(cmd *cobra.Command, statePath, term, format string) error {
	// FTS5 query
	query := `
		SELECT 
			m.path,
			m.name,
			m.description,
			highlight(models_fts, 3, '>>>', '<<<') AS match_context
		FROM models_fts
		JOIN models m ON models_fts.rowid = m.rowid
		WHERE models_fts MATCH ?
		ORDER BY rank
		LIMIT 50
	`

	db, err := openStateDBReadOnly(statePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(cmd.Context(), query, term)
	if err != nil {
		return fmt.Errorf("search failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return renderResults(cmd.OutOrStdout(), rows, format)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
