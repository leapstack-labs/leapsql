package commands

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/spf13/cobra"
)

func runQueryREPL(cmd *cobra.Command, statePath string, opts *QueryOptions) error {
	ctx := cmd.Context()

	// Open database
	db, err := openStateDBReadOnly(statePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Setup history file (project-local)
	historyFile := filepath.Join(filepath.Dir(statePath), "query_history")

	// Get table names for completion
	completer := newTableCompleter(ctx, db)

	// Configure readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "leapsql> ",
		HistoryFile:     historyFile,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       ".quit",
	})
	if err != nil {
		return fmt.Errorf("failed to initialize REPL: %w", err)
	}
	defer func() { _ = rl.Close() }()

	// Print welcome message
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "LeapSQL Query REPL (state: %s)\n", statePath)
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Type .help for commands, .quit to exit")
	_, _ = fmt.Fprintln(cmd.OutOrStdout())

	// REPL loop
	var multiLineBuffer strings.Builder
	for {
		line, err := rl.Readline()
		if errors.Is(err, readline.ErrInterrupt) {
			multiLineBuffer.Reset()
			rl.SetPrompt("leapsql> ")
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Handle dot-commands
		if strings.HasPrefix(line, ".") {
			if handled := handleDotCommand(ctx, cmd, db, line, opts.Format); handled {
				if line == ".quit" || line == ".exit" {
					break
				}
				continue
			}
		}

		// Accumulate multi-line SQL until semicolon
		multiLineBuffer.WriteString(line)
		if !strings.HasSuffix(line, ";") {
			multiLineBuffer.WriteString(" ")
			rl.SetPrompt("    ...> ")
			continue
		}
		rl.SetPrompt("leapsql> ")

		// Execute query
		query := strings.TrimSuffix(multiLineBuffer.String(), ";")
		multiLineBuffer.Reset()

		if err := executeAndRenderQuery(ctx, cmd, db, query, opts.Format); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout())
	}

	return nil
}

// executeAndRenderQuery executes a query and renders results, properly closing rows with defer.
func executeAndRenderQuery(ctx context.Context, cmd *cobra.Command, db *sql.DB, query, format string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	return renderResults(cmd.OutOrStdout(), rows, format)
}

func handleDotCommand(ctx context.Context, cmd *cobra.Command, db *sql.DB, line, format string) bool {
	parts := strings.Fields(line)
	command := strings.ToLower(parts[0])

	switch command {
	case ".quit", ".exit":
		return true

	case ".help":
		printREPLHelp(cmd.OutOrStdout())
		return true

	case ".tables":
		if err := listTablesFromDB(ctx, cmd.OutOrStdout(), db, format, false); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		}
		return true

	case ".views":
		if err := listTablesFromDB(ctx, cmd.OutOrStdout(), db, format, true); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		}
		return true

	case ".schema":
		if len(parts) < 2 {
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Usage: .schema <table>")
			return true
		}
		if err := showSchemaFromDB(ctx, cmd.OutOrStdout(), db, parts[1], format); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		}
		return true

	case ".clear":
		fmt.Print("\033[H\033[2J")
		return true

	default:
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Unknown command: %s (type .help for commands)\n", command)
		return true
	}
}

func printREPLHelp(w io.Writer) {
	help := `
Commands:
  .help           Show this help message
  .tables         List all tables and views
  .views          List views only
  .schema <name>  Show schema for a table or view
  .clear          Clear the screen
  .quit / .exit   Exit the REPL

Tips:
  - SQL statements must end with a semicolon (;)
  - Use arrow keys to navigate history
  - Tab completion works for table names
`
	_, _ = fmt.Fprintln(w, help)
}

// newTableCompleter creates a readline completer for table names.
func newTableCompleter(ctx context.Context, db *sql.DB) *readline.PrefixCompleter {
	// Get all table and view names
	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type IN ('table', 'view') 
		AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return readline.NewPrefixCompleter()
	}
	defer func() { _ = rows.Close() }()

	var items []readline.PrefixCompleterInterface
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			items = append(items, readline.PcItem(name))
		}
	}
	// Ignore rows.Err() as this is for autocomplete, not critical
	_ = rows.Err()

	// Add dot-commands
	items = append(items,
		readline.PcItem(".help"),
		readline.PcItem(".tables"),
		readline.PcItem(".views"),
		readline.PcItem(".schema"),
		readline.PcItem(".clear"),
		readline.PcItem(".quit"),
		readline.PcItem(".exit"),
	)

	return readline.NewPrefixCompleter(items...)
}
