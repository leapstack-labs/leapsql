package commands

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
)

func renderResults(w io.Writer, rows *sql.Rows, format string) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Collect all rows
	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make(map[string]any)
		for i, col := range cols {
			val := values[i]
			// Convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			row[col] = val
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	switch format {
	case "json":
		return renderJSON(w, results)
	case "csv":
		return renderCSV(w, cols, results)
	case "md", "markdown":
		return renderMarkdown(w, cols, results)
	default:
		return renderTable(w, cols, results)
	}
}

func renderTable(w io.Writer, cols []string, results []map[string]any) error {
	if len(results) == 0 {
		_, _ = fmt.Fprintln(w, "(0 rows)")
		return nil
	}

	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(table.StyleLight)

	// Header
	headerRow := make(table.Row, len(cols))
	for i, col := range cols {
		headerRow[i] = col
	}
	t.AppendHeader(headerRow)

	// Rows
	for _, result := range results {
		row := make(table.Row, len(cols))
		for i, col := range cols {
			row[i] = formatValue(result[col])
		}
		t.AppendRow(row)
	}

	t.Render()
	_, _ = fmt.Fprintf(w, "(%d rows)\n", len(results))
	return nil
}

func renderJSON(w io.Writer, results []map[string]any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}

func renderCSV(w io.Writer, cols []string, results []map[string]any) error {
	// Header
	_, _ = fmt.Fprintln(w, strings.Join(cols, ","))

	// Rows
	for _, result := range results {
		values := make([]string, len(cols))
		for i, col := range cols {
			values[i] = escapeCSV(formatValue(result[col]))
		}
		_, _ = fmt.Fprintln(w, strings.Join(values, ","))
	}
	return nil
}

func renderMarkdown(w io.Writer, cols []string, results []map[string]any) error {
	if len(results) == 0 {
		_, _ = fmt.Fprintln(w, "(0 rows)")
		return nil
	}

	// Header
	_, _ = fmt.Fprintf(w, "| %s |\n", strings.Join(cols, " | "))
	// Separator
	seps := make([]string, len(cols))
	for i := range seps {
		seps[i] = "---"
	}
	_, _ = fmt.Fprintf(w, "| %s |\n", strings.Join(seps, " | "))

	// Rows
	for _, result := range results {
		values := make([]string, len(cols))
		for i, col := range cols {
			values[i] = formatValue(result[col])
		}
		_, _ = fmt.Fprintf(w, "| %s |\n", strings.Join(values, " | "))
	}
	return nil
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func escapeCSV(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

// Helper functions for subcommands

func listTables(cmd *cobra.Command, statePath, format string, viewsOnly bool) error {
	db, err := openStateDBReadOnly(statePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	return listTablesFromDB(cmd.Context(), cmd.OutOrStdout(), db, format, viewsOnly)
}

func listTablesFromDB(ctx context.Context, w io.Writer, db *sql.DB, format string, viewsOnly bool) error {
	query := `
		SELECT name, type 
		FROM sqlite_master 
		WHERE type IN ('table', 'view') 
		AND name NOT LIKE 'sqlite_%'
		AND name NOT LIKE '%_fts%'
		AND name NOT LIKE 'goose_%'
	`
	if viewsOnly {
		query += ` AND type = 'view'`
	}
	query += ` ORDER BY type DESC, name`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	return renderResults(w, rows, format)
}

func showSchema(cmd *cobra.Command, statePath, tableName, format string) error {
	db, err := openStateDBReadOnly(statePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	return showSchemaFromDB(cmd.Context(), cmd.OutOrStdout(), db, tableName, format)
}

func showSchemaFromDB(ctx context.Context, w io.Writer, db *sql.DB, tableName, format string) error {
	// Get table info using PRAGMA
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var columns []columnInfo

	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return err
		}

		nullable := "YES"
		if notNull == 1 {
			nullable = "NO"
		}

		defaultVal := ""
		if dflt.Valid {
			defaultVal = dflt.String
		}
		if pk == 1 {
			if defaultVal != "" {
				defaultVal += " "
			}
			defaultVal += "(primary key)"
		}

		columns = append(columns, columnInfo{
			Name:     name,
			Type:     colType,
			Nullable: nullable,
			Default:  defaultVal,
			PK:       pk == 1,
		})
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(columns) == 0 {
		return fmt.Errorf("table or view '%s' not found", tableName)
	}

	// Determine if it's a table or view
	var objType string
	err = db.QueryRowContext(ctx, `
		SELECT type FROM sqlite_master 
		WHERE name = ? AND type IN ('table', 'view')
	`, tableName).Scan(&objType)
	if err != nil {
		objType = "table"
	}

	// Render based on format
	if format == "json" {
		return renderSchemaJSON(w, tableName, objType, columns)
	}

	// Default: formatted text output
	title := "Table"
	if objType == "view" {
		title = "View"
	}
	_, _ = fmt.Fprintf(w, "%s: %s\n", title, tableName)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", 60))

	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"Column", "Type", "Nullable", "Default"})

	for _, col := range columns {
		t.AppendRow(table.Row{col.Name, col.Type, col.Nullable, col.Default})
	}
	t.Render()

	// Show indexes for tables
	if objType == "table" {
		indexRows, err := db.QueryContext(ctx, `
			SELECT name FROM sqlite_master 
			WHERE type = 'index' AND tbl_name = ?
			AND name NOT LIKE 'sqlite_%'
		`, tableName)
		if err == nil {
			defer func() { _ = indexRows.Close() }()
			var indexes []string
			for indexRows.Next() {
				var name string
				if indexRows.Scan(&name) == nil {
					indexes = append(indexes, name)
				}
			}
			// Ignore indexRows.Err() as this is optional information
			_ = indexRows.Err()
			if len(indexes) > 0 {
				_, _ = fmt.Fprintln(w)
				_, _ = fmt.Fprintln(w, "Indexes:")
				for _, idx := range indexes {
					_, _ = fmt.Fprintf(w, "  %s\n", idx)
				}
			}
		}
	}

	return nil
}

// columnInfo represents schema column information.
type columnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable string `json:"nullable"`
	Default  string `json:"default"`
	PK       bool   `json:"pk"`
}

type schemaOutput struct {
	Name    string       `json:"name"`
	Type    string       `json:"type"`
	Columns []columnInfo `json:"columns"`
}

func renderSchemaJSON(w io.Writer, tableName, objType string, columns []columnInfo) error {
	schema := schemaOutput{
		Name:    tableName,
		Type:    objType,
		Columns: columns,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(schema)
}
