// Package statequery provides handlers for querying the state database.
package statequery

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
	commonComponents "github.com/leapstack-labs/leapsql/internal/ui/features/common/components"
	"github.com/leapstack-labs/leapsql/internal/ui/features/statequery/components"
	"github.com/leapstack-labs/leapsql/internal/ui/features/statequery/pages"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

const (
	maxRows      = 1000
	queryTimeout = 30 * time.Second
)

// QuerySignals represents the signals sent from the frontend.
type QuerySignals struct {
	SQL string `json:"sql"`
}

// Handlers provides HTTP handlers for the state query feature.
type Handlers struct {
	store        core.Store
	sessionStore sessions.Store
	notifier     *notifier.Notifier
	isDev        bool
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(store core.Store, sessionStore sessions.Store, notify *notifier.Notifier, isDev bool) *Handlers {
	return &Handlers{
		store:        store,
		sessionStore: sessionStore,
		notifier:     notify,
		isDev:        isDev,
	}
}

// getDB returns the underlying database connection.
func (h *Handlers) getDB() (*sql.DB, error) {
	qstore, ok := h.store.(core.QueryableStore)
	if !ok {
		return nil, fmt.Errorf("store does not support direct queries")
	}
	return qstore.DB(), nil
}

// QueryPage renders the query page shell.
func (h *Handlers) QueryPage(w http.ResponseWriter, r *http.Request) {
	if err := pages.QueryPage("State Query", h.isDev).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// QueryPageSSE sends the full app view for the query page using fat morph pattern.
func (h *Handlers) QueryPageSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	appData, err := h.buildQueryAppData(r.Context())
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	if err := sse.PatchElementTempl(commonComponents.AppContainer(appData)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// buildQueryAppData assembles all data needed for the query view.
func (h *Handlers) buildQueryAppData(ctx context.Context) (commonComponents.AppData, error) {
	data := commonComponents.AppData{
		CurrentPath: "/query",
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return data, err
	}
	data.ExplorerTree = common.BuildExplorerTree(models)

	// Get tables and views
	db, err := h.getDB()
	if err != nil {
		// If we can't get the DB, still render the page with empty table list
		data.Query = &commonComponents.QueryViewData{
			Tables: []commonComponents.TableItem{},
			Views:  []commonComponents.TableItem{},
		}
		return data, nil
	}

	tables, views, err := h.listTablesAndViewsForAppData(ctx, db)
	if err != nil {
		return data, err
	}

	data.Query = &commonComponents.QueryViewData{
		Tables: tables,
		Views:  views,
	}

	return data, nil
}

// listTablesAndViewsForAppData returns tables/views in the format for AppData.
func (h *Handlers) listTablesAndViewsForAppData(ctx context.Context, db *sql.DB) ([]commonComponents.TableItem, []commonComponents.TableItem, error) {
	query := `
		SELECT name, type 
		FROM sqlite_master 
		WHERE type IN ('table', 'view') 
		AND name NOT LIKE 'sqlite_%'
		AND name NOT LIKE '%_fts%'
		AND name NOT LIKE 'goose_%'
		ORDER BY type, name
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var tables, views []commonComponents.TableItem
	for rows.Next() {
		var name, objType string
		if err := rows.Scan(&name, &objType); err != nil {
			continue
		}
		item := commonComponents.TableItem{Name: name, Type: objType}
		if objType == "view" {
			views = append(views, item)
		} else {
			tables = append(tables, item)
		}
	}

	return tables, views, nil
}

// ExecuteQuerySSE executes a SQL query and returns results.
func (h *Handlers) ExecuteQuerySSE(w http.ResponseWriter, r *http.Request) {
	// Read signals BEFORE creating SSE (SSE consumes the request body)
	var signals QuerySignals
	if err := datastar.ReadSignals(r, &signals); err != nil {
		sse := datastar.NewSSE(w, r)
		_ = sse.PatchElementTempl(components.QueryResults(components.QueryResult{
			Error: "Failed to read signals: " + err.Error(),
		}))
		return
	}

	sse := datastar.NewSSE(w, r)

	query := strings.TrimSpace(signals.SQL)
	if query == "" {
		_ = sse.PatchElementTempl(components.QueryResults(components.QueryResult{
			Error: "Query cannot be empty",
		}))
		return
	}

	db, err := h.getDB()
	if err != nil {
		_ = sse.PatchElementTempl(components.QueryResults(components.QueryResult{
			Error: err.Error(),
		}))
		return
	}

	// Execute with timeout
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	start := time.Now()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		_ = sse.PatchElementTempl(components.QueryResults(components.QueryResult{
			Error: err.Error(),
		}))
		return
	}
	defer rows.Close()

	// Get columns
	cols, err := rows.Columns()
	if err != nil {
		_ = sse.PatchElementTempl(components.QueryResults(components.QueryResult{
			Error: err.Error(),
		}))
		return
	}

	// Collect rows
	var results [][]string
	for rows.Next() && len(results) < maxRows {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make([]string, len(cols))
		for i, val := range values {
			row[i] = formatValue(val)
		}
		results = append(results, row)
	}

	queryMS := time.Since(start).Milliseconds()

	result := components.QueryResult{
		Columns:   cols,
		Rows:      results,
		RowCount:  len(results),
		Truncated: len(results) == maxRows,
		QueryMS:   queryMS,
	}

	if err := sse.PatchElementTempl(components.QueryResults(result)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// TablesSSE sends the list of tables and views.
func (h *Handlers) TablesSSE(w http.ResponseWriter, r *http.Request) {
	// Same as QueryPageSSE
	h.QueryPageSSE(w, r)
}

// SchemaSSE returns schema for a table.
func (h *Handlers) SchemaSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	tableName := chi.URLParam(r, "name")

	db, err := h.getDB()
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	schema, err := h.getTableSchema(r.Context(), db, tableName)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to get schema: %w", err))
		return
	}

	if err := sse.PatchElementTempl(components.SchemaPanel(schema)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// SearchSSE performs full-text search across models.
func (h *Handlers) SearchSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	term := strings.TrimSpace(r.URL.Query().Get("term"))
	if term == "" {
		_ = sse.PatchElementTempl(components.SearchResults(nil))
		return
	}

	db, err := h.getDB()
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	results, err := h.searchModels(r.Context(), db, term)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("search failed: %w", err))
		return
	}

	if err := sse.PatchElementTempl(components.SearchResults(results)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// Helper functions

func (h *Handlers) getTableSchema(ctx context.Context, db *sql.DB, tableName string) (components.SchemaData, error) {
	// Get object type
	var objType string
	err := db.QueryRowContext(ctx, `
		SELECT type FROM sqlite_master 
		WHERE name = ? AND type IN ('table', 'view')
	`, tableName).Scan(&objType)
	if err != nil {
		return components.SchemaData{}, fmt.Errorf("table not found: %s", tableName)
	}

	// Get columns using PRAGMA (table name must be sanitized/validated)
	// PRAGMA doesn't support parameterized queries, so we validate the table exists first
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return components.SchemaData{}, err
	}
	defer rows.Close()

	var columns []components.ColumnSchema
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			continue
		}

		columns = append(columns, components.ColumnSchema{
			Name:     name,
			Type:     colType,
			Nullable: notNull == 0,
			Default:  dflt.String,
			IsPK:     pk == 1,
		})
	}

	return components.SchemaData{
		Name:    tableName,
		Type:    objType,
		Columns: columns,
	}, nil
}

func (h *Handlers) searchModels(ctx context.Context, db *sql.DB, term string) ([]components.SearchResultItem, error) {
	// Try using FTS5 if available, fall back to LIKE
	query := `
		SELECT path, name, description
		FROM models
		WHERE path LIKE ? OR name LIKE ? OR description LIKE ?
		LIMIT 50
	`
	searchTerm := "%" + term + "%"

	rows, err := db.QueryContext(ctx, query, searchTerm, searchTerm, searchTerm)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []components.SearchResultItem
	for rows.Next() {
		var path, name string
		var description sql.NullString

		if err := rows.Scan(&path, &name, &description); err != nil {
			continue
		}

		results = append(results, components.SearchResultItem{
			Path:        path,
			Name:        name,
			Description: description.String,
		})
	}

	return results, nil
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}
