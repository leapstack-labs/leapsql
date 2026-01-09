package database

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the database browser feature.
type Handlers struct {
	engine       *engine.Engine
	store        core.Store
	sessionStore sessions.Store
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(eng *engine.Engine, store core.Store, sessionStore sessions.Store) *Handlers {
	return &Handlers{
		engine:       eng,
		store:        store,
		sessionStore: sessionStore,
	}
}

// StatusSSE sends the database connection status via SSE.
func (h *Handlers) StatusSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	adapter := h.engine.GetAdapter()
	if adapter == nil {
		status := ConnectionStatus{
			Connected: false,
			Message:   "No database configured",
		}
		if err := sse.PatchElementTempl(DatabaseStatus(status)); err != nil {
			_ = sse.ConsoleError(err)
		}
		return
	}

	// Try to ensure connection
	if err := h.engine.EnsureConnected(r.Context()); err != nil {
		status := ConnectionStatus{
			Connected: false,
			Message:   fmt.Sprintf("Connection failed: %v", err),
		}
		if err := sse.PatchElementTempl(DatabaseStatus(status)); err != nil {
			_ = sse.ConsoleError(err)
		}
		return
	}

	dialectCfg := adapter.DialectConfig()
	status := ConnectionStatus{
		Connected:   true,
		DialectName: dialectCfg.Name,
		Message:     "Connected",
	}

	if err := sse.PatchElementTempl(DatabaseStatus(status)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// SchemasSSE sends the list of schemas via SSE.
func (h *Handlers) SchemasSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	if err := h.engine.EnsureConnected(r.Context()); err != nil {
		_ = sse.ConsoleError(fmt.Errorf("not connected: %w", err))
		return
	}

	adapter := h.engine.GetAdapter()
	if adapter == nil {
		_ = sse.ConsoleError(fmt.Errorf("no adapter available"))
		return
	}

	schemas, err := h.listSchemas(r.Context(), adapter)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to list schemas: %w", err))
		return
	}

	if err := sse.PatchElementTempl(SchemaList(schemas)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// TablesSSE sends the list of tables in a schema via SSE.
func (h *Handlers) TablesSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	schemaName := chi.URLParam(r, "name")

	if err := h.engine.EnsureConnected(r.Context()); err != nil {
		_ = sse.ConsoleError(fmt.Errorf("not connected: %w", err))
		return
	}

	adapter := h.engine.GetAdapter()
	if adapter == nil {
		_ = sse.ConsoleError(fmt.Errorf("no adapter available"))
		return
	}

	tables, err := h.listTables(r.Context(), adapter, schemaName)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to list tables: %w", err))
		return
	}

	if err := sse.PatchElementTempl(TableList(schemaName, tables)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// TableMetaSSE sends table metadata (columns) via SSE.
func (h *Handlers) TableMetaSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	schemaName := chi.URLParam(r, "schema")
	tableName := chi.URLParam(r, "table")

	if err := h.engine.EnsureConnected(r.Context()); err != nil {
		_ = sse.ConsoleError(fmt.Errorf("not connected: %w", err))
		return
	}

	adapter := h.engine.GetAdapter()
	if adapter == nil {
		_ = sse.ConsoleError(fmt.Errorf("no adapter available"))
		return
	}

	fullName := tableName
	if schemaName != "" && schemaName != "main" {
		fullName = schemaName + "." + tableName
	}

	meta, err := adapter.GetTableMetadata(r.Context(), fullName)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to get table metadata: %w", err))
		return
	}

	tableMeta := TableMeta{
		Schema:  schemaName,
		Name:    tableName,
		Columns: make([]ColumnMeta, len(meta.Columns)),
	}

	for i, col := range meta.Columns {
		tableMeta.Columns[i] = ColumnMeta{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}

	if err := sse.PatchElementTempl(TableDetail(tableMeta)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// listSchemas queries the database for available schemas.
func (h *Handlers) listSchemas(ctx context.Context, adapter interface {
	Query(ctx context.Context, sql string) (*core.Rows, error)
}) ([]SchemaInfo, error) {
	// Use information_schema for standard SQL databases
	// DuckDB also supports this
	query := `
		SELECT DISTINCT table_schema 
		FROM information_schema.tables 
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
		ORDER BY table_schema
	`

	rows, err := adapter.Query(ctx, query)
	if err != nil {
		// Fallback for databases that don't support information_schema
		return []SchemaInfo{{Name: "main"}}, nil
	}
	defer func() { _ = rows.Close() }()

	var schemas []SchemaInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		// Skip empty schema names
		if strings.TrimSpace(name) == "" {
			continue
		}
		schemas = append(schemas, SchemaInfo{Name: name})
	}

	// If no schemas found, return default "main"
	if len(schemas) == 0 {
		schemas = []SchemaInfo{{Name: "main"}}
	}

	return schemas, nil
}

// listTables queries the database for tables in a schema.
func (h *Handlers) listTables(ctx context.Context, adapter interface {
	Query(ctx context.Context, sql string) (*core.Rows, error)
}, schemaName string) ([]TableInfo, error) {
	query := fmt.Sprintf(`
		SELECT table_name, table_type
		FROM information_schema.tables 
		WHERE table_schema = '%s'
		ORDER BY table_name
	`, schemaName)

	rows, err := adapter.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tables []TableInfo
	for rows.Next() {
		var name, tableType string
		if err := rows.Scan(&name, &tableType); err != nil {
			continue
		}
		tables = append(tables, TableInfo{
			Name: name,
			Type: normalizeTableType(tableType),
		})
	}

	// Sort tables by name
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})

	return tables, nil
}

// normalizeTableType converts database-specific table types to standard ones.
func normalizeTableType(t string) string {
	t = strings.ToLower(t)
	switch {
	case strings.Contains(t, "view"):
		return "view"
	case strings.Contains(t, "table"):
		return "table"
	default:
		return t
	}
}
