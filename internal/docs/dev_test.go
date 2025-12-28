package docs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Phase 4: Response Conversion Tests
// =============================================================================

func TestRowsToQueryResponse_Empty(t *testing.T) {
	db := setupTestDB(t)

	ctx := context.Background()
	// Query with no results
	rows, err := db.DB().QueryContext(ctx, "SELECT * FROM models WHERE 1=0")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	resp, err := rowsToQueryResponse(rows)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Columns)
	assert.Nil(t, resp.Values) // Empty result
}

func TestRowsToQueryResponse_SingleRow(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
	)
	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	rows, err := db.DB().QueryContext(ctx, "SELECT path, name FROM models LIMIT 1")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	resp, err := rowsToQueryResponse(rows)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, []string{"path", "name"}, resp.Columns)
	require.Len(t, resp.Values, 1)
	assert.Equal(t, "staging.customers", resp.Values[0][0])
	assert.Equal(t, "customers", resp.Values[0][1])
}

func TestRowsToQueryResponse_MultipleRows(t *testing.T) {
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
		newTestModel("staging.orders", "orders", "view"),
		newTestModel("marts.summary", "summary", "table"),
	)
	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	rows, err := db.DB().QueryContext(ctx, "SELECT path FROM models ORDER BY path")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	resp, err := rowsToQueryResponse(rows)
	require.NoError(t, err)
	require.Len(t, resp.Values, 3)

	paths := make([]string, 3)
	for i, row := range resp.Values {
		paths[i] = row[0].(string)
	}
	assert.Equal(t, []string{"marts.summary", "staging.customers", "staging.orders"}, paths)
}

func TestRowsToQueryResponse_NullValues(t *testing.T) {
	db := setupTestDB(t)
	model := newTestModel("staging.customers", "customers", "view")
	// unique_key will be NULL since we don't set it
	catalog := newTestCatalogWithModels(model)
	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	ctx := context.Background()
	rows, err := db.DB().QueryContext(ctx, "SELECT unique_key FROM models LIMIT 1")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	resp, err := rowsToQueryResponse(rows)
	require.NoError(t, err)
	require.Len(t, resp.Values, 1)
	assert.Nil(t, resp.Values[0][0]) // NULL should become nil
}

func TestRowsToQueryResponse_ByteSlice(t *testing.T) {
	db := setupTestDB(t)

	ctx := context.Background()
	// Create a temp table with a blob column
	_, err := db.DB().ExecContext(ctx, "CREATE TABLE test_blob (data BLOB)")
	require.NoError(t, err)
	_, err = db.DB().ExecContext(ctx, "INSERT INTO test_blob VALUES (?)", []byte("hello"))
	require.NoError(t, err)

	rows, err := db.DB().QueryContext(ctx, "SELECT data FROM test_blob")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	resp, err := rowsToQueryResponse(rows)
	require.NoError(t, err)
	require.Len(t, resp.Values, 1)
	assert.Equal(t, "hello", resp.Values[0][0]) // []byte should become string
}

// =============================================================================
// Phase 4: Query Handler Tests
// =============================================================================

// testDevServer creates a minimal DevServer for testing handlers
func testDevServer(t *testing.T) *DevServer {
	t.Helper()
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
	)
	err := db.PopulateFromCatalog(catalog)
	require.NoError(t, err)

	manifest := GenerateManifest(catalog)

	return &DevServer{
		metaDB:   db,
		manifest: manifest,
	}
}

func TestHandleQuery_ValidQuery(t *testing.T) {
	server := testDevServer(t)

	body := QueryRequest{SQL: "SELECT path, name FROM models LIMIT 1"}
	bodyJSON, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp QueryResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, []string{"path", "name"}, resp.Columns)
	require.Len(t, resp.Values, 1)
}

func TestHandleQuery_InvalidSQL(t *testing.T) {
	server := testDevServer(t)

	body := QueryRequest{SQL: "INVALID SQL QUERY"}
	bodyJSON, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Query error")
}

func TestHandleQuery_InvalidJSON(t *testing.T) {
	server := testDevServer(t)

	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader("not valid json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Invalid request body")
}

func TestHandleQuery_MethodNotAllowed(t *testing.T) {
	server := testDevServer(t)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandleQuery_EmptyParams(t *testing.T) {
	server := testDevServer(t)

	body := QueryRequest{SQL: "SELECT COUNT(*) FROM models", Params: []any{}}
	bodyJSON, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleQuery_WithParams(t *testing.T) {
	server := testDevServer(t)

	body := QueryRequest{
		SQL:    "SELECT path FROM models WHERE path = ?",
		Params: []any{"staging.customers"},
	}
	bodyJSON, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp QueryResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Values, 1)
	assert.Equal(t, "staging.customers", resp.Values[0][0])
}

func TestHandleQuery_DBNotReady(t *testing.T) {
	server := &DevServer{
		metaDB: nil, // DB not initialized
	}

	body := QueryRequest{SQL: "SELECT 1"}
	bodyJSON, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.handleQuery(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "Database not initialized")
}

// =============================================================================
// Phase 4: Manifest Handler Tests
// =============================================================================

func TestHandleManifest_ReturnsJSON(t *testing.T) {
	server := testDevServer(t)

	req := httptest.NewRequest(http.MethodGet, "/manifest", nil)
	rec := httptest.NewRecorder()

	server.handleManifest(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var manifest Manifest
	err := json.NewDecoder(rec.Body).Decode(&manifest)
	require.NoError(t, err)
	assert.Equal(t, "test_project", manifest.ProjectName)
	assert.Equal(t, 1, manifest.Stats.ModelCount)
}

func TestHandleManifest_NotReady(t *testing.T) {
	server := &DevServer{
		manifest: nil, // Manifest not initialized
	}

	req := httptest.NewRequest(http.MethodGet, "/manifest", nil)
	rec := httptest.NewRecorder()

	server.handleManifest(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "Manifest not initialized")
}

func TestHandleManifest_CacheHeaders(t *testing.T) {
	server := testDevServer(t)

	req := httptest.NewRequest(http.MethodGet, "/manifest", nil)
	rec := httptest.NewRecorder()

	server.handleManifest(rec, req)

	assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
}
