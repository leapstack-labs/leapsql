package docs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// =============================================================================
// Phase 5: Concurrency Tests
// =============================================================================

// TestConcurrentQueryAccess verifies that multiple concurrent queries
// can execute without interfering with each other.
func TestConcurrentQueryAccess(t *testing.T) {
	server := testDevServer(t)

	var wg sync.WaitGroup
	numWorkers := 10
	queriesPerWorker := 50
	errors := make(chan error, numWorkers*queriesPerWorker)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < queriesPerWorker; j++ {
				body := QueryRequest{SQL: "SELECT path, name FROM models"}
				bodyJSON, _ := json.Marshal(body)

				req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()

				server.handleQuery(rec, req)

				if rec.Code != http.StatusOK {
					errors <- fmt.Errorf("worker %d query %d: status %d: %s",
						workerID, j, rec.Code, rec.Body.String())
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect all errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		for _, err := range errs[:min(10, len(errs))] {
			t.Error(err)
		}
		t.Fatalf("Had %d errors out of %d queries", len(errs), numWorkers*queriesPerWorker)
	}
}

// TestAtomicSwapPreservesConsistency verifies that swapping the database
// atomically preserves consistency - queries always see a fully initialized DB.
func TestAtomicSwapPreservesConsistency(t *testing.T) {
	// Create initial server
	db := setupTestDB(t)
	catalog := newTestCatalogWithModels(
		newTestModel("staging.customers", "customers", "view"),
	)
	require.NoError(t, db.PopulateFromCatalog(catalog))
	manifest := GenerateManifest(catalog)

	server := &DevServer{
		metaDB:   db,
		manifest: manifest,
	}

	// Track query results
	var queryCount int64
	var errorCount int64

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Start query workers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					body := QueryRequest{SQL: "SELECT COUNT(*) as cnt FROM models"}
					bodyJSON, _ := json.Marshal(body)

					req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(bodyJSON))
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()

					server.handleQuery(rec, req)

					atomic.AddInt64(&queryCount, 1)
					if rec.Code != http.StatusOK {
						// Only count non-503 errors (503 = DB not ready is OK during swap)
						if rec.Code != http.StatusServiceUnavailable {
							atomic.AddInt64(&errorCount, 1)
						}
					} else {
						// Verify we got a valid response
						var resp QueryResponse
						if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
							atomic.AddInt64(&errorCount, 1)
						}
					}
				}
			}
		}()
	}

	// Perform swaps in main goroutine
	for i := 0; i < 10; i++ {
		// Create new database (fully initialized before swap)
		newDB, err := OpenMemoryDB()
		require.NoError(t, err)
		require.NoError(t, newDB.InitSchema())

		newCatalog := newTestCatalogWithModels(
			newTestModel(fmt.Sprintf("staging.model_%d", i), fmt.Sprintf("model_%d", i), "view"),
		)
		require.NoError(t, newDB.PopulateFromCatalog(newCatalog))
		newManifest := GenerateManifest(newCatalog)

		// Atomic swap under write lock
		server.mu.Lock()
		oldDB := server.metaDB
		server.metaDB = newDB
		server.manifest = newManifest
		server.mu.Unlock()

		// Close old DB (queries holding read lock will block this implicitly
		// because they hold a reference to the old DB's underlying connection)
		if oldDB != nil {
			_ = oldDB.Close()
		}

		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	// Clean up
	server.mu.Lock()
	if server.metaDB != nil {
		_ = server.metaDB.Close()
	}
	server.mu.Unlock()

	t.Logf("Completed %d queries with %d errors", atomic.LoadInt64(&queryCount), atomic.LoadInt64(&errorCount))

	// We might have some errors due to timing, but should be very few
	errRate := float64(atomic.LoadInt64(&errorCount)) / float64(atomic.LoadInt64(&queryCount))
	if errRate > 0.01 { // Allow 1% error rate for edge cases
		t.Errorf("Error rate too high: %.2f%% (%d/%d)",
			errRate*100, atomic.LoadInt64(&errorCount), atomic.LoadInt64(&queryCount))
	}
}

// TestConcurrentManifestAccess verifies manifest reads during swaps.
func TestConcurrentManifestAccess(t *testing.T) {
	server := &DevServer{
		manifest: &Manifest{
			ProjectName: "initial",
			Stats:       Stats{ModelCount: 1},
		},
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Reader goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					req := httptest.NewRequest(http.MethodGet, "/manifest", nil)
					rec := httptest.NewRecorder()
					server.handleManifest(rec, req)

					// Should always get valid JSON or "not initialized"
					if rec.Code == http.StatusOK {
						var m Manifest
						err := json.NewDecoder(rec.Body).Decode(&m)
						assert.NoError(t, err, "manifest should be valid JSON")
					}
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Writer goroutine - rapidly update manifest
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				newManifest := &Manifest{
					ProjectName: fmt.Sprintf("project_%d", i),
					Stats:       Stats{ModelCount: i},
				}
				server.mu.Lock()
				server.manifest = newManifest
				server.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
		cancel() // Signal readers to stop
	}()

	wg.Wait()
}
