//go:build integration

package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/testutil"
)

// scenarioTest defines a test case for a testdata scenario.
type scenarioTest struct {
	name            string
	scenario        string                        // testdata subdirectory
	wantDiscoverErr string                        // substring of expected error, empty = success
	wantModels      int                           // expected model count
	wantMacros      int                           // expected macro count
	wantDAGNodes    int                           // expected DAG node count
	validate        func(t *testing.T, e *Engine) // custom validation
}

var scenarioTests = []scenarioTest{
	{
		name:         "basic project",
		scenario:     "basic",
		wantModels:   3,
		wantMacros:   1,
		wantDAGNodes: 3,
		validate: func(t *testing.T, e *Engine) {
			// Verify customer_summary depends on both staging models
			parents := e.GetGraph().GetParents("marts.customer_summary")
			if len(parents) != 2 {
				t.Errorf("customer_summary should have 2 parents, got %d", len(parents))
			}
		},
	},
	{
		name:         "diamond dependency",
		scenario:     "diamond",
		wantModels:   4,
		wantDAGNodes: 4,
		validate: func(t *testing.T, e *Engine) {
			graph := e.GetGraph()

			// combined_report should be last in topological order
			sorted, err := graph.TopologicalSort()
			if err != nil {
				t.Fatalf("TopologicalSort failed: %v", err)
			}
			last := sorted[len(sorted)-1].ID
			if last != "combined_report" {
				t.Errorf("expected combined_report last, got %s", last)
			}

			// combined_report should have 2 parents
			parents := graph.GetParents("combined_report")
			if len(parents) != 2 {
				t.Errorf("combined_report should have 2 parents, got %d", len(parents))
			}

			// base_events should have 2 children
			children := graph.GetChildren("base_events")
			if len(children) != 2 {
				t.Errorf("base_events should have 2 children, got %d", len(children))
			}
		},
	},
	{
		name:            "circular dependency error",
		scenario:        "circular",
		wantDiscoverErr: "circular dependency",
	},
	{
		name:         "view materialization",
		scenario:     "views",
		wantModels:   2,
		wantDAGNodes: 2,
		validate: func(t *testing.T, e *Engine) {
			models := e.GetModels()
			m, ok := models["active_customers"]
			if !ok {
				t.Fatal("active_customers model not found")
			}
			if m.Materialized != "view" {
				t.Errorf("expected view materialization, got %s", m.Materialized)
			}
		},
	},
	{
		name:         "macro templates",
		scenario:     "templated",
		wantModels:   1,
		wantMacros:   1,
		wantDAGNodes: 1,
		validate: func(t *testing.T, e *Engine) {
			sql, err := e.RenderModel("formatted_orders")
			if err != nil {
				t.Fatalf("RenderModel failed: %v", err)
			}
			if !strings.Contains(sql, "ROUND") {
				t.Errorf("expected macro expansion with ROUND, got: %s", sql)
			}
			if !strings.Contains(sql, "CASE") {
				t.Errorf("expected macro expansion with CASE, got: %s", sql)
			}
		},
	},
	{
		name:         "incremental materialization",
		scenario:     "incremental",
		wantModels:   1,
		wantDAGNodes: 1,
		validate: func(t *testing.T, e *Engine) {
			models := e.GetModels()
			m, ok := models["event_log"]
			if !ok {
				t.Fatal("event_log model not found")
			}
			if m.Materialized != "incremental" {
				t.Errorf("expected incremental materialization, got %s", m.Materialized)
			}
			if m.UniqueKey != "event_id" {
				t.Errorf("expected unique_key=event_id, got %s", m.UniqueKey)
			}
		},
	},
}

func TestIntegration_Scenarios(t *testing.T) {
	for _, tc := range scenarioTests {
		t.Run(tc.name, func(t *testing.T) {
			scenarioDir := filepath.Join("testdata", tc.scenario)
			tmpDir := copyScenario(t, scenarioDir)

			cfg := Config{
				ModelsDir: filepath.Join(tmpDir, "models"),
				SeedsDir:  filepath.Join(tmpDir, "seeds"),
				MacrosDir: filepath.Join(tmpDir, "macros"),
				StatePath: filepath.Join(tmpDir, "state.db"),
				Target:    defaultTestTarget(),
			}

			eng, err := New(cfg)
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}
			defer eng.Close()

			// Run discovery
			result, discoverErr := eng.Discover(DiscoveryOptions{})

			// Check discover error expectation
			if tc.wantDiscoverErr != "" {
				if discoverErr == nil {
					t.Fatalf("expected discover error containing %q, got nil", tc.wantDiscoverErr)
				}
				if !strings.Contains(discoverErr.Error(), tc.wantDiscoverErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantDiscoverErr, discoverErr)
				}
				return // Error case - done
			}

			if discoverErr != nil {
				t.Fatalf("Discover() failed: %v", discoverErr)
			}

			// Validate model count
			if tc.wantModels > 0 && result.ModelsTotal != tc.wantModels {
				t.Errorf("expected %d models, got %d", tc.wantModels, result.ModelsTotal)
			}

			// Validate macro count
			if tc.wantMacros > 0 && result.MacrosTotal != tc.wantMacros {
				t.Errorf("expected %d macros, got %d", tc.wantMacros, result.MacrosTotal)
			}

			// Validate DAG node count
			if tc.wantDAGNodes > 0 && eng.GetGraph().NodeCount() != tc.wantDAGNodes {
				t.Errorf("expected %d DAG nodes, got %d", tc.wantDAGNodes, eng.GetGraph().NodeCount())
			}

			// Run custom validation
			if tc.validate != nil {
				tc.validate(t, eng)
			}
		})
	}
}

// copyScenario copies a scenario directory to a temp directory for testing.
func copyScenario(t *testing.T, scenarioDir string) string {
	t.Helper()
	tmpDir := t.TempDir()

	err := filepath.Walk(scenarioDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(scenarioDir, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(tmpDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, 0755)
		}

		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		dst, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dst.Close()

		_, err = io.Copy(dst, src)
		return err
	})

	if err != nil {
		t.Fatalf("Failed to copy scenario %s: %v", scenarioDir, err)
	}

	return tmpDir
}

// executionTest defines a test case for full execution validation.
type executionTest struct {
	name     string
	scenario string
	queries  []queryCheck
}

type queryCheck struct {
	sql      string
	wantRows int // -1 = just verify query succeeds
}

var executionTests = []executionTest{
	{
		name:     "basic execution",
		scenario: "basic",
		queries: []queryCheck{
			{sql: "SELECT COUNT(*) FROM staging.stg_customers", wantRows: 3},
			{sql: "SELECT COUNT(*) FROM staging.stg_orders", wantRows: 4},
			{sql: "SELECT COUNT(*) FROM marts.customer_summary", wantRows: 3},
		},
	},
	{
		name:     "diamond execution",
		scenario: "diamond",
		queries: []queryCheck{
			{sql: "SELECT COUNT(*) FROM base_events", wantRows: 5},
			{sql: "SELECT COUNT(*) FROM user_events", wantRows: 3},
			{sql: "SELECT COUNT(*) FROM product_events", wantRows: 3},
			{sql: "SELECT COUNT(*) FROM combined_report", wantRows: 9}, // 3x3 cross join
		},
	},
	{
		name:     "view execution",
		scenario: "views",
		queries: []queryCheck{
			{sql: "SELECT COUNT(*) FROM customers_base", wantRows: 3},
			{sql: "SELECT COUNT(*) FROM active_customers", wantRows: 2}, // only active
		},
	},
	{
		name:     "templated execution",
		scenario: "templated",
		queries: []queryCheck{
			{sql: "SELECT COUNT(*) FROM formatted_orders WHERE id = 1", wantRows: 1},
			{sql: "SELECT COUNT(*) FROM formatted_orders WHERE status_label = 'Pending'", wantRows: 1},
		},
	},
}

func TestIntegration_Execution(t *testing.T) {
	for _, tc := range executionTests {
		t.Run(tc.name, func(t *testing.T) {
			scenarioDir := filepath.Join("testdata", tc.scenario)
			tmpDir := copyScenario(t, scenarioDir)

			cfg := Config{
				ModelsDir:    filepath.Join(tmpDir, "models"),
				SeedsDir:     filepath.Join(tmpDir, "seeds"),
				MacrosDir:    filepath.Join(tmpDir, "macros"),
				DatabasePath: "", // in-memory
				StatePath:    filepath.Join(tmpDir, "state.db"),
				Target:       defaultTestTarget(),
			}

			eng, err := New(cfg)
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}
			defer eng.Close()

			ctx := context.Background()

			// Load seeds
			if err := eng.LoadSeeds(ctx); err != nil {
				t.Fatalf("LoadSeeds() failed: %v", err)
			}

			// Discover
			if _, err := eng.Discover(DiscoveryOptions{}); err != nil {
				t.Fatalf("Discover() failed: %v", err)
			}

			// Run
			run, err := eng.Run(ctx, "test")
			if err != nil {
				t.Fatalf("Run() failed: %v", err)
			}
			if run.Status != "completed" {
				t.Fatalf("Run status = %q, want completed. Error: %s", run.Status, run.Error)
			}

			// Validate queries
			for _, qc := range tc.queries {
				rows, err := eng.db.Query(ctx, qc.sql)
				if err != nil {
					t.Errorf("query %q failed: %v", qc.sql, err)
					continue
				}

				if qc.wantRows >= 0 {
					var count int
					if rows.Next() {
						rows.Scan(&count)
					}
					rows.Close()
					if count != qc.wantRows {
						t.Errorf("query %q: got %d rows, want %d", qc.sql, count, qc.wantRows)
					}
				} else {
					rows.Close()
				}
			}
		})
	}
}

func TestIntegration_IncrementalUpsert(t *testing.T) {
	scenarioDir := filepath.Join("testdata", "incremental")
	tmpDir := copyScenario(t, scenarioDir)

	// For incremental test, we need persistent DB
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	cfg := Config{
		ModelsDir:    filepath.Join(tmpDir, "models"),
		SeedsDir:     filepath.Join(tmpDir, "seeds"),
		MacrosDir:    filepath.Join(tmpDir, "macros"),
		DatabasePath: dbPath,
		StatePath:    filepath.Join(tmpDir, "state.db"),
		Target:       defaultTestTarget(),
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()

	// Step 1: Initial load
	if err := eng.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}
	if _, err := eng.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}
	run1, err := eng.Run(ctx, "test")
	if err != nil || run1.Status != "completed" {
		t.Fatalf("First Run() failed: %v, status: %s", err, run1.Status)
	}

	// Verify initial count
	rows, _ := eng.db.Query(ctx, "SELECT COUNT(*) FROM event_log")
	var count int
	rows.Next()
	rows.Scan(&count)
	rows.Close()
	if count != 3 {
		t.Errorf("initial count = %d, want 3", count)
	}

	// Step 2: Modify seed - add new row, update existing
	newSeed := `event_id,event_type,payload,created_at
1,login,user_1_updated,2024-01-01
2,click,button_a,2024-01-02
3,logout,user_1,2024-01-03
4,signup,user_2,2024-01-04`

	seedPath := filepath.Join(tmpDir, "seeds", "raw_events.csv")
	if err := os.WriteFile(seedPath, []byte(newSeed), 0644); err != nil {
		t.Fatalf("Failed to update seed: %v", err)
	}

	// Step 3: Reload seeds and run again
	if err := eng.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() second time failed: %v", err)
	}

	// Force re-discover since seed changed
	if _, err := eng.Discover(DiscoveryOptions{ForceFullRefresh: true}); err != nil {
		t.Fatalf("Second Discover() failed: %v", err)
	}

	run2, err := eng.Run(ctx, "test")
	if err != nil || run2.Status != "completed" {
		t.Fatalf("Second Run() failed: %v, status: %s", err, run2.Status)
	}

	// Step 4: Verify upsert behavior
	// Should have 4 rows total (not 7 from append)
	rows, _ = eng.db.Query(ctx, "SELECT COUNT(*) FROM event_log")
	rows.Next()
	rows.Scan(&count)
	rows.Close()
	if count != 4 {
		t.Errorf("after upsert count = %d, want 4", count)
	}

	// Verify row 1 was updated
	rows, _ = eng.db.Query(ctx, "SELECT payload FROM event_log WHERE event_id = 1")
	var payload string
	rows.Next()
	rows.Scan(&payload)
	rows.Close()
	if payload != "user_1_updated" {
		t.Errorf("row 1 payload = %q, want 'user_1_updated'", payload)
	}

	// Verify row 4 was added
	rows, _ = eng.db.Query(ctx, "SELECT event_type FROM event_log WHERE event_id = 4")
	var eventType string
	if rows.Next() {
		rows.Scan(&eventType)
	}
	rows.Close()
	if eventType != "signup" {
		t.Errorf("row 4 event_type = %q, want 'signup'", eventType)
	}
}
