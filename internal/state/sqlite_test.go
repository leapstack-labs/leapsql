package state

import (
	"testing"
	"time"
)

func setupTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store := NewSQLiteStore()
	if err := store.Open(":memory:"); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}
	return store
}

func TestSQLiteStore_OpenClose(t *testing.T) {
	store := NewSQLiteStore()

	if err := store.Open(":memory:"); err != nil {
		t.Fatalf("failed to open in-memory store: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}
}

func TestSQLiteStore_InitSchema(t *testing.T) {
	store := NewSQLiteStore()
	if err := store.Open(":memory:"); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	if err := store.InitSchema(); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}

	// Verify tables exist by querying them
	tables := []string{"runs", "models", "model_runs", "dependencies", "environments", "model_columns", "column_lineage"}
	for _, table := range tables {
		rows, err := store.db.Query("SELECT 1 FROM " + table + " LIMIT 1")
		if err != nil {
			t.Errorf("table %s does not exist: %v", table, err)
		} else {
			rows.Close()
		}
	}
}

// --- Run tests ---

func TestSQLiteStore_CreateRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	run, err := store.CreateRun("production")
	if err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	if run.ID == "" {
		t.Error("run ID should not be empty")
	}
	if run.Environment != "production" {
		t.Errorf("expected environment 'production', got %q", run.Environment)
	}
	if run.Status != RunStatusRunning {
		t.Errorf("expected status 'running', got %q", run.Status)
	}
}

func TestSQLiteStore_GetRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	created, err := store.CreateRun("staging")
	if err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	retrieved, err := store.GetRun(created.ID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}

	if retrieved.ID != created.ID {
		t.Errorf("expected ID %q, got %q", created.ID, retrieved.ID)
	}
	if retrieved.Environment != "staging" {
		t.Errorf("expected environment 'staging', got %q", retrieved.Environment)
	}
}

func TestSQLiteStore_GetRun_NotFound(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	_, err := store.GetRun("nonexistent-id")
	if err == nil {
		t.Error("expected error for nonexistent run")
	}
}

func TestSQLiteStore_CompleteRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	run, _ := store.CreateRun("dev")

	err := store.CompleteRun(run.ID, RunStatusCompleted, "")
	if err != nil {
		t.Fatalf("failed to complete run: %v", err)
	}

	retrieved, _ := store.GetRun(run.ID)
	if retrieved.Status != RunStatusCompleted {
		t.Errorf("expected status 'completed', got %q", retrieved.Status)
	}
	if retrieved.CompletedAt == nil {
		t.Error("completed_at should not be nil")
	}
}

func TestSQLiteStore_CompleteRun_WithError(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	run, _ := store.CreateRun("dev")

	err := store.CompleteRun(run.ID, RunStatusFailed, "something went wrong")
	if err != nil {
		t.Fatalf("failed to complete run: %v", err)
	}

	retrieved, _ := store.GetRun(run.ID)
	if retrieved.Status != RunStatusFailed {
		t.Errorf("expected status 'failed', got %q", retrieved.Status)
	}
	if retrieved.Error != "something went wrong" {
		t.Errorf("expected error message, got %q", retrieved.Error)
	}
}

func TestSQLiteStore_GetLatestRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create multiple runs
	store.CreateRun("prod")
	time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	run2, _ := store.CreateRun("prod")

	latest, err := store.GetLatestRun("prod")
	if err != nil {
		t.Fatalf("failed to get latest run: %v", err)
	}

	if latest.ID != run2.ID {
		t.Errorf("expected latest run ID %q, got %q", run2.ID, latest.ID)
	}
}

func TestSQLiteStore_GetLatestRun_NoRuns(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	latest, err := store.GetLatestRun("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if latest != nil {
		t.Error("expected nil for nonexistent environment")
	}
}

// --- Model tests ---

func TestSQLiteStore_RegisterModel(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:         "models.staging.stg_users",
		Name:         "stg_users",
		Materialized: "table",
		ContentHash:  "abc123",
	}

	err := store.RegisterModel(model)
	if err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	if model.ID == "" {
		t.Error("model ID should be generated")
	}
}

func TestSQLiteStore_RegisterModel_Upsert(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:         "models.staging.stg_users",
		Name:         "stg_users",
		Materialized: "table",
		ContentHash:  "abc123",
	}

	store.RegisterModel(model)

	// Update with new hash
	model.ContentHash = "def456"
	err := store.RegisterModel(model)
	if err != nil {
		t.Fatalf("failed to upsert model: %v", err)
	}

	retrieved, _ := store.GetModelByPath("models.staging.stg_users")
	if retrieved.ContentHash != "def456" {
		t.Errorf("expected hash 'def456', got %q", retrieved.ContentHash)
	}
}

func TestSQLiteStore_GetModelByID(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:         "models.staging.stg_orders",
		Name:         "stg_orders",
		Materialized: "view",
		ContentHash:  "hash123",
	}
	store.RegisterModel(model)

	retrieved, err := store.GetModelByID(model.ID)
	if err != nil {
		t.Fatalf("failed to get model: %v", err)
	}

	if retrieved.Name != "stg_orders" {
		t.Errorf("expected name 'stg_orders', got %q", retrieved.Name)
	}
}

func TestSQLiteStore_GetModelByPath(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:         "models.marts.revenue",
		Name:         "revenue",
		Materialized: "incremental",
		UniqueKey:    "transaction_id",
		ContentHash:  "xyz789",
	}
	store.RegisterModel(model)

	retrieved, err := store.GetModelByPath("models.marts.revenue")
	if err != nil {
		t.Fatalf("failed to get model: %v", err)
	}

	if retrieved.Materialized != "incremental" {
		t.Errorf("expected materialized 'incremental', got %q", retrieved.Materialized)
	}
	if retrieved.UniqueKey != "transaction_id" {
		t.Errorf("expected unique_key 'transaction_id', got %q", retrieved.UniqueKey)
	}
}

func TestSQLiteStore_GetModelByPath_NotFound(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	retrieved, err := store.GetModelByPath("nonexistent.model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if retrieved != nil {
		t.Error("expected nil for nonexistent model")
	}
}

func TestSQLiteStore_UpdateModelHash(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:         "models.test",
		Name:         "test",
		Materialized: "table",
		ContentHash:  "original",
	}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	err := store.UpdateModelHash(model.ID, "updated")
	if err != nil {
		t.Fatalf("failed to update hash: %v", err)
	}

	retrieved, _ := store.GetModelByID(model.ID)
	if retrieved.ContentHash != "updated" {
		t.Errorf("expected hash 'updated', got %q", retrieved.ContentHash)
	}
}

func TestSQLiteStore_ListModels(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	models := []*Model{
		{Path: "models.a", Name: "a", Materialized: "table", ContentHash: "1"},
		{Path: "models.b", Name: "b", Materialized: "table", ContentHash: "2"},
		{Path: "models.c", Name: "c", Materialized: "table", ContentHash: "3"},
	}

	for _, m := range models {
		if err := store.RegisterModel(m); err != nil {
			t.Fatalf("failed to register model: %v", err)
		}
	}

	list, err := store.ListModels()
	if err != nil {
		t.Fatalf("failed to list models: %v", err)
	}

	if len(list) != 3 {
		t.Errorf("expected 3 models, got %d", len(list))
	}
}

// --- Model run tests ---

func TestSQLiteStore_RecordModelRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	run, _ := store.CreateRun("test")
	model := &Model{Path: "models.test", Name: "test", Materialized: "table", ContentHash: "hash"}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	modelRun := &ModelRun{
		RunID:   run.ID,
		ModelID: model.ID,
		Status:  ModelRunStatusRunning,
	}

	err := store.RecordModelRun(modelRun)
	if err != nil {
		t.Fatalf("failed to record model run: %v", err)
	}

	if modelRun.ID == "" {
		t.Error("model run ID should be generated")
	}
}

func TestSQLiteStore_UpdateModelRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	run, _ := store.CreateRun("test")
	model := &Model{Path: "models.test", Name: "test", Materialized: "table", ContentHash: "hash"}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	modelRun := &ModelRun{
		RunID:   run.ID,
		ModelID: model.ID,
		Status:  ModelRunStatusRunning,
	}
	if err := store.RecordModelRun(modelRun); err != nil {
		t.Fatalf("failed to record model run: %v", err)
	}

	time.Sleep(10 * time.Millisecond) // Ensure some execution time

	err := store.UpdateModelRun(modelRun.ID, ModelRunStatusSuccess, 100, "")
	if err != nil {
		t.Fatalf("failed to update model run: %v", err)
	}

	runs, _ := store.GetModelRunsForRun(run.ID)
	if len(runs) != 1 {
		t.Fatalf("expected 1 model run, got %d", len(runs))
	}

	if runs[0].Status != ModelRunStatusSuccess {
		t.Errorf("expected status 'success', got %q", runs[0].Status)
	}
	if runs[0].RowsAffected != 100 {
		t.Errorf("expected 100 rows affected, got %d", runs[0].RowsAffected)
	}
	if runs[0].ExecutionMS == 0 {
		t.Error("execution_ms should be > 0")
	}
}

func TestSQLiteStore_GetLatestModelRun(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	run1, err := store.CreateRun("test")
	if err != nil {
		t.Fatalf("failed to create run1: %v", err)
	}
	run2, err := store.CreateRun("test")
	if err != nil {
		t.Fatalf("failed to create run2: %v", err)
	}

	model := &Model{Path: "models.test", Name: "test", ContentHash: "hash"}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	mr1 := &ModelRun{RunID: run1.ID, ModelID: model.ID, Status: ModelRunStatusSuccess}
	if err := store.RecordModelRun(mr1); err != nil {
		t.Fatalf("failed to record model run 1: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	mr2 := &ModelRun{RunID: run2.ID, ModelID: model.ID, Status: ModelRunStatusRunning}
	if err := store.RecordModelRun(mr2); err != nil {
		t.Fatalf("failed to record model run 2: %v", err)
	}

	latest, err := store.GetLatestModelRun(model.ID)
	if err != nil {
		t.Fatalf("failed to get latest model run: %v", err)
	}

	if latest == nil {
		t.Fatal("expected latest model run, got nil")
	}

	if latest.ID != mr2.ID {
		t.Errorf("expected latest model run ID %q, got %q", mr2.ID, latest.ID)
	}
}

// --- Dependency tests ---

func TestSQLiteStore_SetDependencies(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create models
	parent1 := &Model{Path: "models.parent1", Name: "parent1", ContentHash: "1"}
	parent2 := &Model{Path: "models.parent2", Name: "parent2", ContentHash: "2"}
	child := &Model{Path: "models.child", Name: "child", ContentHash: "3"}

	store.RegisterModel(parent1)
	store.RegisterModel(parent2)
	store.RegisterModel(child)

	err := store.SetDependencies(child.ID, []string{parent1.ID, parent2.ID})
	if err != nil {
		t.Fatalf("failed to set dependencies: %v", err)
	}

	deps, _ := store.GetDependencies(child.ID)
	if len(deps) != 2 {
		t.Errorf("expected 2 dependencies, got %d", len(deps))
	}
}

func TestSQLiteStore_SetDependencies_Replace(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	parent1 := &Model{Path: "models.p1", Name: "p1", ContentHash: "1"}
	parent2 := &Model{Path: "models.p2", Name: "p2", ContentHash: "2"}
	child := &Model{Path: "models.c", Name: "c", ContentHash: "3"}

	store.RegisterModel(parent1)
	store.RegisterModel(parent2)
	store.RegisterModel(child)

	// Set initial dependency
	store.SetDependencies(child.ID, []string{parent1.ID})

	// Replace with new dependency
	err := store.SetDependencies(child.ID, []string{parent2.ID})
	if err != nil {
		t.Fatalf("failed to replace dependencies: %v", err)
	}

	deps, _ := store.GetDependencies(child.ID)
	if len(deps) != 1 {
		t.Errorf("expected 1 dependency, got %d", len(deps))
	}
	if deps[0] != parent2.ID {
		t.Errorf("expected parent2 ID, got %q", deps[0])
	}
}

func TestSQLiteStore_GetDependents(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	parent := &Model{Path: "models.parent", Name: "parent", ContentHash: "1"}
	child1 := &Model{Path: "models.child1", Name: "child1", ContentHash: "2"}
	child2 := &Model{Path: "models.child2", Name: "child2", ContentHash: "3"}

	store.RegisterModel(parent)
	store.RegisterModel(child1)
	store.RegisterModel(child2)

	store.SetDependencies(child1.ID, []string{parent.ID})
	store.SetDependencies(child2.ID, []string{parent.ID})

	dependents, err := store.GetDependents(parent.ID)
	if err != nil {
		t.Fatalf("failed to get dependents: %v", err)
	}

	if len(dependents) != 2 {
		t.Errorf("expected 2 dependents, got %d", len(dependents))
	}
}

// --- Environment tests ---

func TestSQLiteStore_CreateEnvironment(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	env, err := store.CreateEnvironment("staging")
	if err != nil {
		t.Fatalf("failed to create environment: %v", err)
	}

	if env.Name != "staging" {
		t.Errorf("expected name 'staging', got %q", env.Name)
	}
}

func TestSQLiteStore_GetEnvironment(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	store.CreateEnvironment("production")

	env, err := store.GetEnvironment("production")
	if err != nil {
		t.Fatalf("failed to get environment: %v", err)
	}

	if env.Name != "production" {
		t.Errorf("expected name 'production', got %q", env.Name)
	}
}

func TestSQLiteStore_GetEnvironment_NotFound(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	env, err := store.GetEnvironment("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env != nil {
		t.Error("expected nil for nonexistent environment")
	}
}

func TestSQLiteStore_UpdateEnvironmentRef(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	store.CreateEnvironment("dev")

	err := store.UpdateEnvironmentRef("dev", "abc123")
	if err != nil {
		t.Fatalf("failed to update environment ref: %v", err)
	}

	env, _ := store.GetEnvironment("dev")
	if env.CommitRef != "abc123" {
		t.Errorf("expected commit_ref 'abc123', got %q", env.CommitRef)
	}
}

// --- Column lineage tests ---

func TestSQLiteStore_SaveModelColumns(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Register the model first (required for foreign key)
	model := &Model{
		Path:        "staging.stg_customers",
		Name:        "stg_customers",
		ContentHash: "abc123",
	}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	columns := []ColumnInfo{
		{
			Name:          "customer_id",
			Index:         0,
			TransformType: "",
			Function:      "",
			Sources: []SourceRef{
				{Table: "raw_customers", Column: "id"},
			},
		},
		{
			Name:          "full_name",
			Index:         1,
			TransformType: "EXPR",
			Function:      "concat",
			Sources: []SourceRef{
				{Table: "raw_customers", Column: "first_name"},
				{Table: "raw_customers", Column: "last_name"},
			},
		},
	}

	err := store.SaveModelColumns("staging.stg_customers", columns)
	if err != nil {
		t.Fatalf("failed to save model columns: %v", err)
	}

	// Verify columns were saved
	retrieved, err := store.GetModelColumns("staging.stg_customers")
	if err != nil {
		t.Fatalf("failed to get model columns: %v", err)
	}

	if len(retrieved) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(retrieved))
	}

	// Check first column
	if retrieved[0].Name != "customer_id" {
		t.Errorf("expected column name 'customer_id', got %q", retrieved[0].Name)
	}
	if len(retrieved[0].Sources) != 1 {
		t.Errorf("expected 1 source for customer_id, got %d", len(retrieved[0].Sources))
	}

	// Check second column
	if retrieved[1].Name != "full_name" {
		t.Errorf("expected column name 'full_name', got %q", retrieved[1].Name)
	}
	if retrieved[1].TransformType != "EXPR" {
		t.Errorf("expected transform_type 'EXPR', got %q", retrieved[1].TransformType)
	}
	if retrieved[1].Function != "concat" {
		t.Errorf("expected function 'concat', got %q", retrieved[1].Function)
	}
	if len(retrieved[1].Sources) != 2 {
		t.Errorf("expected 2 sources for full_name, got %d", len(retrieved[1].Sources))
	}
}

func TestSQLiteStore_SaveModelColumns_Upsert(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:        "staging.stg_orders",
		Name:        "stg_orders",
		ContentHash: "abc123",
	}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	// Initial save
	initialColumns := []ColumnInfo{
		{Name: "order_id", Index: 0, Sources: []SourceRef{{Table: "raw_orders", Column: "id"}}},
	}
	if err := store.SaveModelColumns("staging.stg_orders", initialColumns); err != nil {
		t.Fatalf("failed to save initial columns: %v", err)
	}

	// Update with different columns (should replace)
	updatedColumns := []ColumnInfo{
		{Name: "order_id", Index: 0, Sources: []SourceRef{{Table: "raw_orders", Column: "order_id"}}},
		{Name: "total", Index: 1, TransformType: "EXPR", Function: "sum", Sources: []SourceRef{{Table: "raw_orders", Column: "amount"}}},
	}
	if err := store.SaveModelColumns("staging.stg_orders", updatedColumns); err != nil {
		t.Fatalf("failed to save updated columns: %v", err)
	}

	retrieved, err := store.GetModelColumns("staging.stg_orders")
	if err != nil {
		t.Fatalf("failed to get model columns: %v", err)
	}

	if len(retrieved) != 2 {
		t.Fatalf("expected 2 columns after update, got %d", len(retrieved))
	}

	// Check that the source was updated
	if len(retrieved[0].Sources) != 1 || retrieved[0].Sources[0].Column != "order_id" {
		t.Errorf("expected source column 'order_id', got %v", retrieved[0].Sources)
	}
}

func TestSQLiteStore_GetModelColumns_NotFound(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	columns, err := store.GetModelColumns("nonexistent.model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(columns) != 0 {
		t.Errorf("expected empty slice for nonexistent model, got %d columns", len(columns))
	}
}

func TestSQLiteStore_DeleteModelColumns(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:        "staging.stg_products",
		Name:        "stg_products",
		ContentHash: "abc123",
	}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	columns := []ColumnInfo{
		{Name: "product_id", Index: 0, Sources: []SourceRef{{Table: "raw_products", Column: "id"}}},
		{Name: "name", Index: 1, Sources: []SourceRef{{Table: "raw_products", Column: "name"}}},
	}
	if err := store.SaveModelColumns("staging.stg_products", columns); err != nil {
		t.Fatalf("failed to save columns: %v", err)
	}

	// Delete columns
	err := store.DeleteModelColumns("staging.stg_products")
	if err != nil {
		t.Fatalf("failed to delete columns: %v", err)
	}

	// Verify deletion
	retrieved, err := store.GetModelColumns("staging.stg_products")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(retrieved) != 0 {
		t.Errorf("expected 0 columns after deletion, got %d", len(retrieved))
	}
}

func TestSQLiteStore_TraceColumnBackward(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create a chain of models:
	// raw_customers -> stg_customers -> customer_summary
	// The trace should show the upstream lineage

	stgModel := &Model{
		Path:        "staging.stg_customers",
		Name:        "stg_customers",
		ContentHash: "abc",
	}
	martModel := &Model{
		Path:        "marts.customer_summary",
		Name:        "customer_summary",
		ContentHash: "def",
	}
	if err := store.RegisterModel(stgModel); err != nil {
		t.Fatalf("failed to register stg model: %v", err)
	}
	if err := store.RegisterModel(martModel); err != nil {
		t.Fatalf("failed to register mart model: %v", err)
	}

	// stg_customers gets customer_id from raw_customers.id
	stgColumns := []ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "raw_customers", Column: "id"}}},
	}
	if err := store.SaveModelColumns("staging.stg_customers", stgColumns); err != nil {
		t.Fatalf("failed to save stg columns: %v", err)
	}

	// customer_summary gets customer_id from stg_customers.customer_id
	martColumns := []ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
	}
	if err := store.SaveModelColumns("marts.customer_summary", martColumns); err != nil {
		t.Fatalf("failed to save mart columns: %v", err)
	}

	// Trace backward from customer_summary.customer_id
	results, err := store.TraceColumnBackward("marts.customer_summary", "customer_id")
	if err != nil {
		t.Fatalf("failed to trace column backward: %v", err)
	}

	// Should find at least the direct source (stg_customers.customer_id)
	if len(results) == 0 {
		t.Fatal("expected at least 1 trace result, got 0")
	}

	// First result should be depth 1 (direct source)
	foundStgCustomers := false
	foundRawCustomers := false
	for _, r := range results {
		if r.ModelPath == "stg_customers" && r.ColumnName == "customer_id" && r.Depth == 1 {
			foundStgCustomers = true
			if r.IsExternal {
				t.Error("stg_customers should not be marked as external")
			}
		}
		if r.ModelPath == "raw_customers" && r.ColumnName == "id" && r.Depth == 2 {
			foundRawCustomers = true
			if !r.IsExternal {
				t.Error("raw_customers should be marked as external (not a registered model)")
			}
		}
	}

	if !foundStgCustomers {
		t.Error("did not find stg_customers.customer_id at depth 1")
	}
	if !foundRawCustomers {
		t.Error("did not find raw_customers.id at depth 2")
	}
}

func TestSQLiteStore_TraceColumnForward(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create models: stg_customers -> customer_summary
	stgModel := &Model{
		Path:        "staging.stg_customers",
		Name:        "stg_customers",
		ContentHash: "abc",
	}
	martModel := &Model{
		Path:        "marts.customer_summary",
		Name:        "customer_summary",
		ContentHash: "def",
	}
	if err := store.RegisterModel(stgModel); err != nil {
		t.Fatalf("failed to register stg model: %v", err)
	}
	if err := store.RegisterModel(martModel); err != nil {
		t.Fatalf("failed to register mart model: %v", err)
	}

	// stg_customers has customer_id
	stgColumns := []ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "raw_customers", Column: "id"}}},
		{Name: "email", Index: 1, Sources: []SourceRef{{Table: "raw_customers", Column: "email"}}},
	}
	if err := store.SaveModelColumns("staging.stg_customers", stgColumns); err != nil {
		t.Fatalf("failed to save stg columns: %v", err)
	}

	// customer_summary uses stg_customers.customer_id
	martColumns := []ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
		{Name: "contact", Index: 1, Sources: []SourceRef{{Table: "stg_customers", Column: "email"}}},
	}
	if err := store.SaveModelColumns("marts.customer_summary", martColumns); err != nil {
		t.Fatalf("failed to save mart columns: %v", err)
	}

	// Trace forward from stg_customers.customer_id - where does it go?
	results, err := store.TraceColumnForward("staging.stg_customers", "customer_id")
	if err != nil {
		t.Fatalf("failed to trace column forward: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 trace result, got 0")
	}

	// Should find customer_summary.customer_id at depth 1
	found := false
	for _, r := range results {
		if r.ModelPath == "marts.customer_summary" && r.ColumnName == "customer_id" && r.Depth == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Error("did not find marts.customer_summary.customer_id at depth 1")
	}
}

func TestSQLiteStore_TraceColumnForward_MultipleConsumers(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create models: stg_customers -> (customer_summary, customer_metrics)
	stgModel := &Model{Path: "staging.stg_customers", Name: "stg_customers", ContentHash: "abc"}
	summaryModel := &Model{Path: "marts.customer_summary", Name: "customer_summary", ContentHash: "def"}
	metricsModel := &Model{Path: "marts.customer_metrics", Name: "customer_metrics", ContentHash: "ghi"}

	for _, m := range []*Model{stgModel, summaryModel, metricsModel} {
		if err := store.RegisterModel(m); err != nil {
			t.Fatalf("failed to register model %s: %v", m.Path, err)
		}
	}

	// stg_customers has customer_id
	if err := store.SaveModelColumns("staging.stg_customers", []ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "raw", Column: "id"}}},
	}); err != nil {
		t.Fatalf("failed to save stg columns: %v", err)
	}

	// Both mart models use stg_customers.customer_id
	if err := store.SaveModelColumns("marts.customer_summary", []ColumnInfo{
		{Name: "cust_id", Index: 0, Sources: []SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
	}); err != nil {
		t.Fatalf("failed to save summary columns: %v", err)
	}
	if err := store.SaveModelColumns("marts.customer_metrics", []ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
	}); err != nil {
		t.Fatalf("failed to save metrics columns: %v", err)
	}

	// Trace forward - should find both consumers
	results, err := store.TraceColumnForward("staging.stg_customers", "customer_id")
	if err != nil {
		t.Fatalf("failed to trace column forward: %v", err)
	}

	if len(results) < 2 {
		t.Fatalf("expected at least 2 trace results, got %d", len(results))
	}

	foundSummary := false
	foundMetrics := false
	for _, r := range results {
		if r.ModelPath == "marts.customer_summary" {
			foundSummary = true
		}
		if r.ModelPath == "marts.customer_metrics" {
			foundMetrics = true
		}
	}

	if !foundSummary {
		t.Error("did not find marts.customer_summary in forward trace")
	}
	if !foundMetrics {
		t.Error("did not find marts.customer_metrics in forward trace")
	}
}

func TestSQLiteStore_TraceColumn_EmptyResults(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Trace a column that doesn't exist
	backward, err := store.TraceColumnBackward("nonexistent.model", "col")
	if err != nil {
		t.Fatalf("unexpected error on backward trace: %v", err)
	}
	if len(backward) != 0 {
		t.Errorf("expected empty backward trace, got %d results", len(backward))
	}

	forward, err := store.TraceColumnForward("nonexistent.model", "col")
	if err != nil {
		t.Fatalf("unexpected error on forward trace: %v", err)
	}
	if len(forward) != 0 {
		t.Errorf("expected empty forward trace, got %d results", len(forward))
	}
}
