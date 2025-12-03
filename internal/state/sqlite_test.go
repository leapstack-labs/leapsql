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
	tables := []string{"runs", "models", "model_runs", "dependencies", "environments"}
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
