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

// --- Run lifecycle tests ---

func TestSQLiteStore_RunLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, store *SQLiteStore) *Run
		operation func(t *testing.T, store *SQLiteStore, run *Run)
		verify    func(t *testing.T, store *SQLiteStore, run *Run)
	}{
		{
			name: "create run",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				run, err := store.CreateRun("production")
				if err != nil {
					t.Fatalf("failed to create run: %v", err)
				}
				return run
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run) {
				if run.ID == "" {
					t.Error("run ID should not be empty")
				}
				if run.Environment != "production" {
					t.Errorf("expected environment 'production', got %q", run.Environment)
				}
				if run.Status != RunStatusRunning {
					t.Errorf("expected status 'running', got %q", run.Status)
				}
			},
		},
		{
			name: "get run",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				run, err := store.CreateRun("staging")
				if err != nil {
					t.Fatalf("failed to create run: %v", err)
				}
				return run
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run) {
				retrieved, err := store.GetRun(run.ID)
				if err != nil {
					t.Fatalf("failed to get run: %v", err)
				}
				if retrieved.ID != run.ID {
					t.Errorf("expected ID %q, got %q", run.ID, retrieved.ID)
				}
				if retrieved.Environment != "staging" {
					t.Errorf("expected environment 'staging', got %q", retrieved.Environment)
				}
			},
		},
		{
			name: "get run not found",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				return nil
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run) {
				_, err := store.GetRun("nonexistent-id")
				if err == nil {
					t.Error("expected error for nonexistent run")
				}
			},
		},
		{
			name: "complete run success",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				run, _ := store.CreateRun("dev")
				return run
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run) {
				err := store.CompleteRun(run.ID, RunStatusCompleted, "")
				if err != nil {
					t.Fatalf("failed to complete run: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run) {
				retrieved, _ := store.GetRun(run.ID)
				if retrieved.Status != RunStatusCompleted {
					t.Errorf("expected status 'completed', got %q", retrieved.Status)
				}
				if retrieved.CompletedAt == nil {
					t.Error("completed_at should not be nil")
				}
			},
		},
		{
			name: "complete run with error",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				run, _ := store.CreateRun("dev")
				return run
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run) {
				err := store.CompleteRun(run.ID, RunStatusFailed, "something went wrong")
				if err != nil {
					t.Fatalf("failed to complete run: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run) {
				retrieved, _ := store.GetRun(run.ID)
				if retrieved.Status != RunStatusFailed {
					t.Errorf("expected status 'failed', got %q", retrieved.Status)
				}
				if retrieved.Error != "something went wrong" {
					t.Errorf("expected error message, got %q", retrieved.Error)
				}
			},
		},
		{
			name: "get latest run",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				store.CreateRun("prod")
				time.Sleep(10 * time.Millisecond)
				run2, _ := store.CreateRun("prod")
				return run2
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run) {
				latest, err := store.GetLatestRun("prod")
				if err != nil {
					t.Fatalf("failed to get latest run: %v", err)
				}
				if latest.ID != run.ID {
					t.Errorf("expected latest run ID %q, got %q", run.ID, latest.ID)
				}
			},
		},
		{
			name: "get latest run no runs",
			setup: func(t *testing.T, store *SQLiteStore) *Run {
				return nil
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run) {
				latest, err := store.GetLatestRun("nonexistent")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if latest != nil {
					t.Error("expected nil for nonexistent environment")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			var run *Run
			if tt.setup != nil {
				run = tt.setup(t, store)
			}
			if tt.operation != nil {
				tt.operation(t, store, run)
			}
			if tt.verify != nil {
				tt.verify(t, store, run)
			}
		})
	}
}

// --- Model operations tests ---

func TestSQLiteStore_ModelOperations(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, store *SQLiteStore) *Model
		operation func(t *testing.T, store *SQLiteStore, model *Model)
		verify    func(t *testing.T, store *SQLiteStore, model *Model)
	}{
		{
			name: "register model",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
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
				return model
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
				if model.ID == "" {
					t.Error("model ID should be generated")
				}
			},
		},
		{
			name: "register model upsert",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
				model := &Model{
					Path:         "models.staging.stg_users",
					Name:         "stg_users",
					Materialized: "table",
					ContentHash:  "abc123",
				}
				store.RegisterModel(model)
				return model
			},
			operation: func(t *testing.T, store *SQLiteStore, model *Model) {
				model.ContentHash = "def456"
				err := store.RegisterModel(model)
				if err != nil {
					t.Fatalf("failed to upsert model: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
				retrieved, _ := store.GetModelByPath("models.staging.stg_users")
				if retrieved.ContentHash != "def456" {
					t.Errorf("expected hash 'def456', got %q", retrieved.ContentHash)
				}
			},
		},
		{
			name: "get model by ID",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
				model := &Model{
					Path:         "models.staging.stg_orders",
					Name:         "stg_orders",
					Materialized: "view",
					ContentHash:  "hash123",
				}
				store.RegisterModel(model)
				return model
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
				retrieved, err := store.GetModelByID(model.ID)
				if err != nil {
					t.Fatalf("failed to get model: %v", err)
				}
				if retrieved.Name != "stg_orders" {
					t.Errorf("expected name 'stg_orders', got %q", retrieved.Name)
				}
			},
		},
		{
			name: "get model by path",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
				model := &Model{
					Path:         "models.marts.revenue",
					Name:         "revenue",
					Materialized: "incremental",
					UniqueKey:    "transaction_id",
					ContentHash:  "xyz789",
				}
				store.RegisterModel(model)
				return model
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
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
			},
		},
		{
			name: "get model by path not found",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
				return nil
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
				retrieved, err := store.GetModelByPath("nonexistent.model")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if retrieved != nil {
					t.Error("expected nil for nonexistent model")
				}
			},
		},
		{
			name: "update model hash",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
				model := &Model{
					Path:         "models.test",
					Name:         "test",
					Materialized: "table",
					ContentHash:  "original",
				}
				if err := store.RegisterModel(model); err != nil {
					t.Fatalf("failed to register model: %v", err)
				}
				return model
			},
			operation: func(t *testing.T, store *SQLiteStore, model *Model) {
				err := store.UpdateModelHash(model.ID, "updated")
				if err != nil {
					t.Fatalf("failed to update hash: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
				retrieved, _ := store.GetModelByID(model.ID)
				if retrieved.ContentHash != "updated" {
					t.Errorf("expected hash 'updated', got %q", retrieved.ContentHash)
				}
			},
		},
		{
			name: "list models",
			setup: func(t *testing.T, store *SQLiteStore) *Model {
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
				return nil
			},
			verify: func(t *testing.T, store *SQLiteStore, model *Model) {
				list, err := store.ListModels()
				if err != nil {
					t.Fatalf("failed to list models: %v", err)
				}
				if len(list) != 3 {
					t.Errorf("expected 3 models, got %d", len(list))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			var model *Model
			if tt.setup != nil {
				model = tt.setup(t, store)
			}
			if tt.operation != nil {
				tt.operation(t, store, model)
			}
			if tt.verify != nil {
				tt.verify(t, store, model)
			}
		})
	}
}

// --- Model frontmatter fields tests ---

func TestSQLiteStore_ModelFrontmatter(t *testing.T) {
	tests := []struct {
		name   string
		model  *Model
		verify func(t *testing.T, retrieved *Model)
	}{
		{
			name: "with all frontmatter fields",
			model: &Model{
				Path:         "models.staging.stg_users",
				Name:         "stg_users",
				Materialized: "incremental",
				UniqueKey:    "user_id",
				ContentHash:  "abc123",
				Owner:        "data-team",
				Schema:       "analytics",
				Tags:         []string{"pii", "daily"},
				Tests: []TestConfig{
					{Unique: []string{"user_id"}},
					{NotNull: []string{"user_id", "email"}},
				},
				Meta: map[string]any{
					"priority": "high",
					"sla":      24,
				},
			},
			verify: func(t *testing.T, retrieved *Model) {
				if retrieved.Owner != "data-team" {
					t.Errorf("expected owner 'data-team', got %q", retrieved.Owner)
				}
				if retrieved.Schema != "analytics" {
					t.Errorf("expected schema 'analytics', got %q", retrieved.Schema)
				}
				if len(retrieved.Tags) != 2 || retrieved.Tags[0] != "pii" || retrieved.Tags[1] != "daily" {
					t.Errorf("expected tags [pii, daily], got %v", retrieved.Tags)
				}
				if len(retrieved.Tests) != 2 {
					t.Errorf("expected 2 tests, got %d", len(retrieved.Tests))
				}
				if retrieved.Tests[0].Unique[0] != "user_id" {
					t.Errorf("expected first test unique ['user_id'], got %v", retrieved.Tests[0].Unique)
				}
				if retrieved.Meta["priority"] != "high" {
					t.Errorf("expected meta.priority 'high', got %v", retrieved.Meta["priority"])
				}
				if sla, ok := retrieved.Meta["sla"].(float64); !ok || sla != 24 {
					t.Errorf("expected meta.sla 24, got %v", retrieved.Meta["sla"])
				}
			},
		},
		{
			name: "with empty optional fields",
			model: &Model{
				Path:         "models.simple",
				Name:         "simple",
				Materialized: "table",
				ContentHash:  "hash123",
			},
			verify: func(t *testing.T, retrieved *Model) {
				if retrieved.Owner != "" {
					t.Errorf("expected empty owner, got %q", retrieved.Owner)
				}
				if retrieved.Schema != "" {
					t.Errorf("expected empty schema, got %q", retrieved.Schema)
				}
				if len(retrieved.Tags) != 0 {
					t.Errorf("expected empty tags, got %v", retrieved.Tags)
				}
				if len(retrieved.Tests) != 0 {
					t.Errorf("expected empty tests, got %v", retrieved.Tests)
				}
				if len(retrieved.Meta) != 0 {
					t.Errorf("expected empty meta, got %v", retrieved.Meta)
				}
			},
		},
		{
			name: "with accepted values test",
			model: &Model{
				Path:         "models.accepted_values_test",
				Name:         "accepted_values_test",
				Materialized: "table",
				ContentHash:  "hash",
				Tests: []TestConfig{
					{
						AcceptedValues: &AcceptedValuesConfig{
							Column: "status",
							Values: []string{"active", "inactive", "pending"},
						},
					},
				},
			},
			verify: func(t *testing.T, retrieved *Model) {
				if len(retrieved.Tests) != 1 {
					t.Fatalf("expected 1 test, got %d", len(retrieved.Tests))
				}
				if retrieved.Tests[0].AcceptedValues == nil {
					t.Fatal("expected AcceptedValues to not be nil")
				}
				if retrieved.Tests[0].AcceptedValues.Column != "status" {
					t.Errorf("expected column 'status', got %q", retrieved.Tests[0].AcceptedValues.Column)
				}
				if len(retrieved.Tests[0].AcceptedValues.Values) != 3 {
					t.Errorf("expected 3 values, got %d", len(retrieved.Tests[0].AcceptedValues.Values))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			if err := store.RegisterModel(tt.model); err != nil {
				t.Fatalf("failed to register model: %v", err)
			}

			retrieved, err := store.GetModelByPath(tt.model.Path)
			if err != nil {
				t.Fatalf("failed to get model: %v", err)
			}

			tt.verify(t, retrieved)
		})
	}
}

func TestSQLiteStore_ModelFrontmatter_Update(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Register initial model
	model := &Model{
		Path:         "models.update_test",
		Name:         "update_test",
		Materialized: "table",
		ContentHash:  "hash1",
		Owner:        "team-a",
		Tags:         []string{"initial"},
	}
	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	// Update the model with new frontmatter fields
	model.ContentHash = "hash2"
	model.Owner = "team-b"
	model.Schema = "new_schema"
	model.Tags = []string{"updated", "v2"}
	model.Tests = []TestConfig{{NotNull: []string{"id"}}}
	model.Meta = map[string]any{"version": 2}

	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to update model: %v", err)
	}

	retrieved, err := store.GetModelByPath("models.update_test")
	if err != nil {
		t.Fatalf("failed to get model: %v", err)
	}

	if retrieved.Owner != "team-b" {
		t.Errorf("expected owner 'team-b', got %q", retrieved.Owner)
	}
	if retrieved.Schema != "new_schema" {
		t.Errorf("expected schema 'new_schema', got %q", retrieved.Schema)
	}
	if len(retrieved.Tags) != 2 || retrieved.Tags[0] != "updated" {
		t.Errorf("expected tags [updated, v2], got %v", retrieved.Tags)
	}
	if len(retrieved.Tests) != 1 {
		t.Errorf("expected 1 test, got %d", len(retrieved.Tests))
	}
	if version, ok := retrieved.Meta["version"].(float64); !ok || version != 2 {
		t.Errorf("expected meta.version 2, got %v", retrieved.Meta["version"])
	}
}

func TestSQLiteStore_GetModelByID_WithFrontmatterFields(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	model := &Model{
		Path:         "models.get_by_id_test",
		Name:         "get_by_id_test",
		Materialized: "view",
		ContentHash:  "hash123",
		Owner:        "analytics",
		Schema:       "reporting",
		Tags:         []string{"finance"},
		Meta:         map[string]any{"department": "finance"},
	}

	if err := store.RegisterModel(model); err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	retrieved, err := store.GetModelByID(model.ID)
	if err != nil {
		t.Fatalf("failed to get model by ID: %v", err)
	}

	if retrieved.Owner != "analytics" {
		t.Errorf("expected owner 'analytics', got %q", retrieved.Owner)
	}
	if retrieved.Schema != "reporting" {
		t.Errorf("expected schema 'reporting', got %q", retrieved.Schema)
	}
	if len(retrieved.Tags) != 1 || retrieved.Tags[0] != "finance" {
		t.Errorf("expected tags [finance], got %v", retrieved.Tags)
	}
}

func TestSQLiteStore_ListModels_WithFrontmatterFields(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	models := []*Model{
		{
			Path: "models.list_a", Name: "list_a", Materialized: "table", ContentHash: "1",
			Owner: "team-a", Tags: []string{"tag-a"},
		},
		{
			Path: "models.list_b", Name: "list_b", Materialized: "table", ContentHash: "2",
			Owner: "team-b", Tags: []string{"tag-b"},
		},
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

	if len(list) != 2 {
		t.Fatalf("expected 2 models, got %d", len(list))
	}

	if list[0].Owner != "team-a" {
		t.Errorf("expected owner 'team-a', got %q", list[0].Owner)
	}
	if len(list[0].Tags) != 1 || list[0].Tags[0] != "tag-a" {
		t.Errorf("expected tags [tag-a], got %v", list[0].Tags)
	}

	if list[1].Owner != "team-b" {
		t.Errorf("expected owner 'team-b', got %q", list[1].Owner)
	}
}

// --- Model run tests ---

func TestSQLiteStore_ModelRun(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, store *SQLiteStore) (*Run, *Model)
		operation func(t *testing.T, store *SQLiteStore, run *Run, model *Model) *ModelRun
		verify    func(t *testing.T, store *SQLiteStore, run *Run, modelRun *ModelRun)
	}{
		{
			name: "record model run",
			setup: func(t *testing.T, store *SQLiteStore) (*Run, *Model) {
				run, _ := store.CreateRun("test")
				model := &Model{Path: "models.test", Name: "test", Materialized: "table", ContentHash: "hash"}
				if err := store.RegisterModel(model); err != nil {
					t.Fatalf("failed to register model: %v", err)
				}
				return run, model
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run, model *Model) *ModelRun {
				modelRun := &ModelRun{
					RunID:   run.ID,
					ModelID: model.ID,
					Status:  ModelRunStatusRunning,
				}
				err := store.RecordModelRun(modelRun)
				if err != nil {
					t.Fatalf("failed to record model run: %v", err)
				}
				return modelRun
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run, modelRun *ModelRun) {
				if modelRun.ID == "" {
					t.Error("model run ID should be generated")
				}
			},
		},
		{
			name: "update model run",
			setup: func(t *testing.T, store *SQLiteStore) (*Run, *Model) {
				run, _ := store.CreateRun("test")
				model := &Model{Path: "models.test", Name: "test", Materialized: "table", ContentHash: "hash"}
				if err := store.RegisterModel(model); err != nil {
					t.Fatalf("failed to register model: %v", err)
				}
				return run, model
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run, model *Model) *ModelRun {
				modelRun := &ModelRun{
					RunID:   run.ID,
					ModelID: model.ID,
					Status:  ModelRunStatusRunning,
				}
				if err := store.RecordModelRun(modelRun); err != nil {
					t.Fatalf("failed to record model run: %v", err)
				}

				time.Sleep(10 * time.Millisecond)

				err := store.UpdateModelRun(modelRun.ID, ModelRunStatusSuccess, 100, "")
				if err != nil {
					t.Fatalf("failed to update model run: %v", err)
				}
				return modelRun
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run, modelRun *ModelRun) {
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
			},
		},
		{
			name: "get latest model run",
			setup: func(t *testing.T, store *SQLiteStore) (*Run, *Model) {
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

				return run2, model
			},
			operation: func(t *testing.T, store *SQLiteStore, run *Run, model *Model) *ModelRun {
				mr2 := &ModelRun{RunID: run.ID, ModelID: model.ID, Status: ModelRunStatusRunning}
				if err := store.RecordModelRun(mr2); err != nil {
					t.Fatalf("failed to record model run 2: %v", err)
				}
				return mr2
			},
			verify: func(t *testing.T, store *SQLiteStore, run *Run, modelRun *ModelRun) {
				model, _ := store.GetModelByPath("models.test")
				latest, err := store.GetLatestModelRun(model.ID)
				if err != nil {
					t.Fatalf("failed to get latest model run: %v", err)
				}
				if latest == nil {
					t.Fatal("expected latest model run, got nil")
				}
				if latest.ID != modelRun.ID {
					t.Errorf("expected latest model run ID %q, got %q", modelRun.ID, latest.ID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			var run *Run
			var model *Model
			if tt.setup != nil {
				run, model = tt.setup(t, store)
			}
			var modelRun *ModelRun
			if tt.operation != nil {
				modelRun = tt.operation(t, store, run, model)
			}
			if tt.verify != nil {
				tt.verify(t, store, run, modelRun)
			}
		})
	}
}

// --- Dependency tests ---

func TestSQLiteStore_Dependencies(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T, store *SQLiteStore) []*Model
		verify func(t *testing.T, store *SQLiteStore, models []*Model)
	}{
		{
			name: "set dependencies",
			setup: func(t *testing.T, store *SQLiteStore) []*Model {
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
				return []*Model{parent1, parent2, child}
			},
			verify: func(t *testing.T, store *SQLiteStore, models []*Model) {
				child := models[2]
				deps, _ := store.GetDependencies(child.ID)
				if len(deps) != 2 {
					t.Errorf("expected 2 dependencies, got %d", len(deps))
				}
			},
		},
		{
			name: "replace dependencies",
			setup: func(t *testing.T, store *SQLiteStore) []*Model {
				parent1 := &Model{Path: "models.p1", Name: "p1", ContentHash: "1"}
				parent2 := &Model{Path: "models.p2", Name: "p2", ContentHash: "2"}
				child := &Model{Path: "models.c", Name: "c", ContentHash: "3"}

				store.RegisterModel(parent1)
				store.RegisterModel(parent2)
				store.RegisterModel(child)

				store.SetDependencies(child.ID, []string{parent1.ID})
				err := store.SetDependencies(child.ID, []string{parent2.ID})
				if err != nil {
					t.Fatalf("failed to replace dependencies: %v", err)
				}
				return []*Model{parent1, parent2, child}
			},
			verify: func(t *testing.T, store *SQLiteStore, models []*Model) {
				parent2 := models[1]
				child := models[2]
				deps, _ := store.GetDependencies(child.ID)
				if len(deps) != 1 {
					t.Errorf("expected 1 dependency, got %d", len(deps))
				}
				if deps[0] != parent2.ID {
					t.Errorf("expected parent2 ID, got %q", deps[0])
				}
			},
		},
		{
			name: "get dependents",
			setup: func(t *testing.T, store *SQLiteStore) []*Model {
				parent := &Model{Path: "models.parent", Name: "parent", ContentHash: "1"}
				child1 := &Model{Path: "models.child1", Name: "child1", ContentHash: "2"}
				child2 := &Model{Path: "models.child2", Name: "child2", ContentHash: "3"}

				store.RegisterModel(parent)
				store.RegisterModel(child1)
				store.RegisterModel(child2)

				store.SetDependencies(child1.ID, []string{parent.ID})
				store.SetDependencies(child2.ID, []string{parent.ID})

				return []*Model{parent, child1, child2}
			},
			verify: func(t *testing.T, store *SQLiteStore, models []*Model) {
				parent := models[0]
				dependents, err := store.GetDependents(parent.ID)
				if err != nil {
					t.Fatalf("failed to get dependents: %v", err)
				}
				if len(dependents) != 2 {
					t.Errorf("expected 2 dependents, got %d", len(dependents))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			var models []*Model
			if tt.setup != nil {
				models = tt.setup(t, store)
			}
			if tt.verify != nil {
				tt.verify(t, store, models)
			}
		})
	}
}

// --- Environment tests ---

func TestSQLiteStore_Environment(t *testing.T) {
	tests := []struct {
		name      string
		operation func(t *testing.T, store *SQLiteStore)
	}{
		{
			name: "create environment",
			operation: func(t *testing.T, store *SQLiteStore) {
				env, err := store.CreateEnvironment("staging")
				if err != nil {
					t.Fatalf("failed to create environment: %v", err)
				}
				if env.Name != "staging" {
					t.Errorf("expected name 'staging', got %q", env.Name)
				}
			},
		},
		{
			name: "get environment",
			operation: func(t *testing.T, store *SQLiteStore) {
				store.CreateEnvironment("production")
				env, err := store.GetEnvironment("production")
				if err != nil {
					t.Fatalf("failed to get environment: %v", err)
				}
				if env.Name != "production" {
					t.Errorf("expected name 'production', got %q", env.Name)
				}
			},
		},
		{
			name: "get environment not found",
			operation: func(t *testing.T, store *SQLiteStore) {
				env, err := store.GetEnvironment("nonexistent")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if env != nil {
					t.Error("expected nil for nonexistent environment")
				}
			},
		},
		{
			name: "update environment ref",
			operation: func(t *testing.T, store *SQLiteStore) {
				store.CreateEnvironment("dev")
				err := store.UpdateEnvironmentRef("dev", "abc123")
				if err != nil {
					t.Fatalf("failed to update environment ref: %v", err)
				}
				env, _ := store.GetEnvironment("dev")
				if env.CommitRef != "abc123" {
					t.Errorf("expected commit_ref 'abc123', got %q", env.CommitRef)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()
			tt.operation(t, store)
		})
	}
}

// --- Column lineage tests ---

func TestSQLiteStore_ColumnLineage(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T, store *SQLiteStore)
		verify func(t *testing.T, store *SQLiteStore)
	}{
		{
			name: "save and get model columns",
			setup: func(t *testing.T, store *SQLiteStore) {
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
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				retrieved, err := store.GetModelColumns("staging.stg_customers")
				if err != nil {
					t.Fatalf("failed to get model columns: %v", err)
				}

				if len(retrieved) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(retrieved))
				}

				if retrieved[0].Name != "customer_id" {
					t.Errorf("expected column name 'customer_id', got %q", retrieved[0].Name)
				}
				if len(retrieved[0].Sources) != 1 {
					t.Errorf("expected 1 source for customer_id, got %d", len(retrieved[0].Sources))
				}

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
			},
		},
		{
			name: "save model columns upsert",
			setup: func(t *testing.T, store *SQLiteStore) {
				model := &Model{
					Path:        "staging.stg_orders",
					Name:        "stg_orders",
					ContentHash: "abc123",
				}
				if err := store.RegisterModel(model); err != nil {
					t.Fatalf("failed to register model: %v", err)
				}

				initialColumns := []ColumnInfo{
					{Name: "order_id", Index: 0, Sources: []SourceRef{{Table: "raw_orders", Column: "id"}}},
				}
				if err := store.SaveModelColumns("staging.stg_orders", initialColumns); err != nil {
					t.Fatalf("failed to save initial columns: %v", err)
				}

				updatedColumns := []ColumnInfo{
					{Name: "order_id", Index: 0, Sources: []SourceRef{{Table: "raw_orders", Column: "order_id"}}},
					{Name: "total", Index: 1, TransformType: "EXPR", Function: "sum", Sources: []SourceRef{{Table: "raw_orders", Column: "amount"}}},
				}
				if err := store.SaveModelColumns("staging.stg_orders", updatedColumns); err != nil {
					t.Fatalf("failed to save updated columns: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				retrieved, err := store.GetModelColumns("staging.stg_orders")
				if err != nil {
					t.Fatalf("failed to get model columns: %v", err)
				}

				if len(retrieved) != 2 {
					t.Fatalf("expected 2 columns after update, got %d", len(retrieved))
				}

				if len(retrieved[0].Sources) != 1 || retrieved[0].Sources[0].Column != "order_id" {
					t.Errorf("expected source column 'order_id', got %v", retrieved[0].Sources)
				}
			},
		},
		{
			name:  "get model columns not found",
			setup: func(t *testing.T, store *SQLiteStore) {},
			verify: func(t *testing.T, store *SQLiteStore) {
				columns, err := store.GetModelColumns("nonexistent.model")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(columns) != 0 {
					t.Errorf("expected empty slice for nonexistent model, got %d columns", len(columns))
				}
			},
		},
		{
			name: "delete model columns",
			setup: func(t *testing.T, store *SQLiteStore) {
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

				err := store.DeleteModelColumns("staging.stg_products")
				if err != nil {
					t.Fatalf("failed to delete columns: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				retrieved, err := store.GetModelColumns("staging.stg_products")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(retrieved) != 0 {
					t.Errorf("expected 0 columns after deletion, got %d", len(retrieved))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			if tt.setup != nil {
				tt.setup(t, store)
			}
			if tt.verify != nil {
				tt.verify(t, store)
			}
		})
	}
}

// --- Trace tests ---

func TestSQLiteStore_Trace(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T, store *SQLiteStore)
		verify func(t *testing.T, store *SQLiteStore)
	}{
		{
			name: "trace column backward",
			setup: func(t *testing.T, store *SQLiteStore) {
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

				stgColumns := []ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "raw_customers", Column: "id"}}},
				}
				if err := store.SaveModelColumns("staging.stg_customers", stgColumns); err != nil {
					t.Fatalf("failed to save stg columns: %v", err)
				}

				martColumns := []ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
				}
				if err := store.SaveModelColumns("marts.customer_summary", martColumns); err != nil {
					t.Fatalf("failed to save mart columns: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				results, err := store.TraceColumnBackward("marts.customer_summary", "customer_id")
				if err != nil {
					t.Fatalf("failed to trace column backward: %v", err)
				}

				if len(results) == 0 {
					t.Fatal("expected at least 1 trace result, got 0")
				}

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
			},
		},
		{
			name: "trace column forward",
			setup: func(t *testing.T, store *SQLiteStore) {
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

				stgColumns := []ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "raw_customers", Column: "id"}}},
					{Name: "email", Index: 1, Sources: []SourceRef{{Table: "raw_customers", Column: "email"}}},
				}
				if err := store.SaveModelColumns("staging.stg_customers", stgColumns); err != nil {
					t.Fatalf("failed to save stg columns: %v", err)
				}

				martColumns := []ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
					{Name: "contact", Index: 1, Sources: []SourceRef{{Table: "stg_customers", Column: "email"}}},
				}
				if err := store.SaveModelColumns("marts.customer_summary", martColumns); err != nil {
					t.Fatalf("failed to save mart columns: %v", err)
				}
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				results, err := store.TraceColumnForward("staging.stg_customers", "customer_id")
				if err != nil {
					t.Fatalf("failed to trace column forward: %v", err)
				}

				if len(results) == 0 {
					t.Fatal("expected at least 1 trace result, got 0")
				}

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
			},
		},
		{
			name: "trace column forward multiple consumers",
			setup: func(t *testing.T, store *SQLiteStore) {
				stgModel := &Model{Path: "staging.stg_customers", Name: "stg_customers", ContentHash: "abc"}
				summaryModel := &Model{Path: "marts.customer_summary", Name: "customer_summary", ContentHash: "def"}
				metricsModel := &Model{Path: "marts.customer_metrics", Name: "customer_metrics", ContentHash: "ghi"}

				for _, m := range []*Model{stgModel, summaryModel, metricsModel} {
					if err := store.RegisterModel(m); err != nil {
						t.Fatalf("failed to register model %s: %v", m.Path, err)
					}
				}

				if err := store.SaveModelColumns("staging.stg_customers", []ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []SourceRef{{Table: "raw", Column: "id"}}},
				}); err != nil {
					t.Fatalf("failed to save stg columns: %v", err)
				}

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
			},
			verify: func(t *testing.T, store *SQLiteStore) {
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
			},
		},
		{
			name:  "trace column empty results",
			setup: func(t *testing.T, store *SQLiteStore) {},
			verify: func(t *testing.T, store *SQLiteStore) {
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			if tt.setup != nil {
				tt.setup(t, store)
			}
			if tt.verify != nil {
				tt.verify(t, store)
			}
		})
	}
}
