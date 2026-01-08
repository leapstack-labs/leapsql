package state

import (
	"context"
	"testing"
	"time"

	"github.com/leapstack-labs/leapsql/internal/testutil"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


func setupTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store := NewSQLiteStore(testutil.NewTestLogger(t))
	require.NoError(t, store.Open(":memory:"))
	require.NoError(t, store.InitSchema())
	return store
}

// newTestModel creates a Model (core.PersistedModel) for testing.
// This helper uses the composition pattern required by core.PersistedModel.
func newTestModel(path, name, materialized, contentHash string) *core.PersistedModel {
	return &core.PersistedModel{
		Model: &core.Model{
			Path:         path,
			Name:         name,
			Materialized: materialized,
		},
		ContentHash: contentHash,
	}
}

// newTestModelFull creates a Model with all core fields for testing.
func newTestModelFull(coreModel *core.Model, contentHash string) *core.PersistedModel {
	return &core.PersistedModel{
		Model:       coreModel,
		ContentHash: contentHash,
	}
}

func TestSQLiteStore_OpenClose(t *testing.T) {
	store := NewSQLiteStore(testutil.NewTestLogger(t))

	require.NoError(t, store.Open(":memory:"))
	require.NoError(t, store.Close())
}

func TestSQLiteStore_InitSchema(t *testing.T) {
	store := NewSQLiteStore(testutil.NewTestLogger(t))
	require.NoError(t, store.Open(":memory:"))
	defer func() { _ = store.Close() }()

	require.NoError(t, store.InitSchema())

	// Verify tables exist by querying them
	tables := []string{"runs", "models", "model_runs", "dependencies", "environments", "model_columns", "column_lineage"}
	for _, table := range tables {
		func(tableName string) {
			rows, err := store.db.QueryContext(context.Background(), "SELECT 1 FROM "+tableName+" LIMIT 1")
			require.NoError(t, err, "table %s should exist", tableName)
			if rows != nil {
				defer func() { _ = rows.Close() }()
				assert.NoError(t, rows.Err())
			}
		}(table)
	}
}

// --- Run lifecycle tests ---

func TestSQLiteStore_RunLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, store *SQLiteStore) *core.Run
		operation func(t *testing.T, store *SQLiteStore, run *core.Run)
		verify    func(t *testing.T, store *SQLiteStore, run *core.Run)
	}{
		{
			name: "create run",
			setup: func(t *testing.T, store *SQLiteStore) *core.Run {
				run, err := store.CreateRun("production")
				require.NoError(t, err)
				return run
			},
			verify: func(t *testing.T, _ *SQLiteStore, run *core.Run) {
				assert.NotEmpty(t, run.ID)
				assert.Equal(t, "production", run.Environment)
				assert.Equal(t, core.RunStatusRunning, run.Status)
			},
		},
		{
			name: "get run",
			setup: func(t *testing.T, store *SQLiteStore) *core.Run {
				run, err := store.CreateRun("staging")
				require.NoError(t, err)
				return run
			},
			operation: func(t *testing.T, store *SQLiteStore, run *core.Run) {
				retrieved, err := store.GetRun(run.ID)
				require.NoError(t, err)
				assert.Equal(t, run.ID, retrieved.ID)
				assert.Equal(t, "staging", retrieved.Environment)
			},
		},
		{
			name: "get run not found",
			setup: func(_ *testing.T, _ *SQLiteStore) *core.Run {
				return nil
			},
			operation: func(t *testing.T, store *SQLiteStore, _ *core.Run) {
				_, err := store.GetRun("nonexistent-id")
				assert.Error(t, err)
			},
		},
		{
			name: "complete run success",
			setup: func(_ *testing.T, store *SQLiteStore) *core.Run {
				run, _ := store.CreateRun("dev")
				return run
			},
			operation: func(t *testing.T, store *SQLiteStore, run *core.Run) {
				require.NoError(t, store.CompleteRun(run.ID, core.RunStatusCompleted, ""))
			},
			verify: func(t *testing.T, store *SQLiteStore, run *core.Run) {
				retrieved, _ := store.GetRun(run.ID)
				assert.Equal(t, core.RunStatusCompleted, retrieved.Status)
				assert.NotNil(t, retrieved.CompletedAt)
			},
		},
		{
			name: "complete run with error",
			setup: func(_ *testing.T, store *SQLiteStore) *core.Run {
				run, _ := store.CreateRun("dev")
				return run
			},
			operation: func(t *testing.T, store *SQLiteStore, run *core.Run) {
				require.NoError(t, store.CompleteRun(run.ID, core.RunStatusFailed, "something went wrong"))
			},
			verify: func(t *testing.T, store *SQLiteStore, run *core.Run) {
				retrieved, _ := store.GetRun(run.ID)
				assert.Equal(t, core.RunStatusFailed, retrieved.Status)
				assert.Equal(t, "something went wrong", retrieved.Error)
			},
		},
		{
			name: "get latest run",
			setup: func(_ *testing.T, store *SQLiteStore) *core.Run {
				_, _ = store.CreateRun("prod")
				time.Sleep(10 * time.Millisecond)
				run2, _ := store.CreateRun("prod")
				return run2
			},
			verify: func(t *testing.T, store *SQLiteStore, run *core.Run) {
				latest, err := store.GetLatestRun("prod")
				require.NoError(t, err)
				assert.Equal(t, run.ID, latest.ID)
			},
		},
		{
			name: "get latest run no runs",
			setup: func(_ *testing.T, _ *SQLiteStore) *core.Run {
				return nil
			},
			verify: func(t *testing.T, store *SQLiteStore, _ *core.Run) {
				latest, err := store.GetLatestRun("nonexistent")
				require.NoError(t, err)
				assert.Nil(t, latest)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

			var run *core.Run
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
		setup     func(t *testing.T, store *SQLiteStore) *core.PersistedModel
		operation func(t *testing.T, store *SQLiteStore, model *core.PersistedModel)
		verify    func(t *testing.T, store *SQLiteStore, model *core.PersistedModel)
	}{
		{
			name: "register model",
			setup: func(t *testing.T, store *SQLiteStore) *core.PersistedModel {
				model := newTestModel("models.staging.stg_users", "stg_users", "table", "abc123")
				require.NoError(t, store.RegisterModel(model))
				return model
			},
			verify: func(t *testing.T, _ *SQLiteStore, model *core.PersistedModel) {
				assert.NotEmpty(t, model.ID)
			},
		},
		{
			name: "register model upsert",
			setup: func(_ *testing.T, store *SQLiteStore) *core.PersistedModel {
				model := newTestModel("models.staging.stg_users", "stg_users", "table", "abc123")
				_ = store.RegisterModel(model)
				return model
			},
			operation: func(t *testing.T, store *SQLiteStore, model *core.PersistedModel) {
				model.ContentHash = "def456"
				require.NoError(t, store.RegisterModel(model))
			},
			verify: func(t *testing.T, store *SQLiteStore, _ *core.PersistedModel) {
				retrieved, _ := store.GetModelByPath("models.staging.stg_users")
				assert.Equal(t, "def456", retrieved.ContentHash)
			},
		},
		{
			name: "get model by ID",
			setup: func(_ *testing.T, store *SQLiteStore) *core.PersistedModel {
				model := newTestModel("models.staging.stg_orders", "stg_orders", "view", "hash123")
				_ = store.RegisterModel(model)
				return model
			},
			verify: func(t *testing.T, store *SQLiteStore, model *core.PersistedModel) {
				retrieved, err := store.GetModelByID(model.ID)
				require.NoError(t, err)
				assert.Equal(t, "stg_orders", retrieved.Name)
			},
		},
		{
			name: "get model by path",
			setup: func(_ *testing.T, store *SQLiteStore) *core.PersistedModel {
				model := newTestModelFull(&core.Model{
					Path:         "models.marts.revenue",
					Name:         "revenue",
					Materialized: "incremental",
					UniqueKey:    "transaction_id",
				}, "xyz789")
				_ = store.RegisterModel(model)
				return model
			},
			verify: func(t *testing.T, store *SQLiteStore, _ *core.PersistedModel) {
				retrieved, err := store.GetModelByPath("models.marts.revenue")
				require.NoError(t, err)
				assert.Equal(t, "incremental", retrieved.Materialized)
				assert.Equal(t, "transaction_id", retrieved.UniqueKey)
			},
		},
		{
			name: "get model by path not found",
			setup: func(_ *testing.T, _ *SQLiteStore) *core.PersistedModel {
				return nil
			},
			verify: func(t *testing.T, store *SQLiteStore, _ *core.PersistedModel) {
				retrieved, err := store.GetModelByPath("nonexistent.model")
				require.NoError(t, err)
				assert.Nil(t, retrieved)
			},
		},
		{
			name: "update model hash",
			setup: func(t *testing.T, store *SQLiteStore) *core.PersistedModel {
				model := newTestModel("models.test", "test", "table", "original")
				require.NoError(t, store.RegisterModel(model))
				return model
			},
			operation: func(t *testing.T, store *SQLiteStore, model *core.PersistedModel) {
				require.NoError(t, store.UpdateModelHash(model.ID, "updated"))
			},
			verify: func(t *testing.T, store *SQLiteStore, model *core.PersistedModel) {
				retrieved, _ := store.GetModelByID(model.ID)
				assert.Equal(t, "updated", retrieved.ContentHash)
			},
		},
		{
			name: "list models",
			setup: func(t *testing.T, store *SQLiteStore) *core.PersistedModel {
				models := []*core.PersistedModel{
					newTestModel("models.a", "a", "table", "1"),
					newTestModel("models.b", "b", "table", "2"),
					newTestModel("models.c", "c", "table", "3"),
				}
				for _, m := range models {
					require.NoError(t, store.RegisterModel(m))
				}
				return nil
			},
			verify: func(t *testing.T, store *SQLiteStore, _ *core.PersistedModel) {
				list, err := store.ListModels()
				require.NoError(t, err)
				assert.Len(t, list, 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

			var model *core.PersistedModel
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
		model  *core.PersistedModel
		verify func(t *testing.T, retrieved *core.PersistedModel)
	}{
		{
			name: "with all frontmatter fields",
			model: newTestModelFull(&core.Model{
				Path:         "models.staging.stg_users",
				Name:         "stg_users",
				Materialized: "incremental",
				UniqueKey:    "user_id",
				Owner:        "data-team",
				Schema:       "analytics",
				Tags:         []string{"pii", "daily"},
				Tests: []core.TestConfig{
					{Unique: []string{"user_id"}},
					{NotNull: []string{"user_id", "email"}},
				},
				Meta: map[string]any{
					"priority": "high",
					"sla":      24,
				},
			}, "abc123"),
			verify: func(t *testing.T, retrieved *core.PersistedModel) {
				assert.Equal(t, "data-team", retrieved.Owner)
				assert.Equal(t, "analytics", retrieved.Schema)
				assert.Equal(t, []string{"pii", "daily"}, retrieved.Tags)
				require.Len(t, retrieved.Tests, 2)
				assert.Equal(t, []string{"user_id"}, retrieved.Tests[0].Unique)
				assert.Equal(t, "high", retrieved.Meta["priority"])
				assert.InEpsilon(t, float64(24), retrieved.Meta["sla"], 0.0001)
			},
		},
		{
			name:  "with empty optional fields",
			model: newTestModel("models.simple", "simple", "table", "hash123"),
			verify: func(t *testing.T, retrieved *core.PersistedModel) {
				assert.Empty(t, retrieved.Owner)
				assert.Empty(t, retrieved.Schema)
				assert.Empty(t, retrieved.Tags)
				assert.Empty(t, retrieved.Tests)
				assert.Empty(t, retrieved.Meta)
			},
		},
		{
			name: "with accepted values test",
			model: newTestModelFull(&core.Model{
				Path:         "models.accepted_values_test",
				Name:         "accepted_values_test",
				Materialized: "table",
				Tests: []core.TestConfig{
					{
						AcceptedValues: &core.AcceptedValuesConfig{
							Column: "status",
							Values: []string{"active", "inactive", "pending"},
						},
					},
				},
			}, "hash"),
			verify: func(t *testing.T, retrieved *core.PersistedModel) {
				require.Len(t, retrieved.Tests, 1)
				require.NotNil(t, retrieved.Tests[0].AcceptedValues)
				assert.Equal(t, "status", retrieved.Tests[0].AcceptedValues.Column)
				assert.Len(t, retrieved.Tests[0].AcceptedValues.Values, 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

			require.NoError(t, store.RegisterModel(tt.model))

			retrieved, err := store.GetModelByPath(tt.model.Path)
			require.NoError(t, err)

			tt.verify(t, retrieved)
		})
	}
}

func TestSQLiteStore_ModelFrontmatter_Update(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Register initial model
	model := newTestModelFull(&core.Model{
		Path:         "models.update_test",
		Name:         "update_test",
		Materialized: "table",
		Owner:        "team-a",
		Tags:         []string{"initial"},
	}, "hash1")
	require.NoError(t, store.RegisterModel(model))

	// Update the model with new frontmatter fields
	model.ContentHash = "hash2"
	model.Owner = "team-b"
	model.Schema = "new_schema"
	model.Tags = []string{"updated", "v2"}
	model.Tests = []core.TestConfig{{NotNull: []string{"id"}}}
	model.Meta = map[string]any{"version": 2}

	require.NoError(t, store.RegisterModel(model))

	retrieved, err := store.GetModelByPath("models.update_test")
	require.NoError(t, err)

	assert.Equal(t, "team-b", retrieved.Owner)
	assert.Equal(t, "new_schema", retrieved.Schema)
	assert.Equal(t, []string{"updated", "v2"}, retrieved.Tags)
	assert.Len(t, retrieved.Tests, 1)
	assert.InEpsilon(t, float64(2), retrieved.Meta["version"], 0.0001)
}

func TestSQLiteStore_GetModelByID_WithFrontmatterFields(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	model := newTestModelFull(&core.Model{
		Path:         "models.get_by_id_test",
		Name:         "get_by_id_test",
		Materialized: "view",
		Owner:        "analytics",
		Schema:       "reporting",
		Tags:         []string{"finance"},
		Meta:         map[string]any{"department": "finance"},
	}, "hash123")

	require.NoError(t, store.RegisterModel(model))

	retrieved, err := store.GetModelByID(model.ID)
	require.NoError(t, err)

	assert.Equal(t, "analytics", retrieved.Owner)
	assert.Equal(t, "reporting", retrieved.Schema)
	assert.Equal(t, []string{"finance"}, retrieved.Tags)
}

func TestSQLiteStore_ListModels_WithFrontmatterFields(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	models := []*core.PersistedModel{
		newTestModelFull(&core.Model{
			Path: "models.list_a", Name: "list_a", Materialized: "table",
			Owner: "team-a", Tags: []string{"tag-a"},
		}, "1"),
		newTestModelFull(&core.Model{
			Path: "models.list_b", Name: "list_b", Materialized: "table",
			Owner: "team-b", Tags: []string{"tag-b"},
		}, "2"),
	}

	for _, m := range models {
		require.NoError(t, store.RegisterModel(m))
	}

	list, err := store.ListModels()
	require.NoError(t, err)
	require.Len(t, list, 2)

	assert.Equal(t, "team-a", list[0].Owner)
	assert.Equal(t, []string{"tag-a"}, list[0].Tags)
	assert.Equal(t, "team-b", list[1].Owner)
}

// --- Model run tests ---

func TestSQLiteStore_ModelRun(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, store *SQLiteStore) (*core.Run, *core.PersistedModel)
		operation func(t *testing.T, store *SQLiteStore, run *core.Run, model *core.PersistedModel) *core.ModelRun
		verify    func(t *testing.T, store *SQLiteStore, run *core.Run, modelRun *core.ModelRun)
	}{
		{
			name: "record model run",
			setup: func(t *testing.T, store *SQLiteStore) (*core.Run, *core.PersistedModel) {
				run, _ := store.CreateRun("test")
				model := newTestModel("models.test", "test", "table", "hash")
				require.NoError(t, store.RegisterModel(model))
				return run, model
			},
			operation: func(t *testing.T, store *SQLiteStore, run *core.Run, model *core.PersistedModel) *core.ModelRun {
				modelRun := &core.ModelRun{
					RunID:   run.ID,
					ModelID: model.ID,
					Status:  core.ModelRunStatusRunning,
				}
				require.NoError(t, store.RecordModelRun(modelRun))
				return modelRun
			},
			verify: func(t *testing.T, _ *SQLiteStore, _ *core.Run, modelRun *core.ModelRun) {
				assert.NotEmpty(t, modelRun.ID)
			},
		},
		{
			name: "update model run",
			setup: func(t *testing.T, store *SQLiteStore) (*core.Run, *core.PersistedModel) {
				run, _ := store.CreateRun("test")
				model := newTestModel("models.test", "test", "table", "hash")
				require.NoError(t, store.RegisterModel(model))
				return run, model
			},
			operation: func(t *testing.T, store *SQLiteStore, run *core.Run, model *core.PersistedModel) *core.ModelRun {
				modelRun := &core.ModelRun{
					RunID:   run.ID,
					ModelID: model.ID,
					Status:  core.ModelRunStatusRunning,
				}
				require.NoError(t, store.RecordModelRun(modelRun))

				time.Sleep(10 * time.Millisecond)

				require.NoError(t, store.UpdateModelRun(modelRun.ID, core.ModelRunStatusSuccess, 100, "", 0, 50))
				return modelRun
			},
			verify: func(t *testing.T, store *SQLiteStore, run *core.Run, _ *core.ModelRun) {
				runs, _ := store.GetModelRunsForRun(run.ID)
				require.Len(t, runs, 1)
				assert.Equal(t, core.ModelRunStatusSuccess, runs[0].Status)
				assert.Equal(t, int64(100), runs[0].RowsAffected)
				assert.Positive(t, runs[0].ExecutionMS)
			},
		},
		{
			name: "get latest model run",
			setup: func(t *testing.T, store *SQLiteStore) (*core.Run, *core.PersistedModel) {
				run1, err := store.CreateRun("test")
				require.NoError(t, err)
				run2, err := store.CreateRun("test")
				require.NoError(t, err)
				model := newTestModel("models.test", "test", "", "hash")
				require.NoError(t, store.RegisterModel(model))

				mr1 := &core.ModelRun{RunID: run1.ID, ModelID: model.ID, Status: core.ModelRunStatusSuccess}
				require.NoError(t, store.RecordModelRun(mr1))

				time.Sleep(10 * time.Millisecond)

				return run2, model
			},
			operation: func(t *testing.T, store *SQLiteStore, run *core.Run, model *core.PersistedModel) *core.ModelRun {
				mr2 := &core.ModelRun{RunID: run.ID, ModelID: model.ID, Status: core.ModelRunStatusRunning}
				require.NoError(t, store.RecordModelRun(mr2))
				return mr2
			},
			verify: func(t *testing.T, store *SQLiteStore, _ *core.Run, modelRun *core.ModelRun) {
				model, _ := store.GetModelByPath("models.test")
				latest, err := store.GetLatestModelRun(model.ID)
				require.NoError(t, err)
				require.NotNil(t, latest)
				assert.Equal(t, modelRun.ID, latest.ID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

			var run *core.Run
			var model *core.PersistedModel
			if tt.setup != nil {
				run, model = tt.setup(t, store)
			}
			var modelRun *core.ModelRun
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
		setup  func(t *testing.T, store *SQLiteStore) []*core.PersistedModel
		verify func(t *testing.T, store *SQLiteStore, models []*core.PersistedModel)
	}{
		{
			name: "set dependencies",
			setup: func(t *testing.T, store *SQLiteStore) []*core.PersistedModel {
				parent1 := newTestModel("models.parent1", "parent1", "", "1")
				parent2 := newTestModel("models.parent2", "parent2", "", "2")
				child := newTestModel("models.child", "child", "", "3")

				_ = store.RegisterModel(parent1)
				_ = store.RegisterModel(parent2)
				_ = store.RegisterModel(child)

				require.NoError(t, store.SetDependencies(child.ID, []string{parent1.ID, parent2.ID}))
				return []*core.PersistedModel{parent1, parent2, child}
			},
			verify: func(t *testing.T, store *SQLiteStore, models []*core.PersistedModel) {
				child := models[2]
				deps, _ := store.GetDependencies(child.ID)
				assert.Len(t, deps, 2)
			},
		},
		{
			name: "replace dependencies",
			setup: func(t *testing.T, store *SQLiteStore) []*core.PersistedModel {
				parent1 := newTestModel("models.p1", "p1", "", "1")
				parent2 := newTestModel("models.p2", "p2", "", "2")
				child := newTestModel("models.c", "c", "", "3")

				_ = store.RegisterModel(parent1)
				_ = store.RegisterModel(parent2)
				_ = store.RegisterModel(child)

				_ = store.SetDependencies(child.ID, []string{parent1.ID})
				require.NoError(t, store.SetDependencies(child.ID, []string{parent2.ID}))
				return []*core.PersistedModel{parent1, parent2, child}
			},
			verify: func(t *testing.T, store *SQLiteStore, models []*core.PersistedModel) {
				parent2 := models[1]
				child := models[2]
				deps, _ := store.GetDependencies(child.ID)
				assert.Len(t, deps, 1)
				assert.Equal(t, parent2.ID, deps[0])
			},
		},
		{
			name: "get dependents",
			setup: func(_ *testing.T, store *SQLiteStore) []*core.PersistedModel {
				parent := newTestModel("models.parent", "parent", "", "1")
				child1 := newTestModel("models.child1", "child1", "", "2")
				child2 := newTestModel("models.child2", "child2", "", "3")

				_ = store.RegisterModel(parent)
				_ = store.RegisterModel(child1)
				_ = store.RegisterModel(child2)

				_ = store.SetDependencies(child1.ID, []string{parent.ID})
				_ = store.SetDependencies(child2.ID, []string{parent.ID})

				return []*core.PersistedModel{parent, child1, child2}
			},
			verify: func(t *testing.T, store *SQLiteStore, models []*core.PersistedModel) {
				parent := models[0]
				dependents, err := store.GetDependents(parent.ID)
				require.NoError(t, err)
				assert.Len(t, dependents, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

			var models []*core.PersistedModel
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
				require.NoError(t, err)
				assert.Equal(t, "staging", env.Name)
			},
		},
		{
			name: "get environment",
			operation: func(t *testing.T, store *SQLiteStore) {
				_, _ = store.CreateEnvironment("production")
				env, err := store.GetEnvironment("production")
				require.NoError(t, err)
				assert.Equal(t, "production", env.Name)
			},
		},
		{
			name: "get environment not found",
			operation: func(t *testing.T, store *SQLiteStore) {
				env, err := store.GetEnvironment("nonexistent")
				require.NoError(t, err)
				assert.Nil(t, env)
			},
		},
		{
			name: "update environment ref",
			operation: func(t *testing.T, store *SQLiteStore) {
				_, _ = store.CreateEnvironment("dev")
				require.NoError(t, store.UpdateEnvironmentRef("dev", "abc123"))
				env, _ := store.GetEnvironment("dev")
				assert.Equal(t, "abc123", env.CommitRef)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()
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
				model := newTestModel("staging.stg_customers", "stg_customers", "", "abc123")
				require.NoError(t, store.RegisterModel(model))

				columns := []core.ColumnInfo{
					{
						Name:          "customer_id",
						Index:         0,
						TransformType: "",
						Function:      "",
						Sources: []core.SourceRef{
							{Table: "raw_customers", Column: "id"},
						},
					},
					{
						Name:          "full_name",
						Index:         1,
						TransformType: "EXPR",
						Function:      "concat",
						Sources: []core.SourceRef{
							{Table: "raw_customers", Column: "first_name"},
							{Table: "raw_customers", Column: "last_name"},
						},
					},
				}

				require.NoError(t, store.SaveModelColumns("staging.stg_customers", columns))
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				retrieved, err := store.GetModelColumns("staging.stg_customers")
				require.NoError(t, err)
				require.Len(t, retrieved, 2)

				assert.Equal(t, "customer_id", retrieved[0].Name)
				assert.Len(t, retrieved[0].Sources, 1)

				assert.Equal(t, "full_name", retrieved[1].Name)
				assert.Equal(t, core.TransformExpression, retrieved[1].TransformType)
				assert.Equal(t, "concat", retrieved[1].Function)
				assert.Len(t, retrieved[1].Sources, 2)
			},
		},
		{
			name: "save model columns upsert",
			setup: func(t *testing.T, store *SQLiteStore) {
				model := newTestModel("staging.stg_orders", "stg_orders", "", "abc123")
				require.NoError(t, store.RegisterModel(model))

				initialColumns := []core.ColumnInfo{
					{Name: "order_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_orders", Column: "id"}}},
				}
				require.NoError(t, store.SaveModelColumns("staging.stg_orders", initialColumns))

				updatedColumns := []core.ColumnInfo{
					{Name: "order_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_orders", Column: "order_id"}}},
					{Name: "total", Index: 1, TransformType: "EXPR", Function: "sum", Sources: []core.SourceRef{{Table: "raw_orders", Column: "amount"}}},
				}
				require.NoError(t, store.SaveModelColumns("staging.stg_orders", updatedColumns))
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				retrieved, err := store.GetModelColumns("staging.stg_orders")
				require.NoError(t, err)
				require.Len(t, retrieved, 2)
				assert.Len(t, retrieved[0].Sources, 1)
				assert.Equal(t, "order_id", retrieved[0].Sources[0].Column)
			},
		},
		{
			name:  "get model columns not found",
			setup: func(_ *testing.T, _ *SQLiteStore) {},
			verify: func(t *testing.T, store *SQLiteStore) {
				columns, err := store.GetModelColumns("nonexistent.model")
				require.NoError(t, err)
				assert.Empty(t, columns)
			},
		},
		{
			name: "delete model columns",
			setup: func(t *testing.T, store *SQLiteStore) {
				model := newTestModel("staging.stg_products", "stg_products", "", "abc123")
				require.NoError(t, store.RegisterModel(model))

				columns := []core.ColumnInfo{
					{Name: "product_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_products", Column: "id"}}},
					{Name: "name", Index: 1, Sources: []core.SourceRef{{Table: "raw_products", Column: "name"}}},
				}
				require.NoError(t, store.SaveModelColumns("staging.stg_products", columns))
				require.NoError(t, store.DeleteModelColumns("staging.stg_products"))
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				retrieved, err := store.GetModelColumns("staging.stg_products")
				require.NoError(t, err)
				assert.Empty(t, retrieved)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

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
				stgModel := newTestModel("staging.stg_customers", "stg_customers", "", "abc")
				martModel := newTestModel("marts.customer_summary", "customer_summary", "", "def")
				require.NoError(t, store.RegisterModel(stgModel))
				require.NoError(t, store.RegisterModel(martModel))

				stgColumns := []core.ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_customers", Column: "id"}}},
				}
				require.NoError(t, store.SaveModelColumns("staging.stg_customers", stgColumns))

				martColumns := []core.ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
				}
				require.NoError(t, store.SaveModelColumns("marts.customer_summary", martColumns))
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				results, err := store.TraceColumnBackward("marts.customer_summary", "customer_id")
				require.NoError(t, err)
				require.NotEmpty(t, results)

				foundStgCustomers := false
				foundRawCustomers := false
				for _, r := range results {
					if r.ModelPath == "stg_customers" && r.ColumnName == "customer_id" && r.Depth == 1 {
						foundStgCustomers = true
						assert.False(t, r.IsExternal, "stg_customers should not be external")
					}
					if r.ModelPath == "raw_customers" && r.ColumnName == "id" && r.Depth == 2 {
						foundRawCustomers = true
						assert.True(t, r.IsExternal, "raw_customers should be external")
					}
				}

				assert.True(t, foundStgCustomers, "should find stg_customers.customer_id at depth 1")
				assert.True(t, foundRawCustomers, "should find raw_customers.id at depth 2")
			},
		},
		{
			name: "trace column forward",
			setup: func(t *testing.T, store *SQLiteStore) {
				stgModel := newTestModel("staging.stg_customers", "stg_customers", "", "abc")
				martModel := newTestModel("marts.customer_summary", "customer_summary", "", "def")
				require.NoError(t, store.RegisterModel(stgModel))
				require.NoError(t, store.RegisterModel(martModel))

				stgColumns := []core.ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_customers", Column: "id"}}},
					{Name: "email", Index: 1, Sources: []core.SourceRef{{Table: "raw_customers", Column: "email"}}},
				}
				require.NoError(t, store.SaveModelColumns("staging.stg_customers", stgColumns))

				martColumns := []core.ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
					{Name: "contact", Index: 1, Sources: []core.SourceRef{{Table: "stg_customers", Column: "email"}}},
				}
				require.NoError(t, store.SaveModelColumns("marts.customer_summary", martColumns))
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				results, err := store.TraceColumnForward("staging.stg_customers", "customer_id")
				require.NoError(t, err)
				require.NotEmpty(t, results)

				found := false
				for _, r := range results {
					if r.ModelPath == "marts.customer_summary" && r.ColumnName == "customer_id" && r.Depth == 1 {
						found = true
						break
					}
				}
				assert.True(t, found, "should find marts.customer_summary.customer_id at depth 1")
			},
		},
		{
			name: "trace column forward multiple consumers",
			setup: func(t *testing.T, store *SQLiteStore) {
				stgModel := newTestModel("staging.stg_customers", "stg_customers", "", "abc")
				summaryModel := newTestModel("marts.customer_summary", "customer_summary", "", "def")
				metricsModel := newTestModel("marts.customer_metrics", "customer_metrics", "", "ghi")

				for _, m := range []*core.PersistedModel{stgModel, summaryModel, metricsModel} {
					require.NoError(t, store.RegisterModel(m))
				}

				require.NoError(t, store.SaveModelColumns("staging.stg_customers", []core.ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "raw", Column: "id"}}},
				}))

				require.NoError(t, store.SaveModelColumns("marts.customer_summary", []core.ColumnInfo{
					{Name: "cust_id", Index: 0, Sources: []core.SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
				}))
				require.NoError(t, store.SaveModelColumns("marts.customer_metrics", []core.ColumnInfo{
					{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
				}))
			},
			verify: func(t *testing.T, store *SQLiteStore) {
				results, err := store.TraceColumnForward("staging.stg_customers", "customer_id")
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(results), 2)

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

				assert.True(t, foundSummary, "should find marts.customer_summary in forward trace")
				assert.True(t, foundMetrics, "should find marts.customer_metrics in forward trace")
			},
		},
		{
			name:  "trace column empty results",
			setup: func(_ *testing.T, _ *SQLiteStore) {},
			verify: func(t *testing.T, store *SQLiteStore) {
				backward, err := store.TraceColumnBackward("nonexistent.model", "col")
				require.NoError(t, err)
				assert.Empty(t, backward)

				forward, err := store.TraceColumnForward("nonexistent.model", "col")
				require.NoError(t, err)
				assert.Empty(t, forward)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer func() { _ = store.Close() }()

			if tt.setup != nil {
				tt.setup(t, store)
			}
			if tt.verify != nil {
				tt.verify(t, store)
			}
		})
	}
}

// --- Batch query tests ---

func TestSQLiteStore_BatchGetAllColumns(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Setup: Register models and save columns
	models := []*core.PersistedModel{
		newTestModel("staging.stg_customers", "stg_customers", "", "abc"),
		newTestModel("staging.stg_orders", "stg_orders", "", "def"),
		newTestModel("marts.customer_summary", "customer_summary", "", "ghi"),
	}
	for _, m := range models {
		require.NoError(t, store.RegisterModel(m))
	}

	// Save columns for each model
	require.NoError(t, store.SaveModelColumns("staging.stg_customers", []core.ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_customers", Column: "id"}}},
		{Name: "name", Index: 1, Sources: []core.SourceRef{{Table: "raw_customers", Column: "name"}}},
	}))

	require.NoError(t, store.SaveModelColumns("staging.stg_orders", []core.ColumnInfo{
		{Name: "order_id", Index: 0, Sources: []core.SourceRef{{Table: "raw_orders", Column: "id"}}},
		{Name: "customer_id", Index: 1, Sources: []core.SourceRef{{Table: "raw_orders", Column: "customer_id"}}},
		{Name: "total", Index: 2, TransformType: "EXPR", Function: "sum", Sources: []core.SourceRef{{Table: "raw_orders", Column: "amount"}}},
	}))

	require.NoError(t, store.SaveModelColumns("marts.customer_summary", []core.ColumnInfo{
		{Name: "customer_id", Index: 0, Sources: []core.SourceRef{{Table: "stg_customers", Column: "customer_id"}}},
		{Name: "order_count", Index: 1, TransformType: "EXPR", Function: "count", Sources: []core.SourceRef{{Table: "stg_orders", Column: "order_id"}}},
	}))

	// Test: BatchGetAllColumns
	allColumns, err := store.BatchGetAllColumns()
	require.NoError(t, err)

	// Verify: Check we got all models
	assert.Len(t, allColumns, 3, "should have columns for 3 models")

	// Verify: Check stg_customers columns
	stgCustCols := allColumns["staging.stg_customers"]
	assert.Len(t, stgCustCols, 2, "stg_customers should have 2 columns")
	assert.Equal(t, "customer_id", stgCustCols[0].Name)
	assert.Len(t, stgCustCols[0].Sources, 1)
	assert.Equal(t, "raw_customers", stgCustCols[0].Sources[0].Table)

	// Verify: Check stg_orders columns
	stgOrdersCols := allColumns["staging.stg_orders"]
	assert.Len(t, stgOrdersCols, 3, "stg_orders should have 3 columns")
	assert.Equal(t, core.TransformExpression, stgOrdersCols[2].TransformType)
	assert.Equal(t, "sum", stgOrdersCols[2].Function)

	// Verify: Check marts.customer_summary columns
	martsCols := allColumns["marts.customer_summary"]
	assert.Len(t, martsCols, 2, "customer_summary should have 2 columns")
}

func TestSQLiteStore_BatchGetAllDependencies(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Setup: Create models with dependencies
	//   stg_customers (no deps)
	//   stg_orders (no deps)
	//   customer_summary (depends on stg_customers, stg_orders)
	//   order_summary (depends on stg_orders)
	models := []*core.PersistedModel{
		newTestModel("staging.stg_customers", "stg_customers", "", "1"),
		newTestModel("staging.stg_orders", "stg_orders", "", "2"),
		newTestModel("marts.customer_summary", "customer_summary", "", "3"),
		newTestModel("marts.order_summary", "order_summary", "", "4"),
	}
	for _, m := range models {
		require.NoError(t, store.RegisterModel(m))
	}

	// Set dependencies
	require.NoError(t, store.SetDependencies(models[2].ID, []string{models[0].ID, models[1].ID})) // customer_summary -> stg_customers, stg_orders
	require.NoError(t, store.SetDependencies(models[3].ID, []string{models[1].ID}))               // order_summary -> stg_orders

	// Test: BatchGetAllDependencies
	allDeps, err := store.BatchGetAllDependencies()
	require.NoError(t, err)

	// Verify: customer_summary has 2 dependencies
	custSummaryDeps := allDeps[models[2].ID]
	assert.Len(t, custSummaryDeps, 2, "customer_summary should have 2 dependencies")

	// Verify: order_summary has 1 dependency
	orderSummaryDeps := allDeps[models[3].ID]
	assert.Len(t, orderSummaryDeps, 1, "order_summary should have 1 dependency")
	assert.Equal(t, models[1].ID, orderSummaryDeps[0])

	// Verify: staging models have no dependencies
	assert.Empty(t, allDeps[models[0].ID], "stg_customers should have no dependencies")
	assert.Empty(t, allDeps[models[1].ID], "stg_orders should have no dependencies")
}

func TestSQLiteStore_BatchGetAllDependents(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Setup: Same as BatchGetAllDependencies test
	models := []*core.PersistedModel{
		newTestModel("staging.stg_customers", "stg_customers", "", "1"),
		newTestModel("staging.stg_orders", "stg_orders", "", "2"),
		newTestModel("marts.customer_summary", "customer_summary", "", "3"),
		newTestModel("marts.order_summary", "order_summary", "", "4"),
	}
	for _, m := range models {
		require.NoError(t, store.RegisterModel(m))
	}

	require.NoError(t, store.SetDependencies(models[2].ID, []string{models[0].ID, models[1].ID}))
	require.NoError(t, store.SetDependencies(models[3].ID, []string{models[1].ID}))

	// Test: BatchGetAllDependents
	allDependents, err := store.BatchGetAllDependents()
	require.NoError(t, err)

	// Verify: stg_customers has 1 dependent (customer_summary)
	stgCustDependents := allDependents[models[0].ID]
	assert.Len(t, stgCustDependents, 1, "stg_customers should have 1 dependent")
	assert.Equal(t, models[2].ID, stgCustDependents[0])

	// Verify: stg_orders has 2 dependents (customer_summary, order_summary)
	stgOrdersDependents := allDependents[models[1].ID]
	assert.Len(t, stgOrdersDependents, 2, "stg_orders should have 2 dependents")

	// Verify: marts models have no dependents
	assert.Empty(t, allDependents[models[2].ID], "customer_summary should have no dependents")
	assert.Empty(t, allDependents[models[3].ID], "order_summary should have no dependents")
}

func TestSQLiteStore_BatchGetAllColumns_Empty(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Test with no data
	allColumns, err := store.BatchGetAllColumns()
	require.NoError(t, err)
	assert.Empty(t, allColumns)
}

func TestSQLiteStore_BatchGetAllDependencies_Empty(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Test with no data
	allDeps, err := store.BatchGetAllDependencies()
	require.NoError(t, err)
	assert.Empty(t, allDeps)
}

func TestSQLiteStore_BatchGetAllDependents_Empty(t *testing.T) {
	store := setupTestStore(t)
	defer func() { _ = store.Close() }()

	// Test with no data
	allDependents, err := store.BatchGetAllDependents()
	require.NoError(t, err)
	assert.Empty(t, allDependents)
}
