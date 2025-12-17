package engine

// run.go - Execution orchestration for running models

import (
	"context"
	"fmt"
	"time"

	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/internal/state"
)

// Run executes all models in topological order.
func (e *Engine) Run(ctx context.Context, env string) (*state.Run, error) {
	e.logger.Info("starting run", "environment", env)

	// Ensure database is connected before execution
	if err := e.ensureDBConnected(ctx); err != nil {
		return nil, err
	}

	// Create a new run
	run, err := e.store.CreateRun(env)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	e.logger.Debug("created run", "run_id", run.ID)

	// Get topological order
	sorted, err := e.graph.TopologicalSort()
	if err != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, fmt.Sprintf("failed to sort: %v", err))
		return run, err
	}

	e.logger.Debug("executing models", "count", len(sorted))

	// Execute each model
	var runErr error
	for _, node := range sorted {
		m := node.Data.(*parser.ModelConfig)

		// Get model from state store
		model, err := e.store.GetModelByPath(m.Path)
		if err != nil {
			runErr = fmt.Errorf("failed to get model %s: %w", m.Path, err)
			break
		}

		// Record model run start
		modelRun := &state.ModelRun{
			RunID:   run.ID,
			ModelID: model.ID,
			Status:  state.ModelRunStatusRunning,
		}
		if err := e.store.RecordModelRun(modelRun); err != nil {
			runErr = fmt.Errorf("failed to record model run: %w", err)
			break
		}

		// Execute the model
		startTime := time.Now()
		rowsAffected, execErr := e.executeModel(ctx, m, model)
		executionMS := int64(time.Since(startTime).Milliseconds())

		// Update model run status
		if execErr != nil {
			e.logger.Debug("model execution failed", "model", m.Path, "error", execErr.Error())
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusFailed, 0, execErr.Error())
			runErr = execErr
		} else {
			e.logger.Debug("model executed", "model", m.Path, "rows_affected", rowsAffected, "duration_ms", executionMS)
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusSuccess, rowsAffected, "")
		}

		_ = executionMS // Note: execution time tracked but not stored in current schema

		if runErr != nil {
			break
		}
	}

	// Complete the run
	if runErr != nil {
		e.logger.Info("run failed", "run_id", run.ID, "error", runErr.Error())
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, runErr.Error())
	} else {
		e.logger.Info("run completed", "run_id", run.ID)
		_ = e.store.CompleteRun(run.ID, state.RunStatusCompleted, "")
	}

	// Refresh run from store
	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// RunSelected executes only the specified models and their downstream dependents.
// Upstream dependencies must already exist in the database.
func (e *Engine) RunSelected(ctx context.Context, env string, modelPaths []string, includeDownstream bool) (*state.Run, error) {
	// Ensure database is connected before execution
	if err := e.ensureDBConnected(ctx); err != nil {
		return nil, err
	}

	var affected []string
	if includeDownstream {
		// Get affected nodes (selected + downstream)
		affected = e.graph.GetAffectedNodes(modelPaths)
	} else {
		// Only run the selected models
		affected = modelPaths
	}

	// Create subgraph with affected nodes
	subgraph := e.graph.Subgraph(affected)

	// Create a new run
	run, err := e.store.CreateRun(env)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	// Get topological order of subgraph
	sorted, err := subgraph.TopologicalSort()
	if err != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, fmt.Sprintf("failed to sort: %v", err))
		return run, err
	}

	var runErr error
	for _, node := range sorted {
		m := e.models[node.ID]
		if m == nil {
			continue
		}

		model, err := e.store.GetModelByPath(m.Path)
		if err != nil {
			runErr = fmt.Errorf("failed to get model %s: %w", m.Path, err)
			break
		}

		modelRun := &state.ModelRun{
			RunID:   run.ID,
			ModelID: model.ID,
			Status:  state.ModelRunStatusRunning,
		}
		if err := e.store.RecordModelRun(modelRun); err != nil {
			runErr = fmt.Errorf("failed to record model run: %w", err)
			break
		}

		startTime := time.Now()
		rowsAffected, execErr := e.executeModel(ctx, m, model)
		_ = int64(time.Since(startTime).Milliseconds())

		if execErr != nil {
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusFailed, 0, execErr.Error())
			runErr = execErr
		} else {
			_ = e.store.UpdateModelRun(modelRun.ID, state.ModelRunStatusSuccess, rowsAffected, "")
		}

		if runErr != nil {
			break
		}
	}

	if runErr != nil {
		_ = e.store.CompleteRun(run.ID, state.RunStatusFailed, runErr.Error())
	} else {
		_ = e.store.CompleteRun(run.ID, state.RunStatusCompleted, "")
	}

	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// executeModel executes a single model and returns rows affected.
func (e *Engine) executeModel(ctx context.Context, m *parser.ModelConfig, model *state.Model) (int64, error) {
	e.logger.Debug("executing model", "model_path", m.Path, "materialization", m.Materialized)

	sql := e.buildSQL(m, model)

	switch m.Materialized {
	case "table":
		return e.executeTable(ctx, m.Path, sql)
	case "view":
		return e.executeView(ctx, m.Path, sql)
	case "incremental":
		return e.executeIncremental(ctx, m, model, sql)
	default:
		return 0, fmt.Errorf("unknown materialization: %s", m.Materialized)
	}
}
