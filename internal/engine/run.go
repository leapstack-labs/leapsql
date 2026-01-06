package engine

// run.go - Execution orchestration for running models

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// preparedModel holds a model ready for execution after successful render.
type preparedModel struct {
	model     *core.Model
	persisted *core.PersistedModel
	modelRun  *core.ModelRun
	sql       string
	renderMS  int64
}

// Run executes all models in topological order using a two-phase approach:
// Phase 1: Validate all templates (fail fast if any fail)
// Phase 2: Execute all models
func (e *Engine) Run(ctx context.Context, env string) (*core.Run, error) {
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
		_ = e.store.CompleteRun(run.ID, core.RunStatusFailed, fmt.Sprintf("dependency sort failed: %v", err))
		return run, err
	}

	e.logger.Debug("validating models", "count", len(sorted))

	// Phase 1: Validate all templates
	prepared, renderErrors := e.validateAndPrepareModels(run.ID, sorted)

	if len(renderErrors) > 0 {
		// Mark prepared models as skipped
		for _, p := range prepared {
			_ = e.store.UpdateModelRun(p.modelRun.ID, core.ModelRunStatusSkipped, 0,
				"run aborted: other models failed to render", p.renderMS, 0)
		}

		errMsg := fmt.Sprintf("%d model(s) failed to render", len(renderErrors))
		_ = e.store.CompleteRun(run.ID, core.RunStatusFailed, errMsg)

		e.logger.Error("run failed during validation", "run_id", run.ID, "render_errors", len(renderErrors))
		run, _ = e.store.GetRun(run.ID)
		return run, errors.Join(renderErrors...)
	}

	e.logger.Debug("executing models", "count", len(prepared))

	// Phase 2: Execute all models
	runErr := e.executeModels(ctx, run.ID, prepared)

	// Complete run
	if runErr != nil {
		e.logger.Info("run failed", "run_id", run.ID, "error", runErr.Error())
		_ = e.store.CompleteRun(run.ID, core.RunStatusFailed, runErr.Error())
	} else {
		e.logger.Info("run completed", "run_id", run.ID)
		_ = e.store.CompleteRun(run.ID, core.RunStatusCompleted, "")
		_ = e.store.DeleteOldSnapshots(5)
	}

	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// RunSelected executes only the specified models and their downstream dependents.
// Uses a two-phase approach: validate all templates, then execute.
// Upstream dependencies must already exist in the database.
func (e *Engine) RunSelected(ctx context.Context, env string, modelPaths []string, includeDownstream bool) (*core.Run, error) {
	e.logger.Info("starting selected run", "environment", env, "models", modelPaths, "include_downstream", includeDownstream)

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

	e.logger.Debug("created run", "run_id", run.ID)

	// Get topological order of subgraph
	sorted, err := subgraph.TopologicalSort()
	if err != nil {
		_ = e.store.CompleteRun(run.ID, core.RunStatusFailed, fmt.Sprintf("dependency sort failed: %v", err))
		return run, err
	}

	e.logger.Debug("validating models", "count", len(sorted))

	// Phase 1: Validate all templates
	prepared, renderErrors := e.validateAndPrepareModels(run.ID, sorted)

	if len(renderErrors) > 0 {
		// Mark prepared models as skipped
		for _, p := range prepared {
			_ = e.store.UpdateModelRun(p.modelRun.ID, core.ModelRunStatusSkipped, 0,
				"run aborted: other models failed to render", p.renderMS, 0)
		}

		errMsg := fmt.Sprintf("%d model(s) failed to render", len(renderErrors))
		_ = e.store.CompleteRun(run.ID, core.RunStatusFailed, errMsg)

		e.logger.Error("run failed during validation", "run_id", run.ID, "render_errors", len(renderErrors))
		run, _ = e.store.GetRun(run.ID)
		return run, errors.Join(renderErrors...)
	}

	e.logger.Debug("executing models", "count", len(prepared))

	// Phase 2: Execute all models
	runErr := e.executeModels(ctx, run.ID, prepared)

	// Complete run
	if runErr != nil {
		e.logger.Info("run failed", "run_id", run.ID, "error", runErr.Error())
		_ = e.store.CompleteRun(run.ID, core.RunStatusFailed, runErr.Error())
	} else {
		e.logger.Info("run completed", "run_id", run.ID)
		_ = e.store.CompleteRun(run.ID, core.RunStatusCompleted, "")
		_ = e.store.DeleteOldSnapshots(5)
	}

	run, _ = e.store.GetRun(run.ID)
	return run, runErr
}

// validateAndPrepareModels renders all model templates and records ModelRuns.
// Returns prepared models and any render errors encountered.
func (e *Engine) validateAndPrepareModels(runID string, sorted []*dag.Node) ([]preparedModel, []error) {
	var prepared []preparedModel
	var renderErrors []error

	for _, node := range sorted {
		m := node.Data.(*core.Model)

		persisted, err := e.store.GetModelByPath(m.Path)
		if err != nil || persisted == nil {
			// Model not in store - record as failed
			errMsg := fmt.Sprintf("model not found in store: %v", err)
			modelRun := &core.ModelRun{
				RunID:   runID,
				ModelID: m.Path, // Use path as fallback ID
				Status:  core.ModelRunStatusFailed,
				Error:   errMsg,
			}
			_ = e.store.RecordModelRun(modelRun)
			renderErrors = append(renderErrors, fmt.Errorf("%s: not found in store", m.Path))
			continue
		}

		// Create pending ModelRun
		modelRun := &core.ModelRun{
			RunID:   runID,
			ModelID: persisted.ID,
			Status:  core.ModelRunStatusPending,
		}
		if err := e.store.RecordModelRun(modelRun); err != nil {
			renderErrors = append(renderErrors, fmt.Errorf("%s: failed to record model run: %w", m.Path, err))
			continue
		}

		// Render template with timing
		start := time.Now()
		sql, err := e.buildSQL(m, persisted)
		renderMS := time.Since(start).Milliseconds()

		if err != nil {
			_ = e.store.UpdateModelRun(modelRun.ID, core.ModelRunStatusFailed, 0, err.Error(), renderMS, 0)
			renderErrors = append(renderErrors, err)
			continue
		}

		e.logger.Debug("model template rendered", "model", m.Path, "render_ms", renderMS)

		prepared = append(prepared, preparedModel{
			model:     m,
			persisted: persisted,
			modelRun:  modelRun,
			sql:       sql,
			renderMS:  renderMS,
		})
	}

	return prepared, renderErrors
}

// executeModels executes all prepared models in order.
func (e *Engine) executeModels(ctx context.Context, runID string, prepared []preparedModel) error {
	for i, p := range prepared {
		// Update to running
		_ = e.store.UpdateModelRun(p.modelRun.ID, core.ModelRunStatusRunning, 0, "", p.renderMS, 0)

		// Execute
		start := time.Now()
		rowsAffected, err := e.executeModelWithSQL(ctx, p.model, p.persisted, p.sql)
		executionMS := time.Since(start).Milliseconds()

		if err != nil {
			e.logger.Debug("model execution failed", "model", p.model.Path, "error", err)
			_ = e.store.UpdateModelRun(p.modelRun.ID, core.ModelRunStatusFailed, 0, err.Error(), p.renderMS, executionMS)

			// Mark remaining models as skipped
			for j := i + 1; j < len(prepared); j++ {
				_ = e.store.UpdateModelRun(prepared[j].modelRun.ID, core.ModelRunStatusSkipped, 0,
					fmt.Sprintf("skipped: upstream model %s failed", p.model.Path), prepared[j].renderMS, 0)
			}

			return err
		}

		e.logger.Debug("model executed", "model", p.model.Path, "rows", rowsAffected, "exec_ms", executionMS)
		_ = e.store.UpdateModelRun(p.modelRun.ID, core.ModelRunStatusSuccess, rowsAffected, "", p.renderMS, executionMS)
		e.saveModelSnapshot(runID, p.model, p.persisted)
	}

	return nil
}

// executeModelWithSQL executes a model with pre-rendered SQL.
func (e *Engine) executeModelWithSQL(ctx context.Context, m *core.Model, model *core.PersistedModel, sql string) (int64, error) {
	e.logger.Debug("executing model", "model_path", m.Path, "materialization", m.Materialized)

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

// saveModelSnapshot saves column snapshots for models that use SELECT *.
// This enables schema drift detection (PL05) by storing the current column
// state of source tables after a successful model run.
func (e *Engine) saveModelSnapshot(runID string, m *core.Model, model *core.PersistedModel) {
	if !model.UsesSelectStar {
		return
	}

	for _, source := range m.Sources {
		cols, err := e.store.GetModelColumns(source)
		if err != nil || len(cols) == 0 {
			continue
		}

		colNames := make([]string, len(cols))
		for i, c := range cols {
			colNames[i] = c.Name
		}

		_ = e.store.SaveColumnSnapshot(runID, m.Path, source, colNames)
	}
}
