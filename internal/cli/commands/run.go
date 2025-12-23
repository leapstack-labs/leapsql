package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/spf13/cobra"
)

// RunOptions holds options for the run command.
type RunOptions struct {
	Select     string
	Downstream bool
	JSONOutput bool
}

// NewRunCommand creates the run command.
func NewRunCommand() *cobra.Command {
	opts := &RunOptions{}

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run all models or specific models",
		Long: `Execute SQL models in dependency order.

By default, runs all discovered models. Use --select to run specific models.
Use --downstream to also run models that depend on the selected models.

Output adapts to environment:
  - Terminal: Animated progress with spinner
  - Piped/Scripted: Static progress messages`,
		Example: `  # Run all models
  leapsql run

  # Run specific models
  leapsql run --select staging.stg_customers,staging.stg_orders

  # Run a model and its downstream dependents
  leapsql run --select staging.stg_customers --downstream

  # Run with JSON output for CI/CD integration
  leapsql run --json`,
		Aliases: []string{"build"},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRun(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Select, "select", "s", "", "Comma-separated list of models to run")
	cmd.Flags().BoolVar(&opts.Downstream, "downstream", false, "Include downstream dependents when using --select")
	cmd.Flags().BoolVar(&opts.JSONOutput, "json", false, "Output as JSON lines for progress tracking")

	return cmd
}

func runRun(cmd *cobra.Command, opts *RunOptions) error {
	cmdCtx, cleanup, err := NewCommandContext(cmd)
	if err != nil {
		return err
	}
	defer cleanup()

	ctx := context.Background()
	startTime := time.Now()

	cfg := cmdCtx.Cfg
	eng := cmdCtx.Engine
	r := cmdCtx.Renderer

	// Override output mode if JSON flag is set
	if opts.JSONOutput {
		r = output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), output.ModeJSON)
	}

	// Load seeds
	if cfg.Verbose && !opts.JSONOutput {
		r.Muted("Loading seeds...")
	}
	if err := eng.LoadSeeds(ctx); err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	// Discover models
	if cfg.Verbose && !opts.JSONOutput {
		r.Muted("Discovering models...")
	}
	if _, err := eng.Discover(engine.DiscoveryOptions{}); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	if opts.JSONOutput {
		return runWithJSON(eng, r, cfg.Environment, opts.Select, opts.Downstream)
	}
	return runWithRenderer(eng, r, cfg.Environment, opts.Select, opts.Downstream, startTime)
}

// runWithRenderer executes models with adaptive output.
func runWithRenderer(eng *engine.Engine, r *output.Renderer, envName string, selectModels string, downstream bool, startTime time.Time) error {
	ctx := context.Background()
	models := eng.GetModels()

	effectiveMode := r.EffectiveMode()

	if effectiveMode == output.ModeMarkdown {
		r.Println(output.FormatHeader(1, "Run Results"))
		r.Println("")
	} else {
		r.Printf("Found %d models\n", len(models))
	}

	// Determine models to run
	var modelsToRun []string
	if selectModels != "" {
		selected := strings.Split(selectModels, ",")
		for i := range selected {
			selected[i] = strings.TrimSpace(selected[i])
		}
		modelsToRun = selected
	}

	// Create progress tracker for TTY mode
	var progress *output.Progress
	if r.IsTTY() && effectiveMode == output.ModeText {
		total := len(models)
		if selectModels != "" {
			total = len(modelsToRun)
		}
		progress = r.NewProgress(total, "Running models")
		progress.Start()
	}

	// Run models
	var result *state.Run
	var runErr error
	if selectModels != "" {
		downstreamStr := ""
		if downstream {
			downstreamStr = " (+ downstream)"
		}
		if effectiveMode == output.ModeMarkdown {
			r.Println(output.FormatKeyValue("Selected", fmt.Sprintf("%d models%s", len(modelsToRun), downstreamStr)))
		} else if progress == nil {
			r.Printf("Running %d selected models%s...\n", len(modelsToRun), downstreamStr)
		}
		result, runErr = eng.RunSelected(ctx, envName, modelsToRun, downstream)
	} else {
		if effectiveMode == output.ModeMarkdown {
			r.Println(output.FormatKeyValue("Mode", "All models"))
		} else if progress == nil {
			r.Println("Running all models...")
		}
		result, runErr = eng.Run(ctx, envName)
	}

	// Complete progress
	if progress != nil {
		if runErr != nil || (result != nil && result.Status == state.RunStatusFailed) {
			progress.Fail("Run failed")
		} else {
			progress.Complete("Run completed")
		}
	}

	// Output results
	if result != nil {
		if effectiveMode == output.ModeMarkdown {
			r.Println("")
			r.Println(output.FormatKeyValue("Run ID", result.ID))
			r.Println(output.FormatKeyValue("Status", string(result.Status)))
			if result.Error != "" {
				r.Println(output.FormatKeyValue("Error", result.Error))
			}
		} else {
			r.Printf("Run %s: %s\n", result.ID, result.Status)
			if result.Error != "" {
				r.Error(result.Error)
			}
		}
	}

	elapsed := time.Since(startTime)
	if effectiveMode == output.ModeMarkdown {
		r.Println(output.FormatKeyValue("Duration", elapsed.Round(time.Millisecond).String()))
	} else {
		r.Muted(fmt.Sprintf("Completed in %s", elapsed.Round(time.Millisecond)))
	}

	return runErr
}

// runWithJSON executes models with JSON lines output.
func runWithJSON(eng *engine.Engine, r *output.Renderer, envName string, selectModels string, downstream bool) error {
	ctx := context.Background()
	graph := eng.GetGraph()
	store := eng.GetStateStore()

	// Determine which models to run
	var modelPaths []string
	if selectModels != "" {
		selected := strings.Split(selectModels, ",")
		for i := range selected {
			selected[i] = strings.TrimSpace(selected[i])
		}
		if downstream {
			modelPaths = graph.GetAffectedNodes(selected)
		} else {
			modelPaths = selected
		}
	} else {
		sorted, err := graph.TopologicalSort()
		if err != nil {
			return fmt.Errorf("failed to sort models: %w", err)
		}
		for _, node := range sorted {
			modelPaths = append(modelPaths, node.ID)
		}
	}

	// Generate run ID
	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())
	runStartTime := time.Now()

	// Emit run_start event
	emitRunEvent(r, output.RunEvent{
		Event:  "run_start",
		RunID:  runID,
		Models: modelPaths,
	})

	// Execute the run
	var result *state.Run
	var runErr error
	if selectModels != "" {
		selected := strings.Split(selectModels, ",")
		for i := range selected {
			selected[i] = strings.TrimSpace(selected[i])
		}
		result, runErr = eng.RunSelected(ctx, envName, selected, downstream)
	} else {
		result, runErr = eng.Run(ctx, envName)
	}

	// Get model run details from state store
	var successful, failed int
	if result != nil && store != nil {
		modelRuns, err := store.GetModelRunsForRun(result.ID)
		if err == nil {
			for _, mr := range modelRuns {
				// Get model path
				model, err := store.GetModelByID(mr.ModelID)
				modelPath := mr.ModelID
				filePath := ""
				if err == nil && model != nil {
					modelPath = model.Path
				}

				// Emit model events
				emitRunEvent(r, output.RunEvent{
					Event: "model_start",
					RunID: runID,
					Model: modelPath,
				})

				status := string(mr.Status)
				switch mr.Status {
				case state.ModelRunStatusSuccess:
					successful++
				case state.ModelRunStatusFailed:
					failed++
				}

				event := output.RunEvent{
					Event:        "model_complete",
					RunID:        runID,
					Model:        modelPath,
					Status:       status,
					RowsAffected: mr.RowsAffected,
					ExecutionMS:  mr.ExecutionMS,
				}
				if mr.Error != "" {
					event.Error = mr.Error
					event.File = filePath
				}
				emitRunEvent(r, event)
			}
		}
	}

	// Emit run_complete event
	totalMS := int64(time.Since(runStartTime).Milliseconds())
	runStatus := "completed"
	if runErr != nil || (result != nil && result.Status == state.RunStatusFailed) {
		runStatus = "failed"
	}

	emitRunEvent(r, output.RunEvent{
		Event:       "run_complete",
		RunID:       runID,
		Status:      runStatus,
		TotalModels: len(modelPaths),
		Successful:  successful,
		Failed:      failed,
		TotalMS:     totalMS,
	})

	return runErr
}

// emitRunEvent outputs a run event as a JSON line.
func emitRunEvent(r *output.Renderer, event output.RunEvent) {
	event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(event)
	r.Println(string(data))
}
