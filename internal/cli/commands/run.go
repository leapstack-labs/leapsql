package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
Use --downstream to also run models that depend on the selected models.`,
		Example: `  # Run all models
  leapsql run

  # Run specific models
  leapsql run --select staging.stg_customers,staging.stg_orders

  # Run a model and its downstream dependents
  leapsql run --select staging.stg_customers --downstream

  # Run with JSON output for CI/CD integration
  leapsql run --json`,
		Aliases: []string{"build"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRun(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Select, "select", "s", "", "Comma-separated list of models to run")
	cmd.Flags().BoolVar(&opts.Downstream, "downstream", false, "Include downstream dependents when using --select")
	cmd.Flags().BoolVar(&opts.JSONOutput, "json", false, "Output as JSON lines for progress tracking")

	return cmd
}

func runRun(cmd *cobra.Command, opts *RunOptions) error {
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	ctx := context.Background()
	startTime := time.Now()

	verbose := viper.GetBool("verbose")

	// Load seeds
	if verbose && !opts.JSONOutput {
		fmt.Println("Loading seeds...")
	}
	if err := eng.LoadSeeds(ctx); err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	// Discover models
	if verbose && !opts.JSONOutput {
		fmt.Println("Discovering models...")
	}
	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	if opts.JSONOutput {
		return runWithJSON(eng, cfg.Environment, opts.Select, opts.Downstream)
	}
	return runWithText(eng, cfg.Environment, opts.Select, opts.Downstream, startTime)
}

// runWithText executes models with text output.
func runWithText(eng *engine.Engine, envName string, selectModels string, downstream bool, startTime time.Time) error {
	ctx := context.Background()
	models := eng.GetModels()
	fmt.Printf("Found %d models\n", len(models))

	// Run models
	if selectModels != "" {
		// Run selected models
		selected := strings.Split(selectModels, ",")
		for i := range selected {
			selected[i] = strings.TrimSpace(selected[i])
		}
		downstreamStr := ""
		if downstream {
			downstreamStr = " (+ downstream)"
		}
		fmt.Printf("Running %d selected models%s...\n", len(selected), downstreamStr)
		result, err := eng.RunSelected(ctx, envName, selected, downstream)
		if err != nil {
			return fmt.Errorf("run failed: %w", err)
		}
		fmt.Printf("Run %s: %s\n", result.ID, result.Status)
		if result.Error != "" {
			fmt.Printf("Error: %s\n", result.Error)
		}
	} else {
		// Run all models
		fmt.Println("Running all models...")
		result, err := eng.Run(ctx, envName)
		if err != nil {
			return fmt.Errorf("run failed: %w", err)
		}
		fmt.Printf("Run %s: %s\n", result.ID, result.Status)
		if result.Error != "" {
			fmt.Printf("Error: %s\n", result.Error)
		}
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Completed in %s\n", elapsed.Round(time.Millisecond))

	return nil
}

// runWithJSON executes models with JSON lines output.
func runWithJSON(eng *engine.Engine, envName string, selectModels string, downstream bool) error {
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
	emitRunEvent(output.RunEvent{
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
				emitRunEvent(output.RunEvent{
					Event: "model_start",
					RunID: runID,
					Model: modelPath,
				})

				status := string(mr.Status)
				if mr.Status == state.ModelRunStatusSuccess {
					successful++
				} else if mr.Status == state.ModelRunStatusFailed {
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
				emitRunEvent(event)
			}
		}
	}

	// Emit run_complete event
	totalMS := int64(time.Since(runStartTime).Milliseconds())
	runStatus := "completed"
	if runErr != nil || (result != nil && result.Status == state.RunStatusFailed) {
		runStatus = "failed"
	}

	emitRunEvent(output.RunEvent{
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
func emitRunEvent(event output.RunEvent) {
	event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(event)
	fmt.Println(string(data))
}

// Helper functions shared across commands

func getConfig() *Config {
	return &Config{
		ModelsDir:    viper.GetString("models_dir"),
		SeedsDir:     viper.GetString("seeds_dir"),
		MacrosDir:    viper.GetString("macros_dir"),
		DatabasePath: viper.GetString("database"),
		StatePath:    viper.GetString("state_path"),
		Environment:  viper.GetString("environment"),
		Verbose:      viper.GetBool("verbose"),
		OutputFormat: viper.GetString("output"),
	}
}

// Config mirrors the CLI config for use in commands.
type Config struct {
	ModelsDir    string
	SeedsDir     string
	MacrosDir    string
	DatabasePath string
	StatePath    string
	Environment  string
	Verbose      bool
	OutputFormat string
}

func createEngine(cfg *Config) (*engine.Engine, error) {
	// Ensure state directory exists
	stateDir := filepath.Dir(cfg.StatePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	engineCfg := engine.Config{
		ModelsDir:    cfg.ModelsDir,
		SeedsDir:     cfg.SeedsDir,
		MacrosDir:    cfg.MacrosDir,
		DatabasePath: cfg.DatabasePath,
		StatePath:    cfg.StatePath,
	}

	return engine.New(engineCfg)
}
