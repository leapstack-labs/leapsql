package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/adapter"
	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
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
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()

	ctx := context.Background()
	startTime := time.Now()

	verbose := viper.GetBool("verbose")

	// Create renderer
	mode := output.Mode(cfg.OutputFormat)
	if opts.JSONOutput {
		mode = output.ModeJSON
	}
	r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

	// Load seeds
	if verbose && !opts.JSONOutput {
		r.Muted("Loading seeds...")
	}
	if err := eng.LoadSeeds(ctx); err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	// Discover models
	if verbose && !opts.JSONOutput {
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
	// Target holds target configuration (populated from viper if available)
	Target *TargetConfig
}

// TargetConfig holds database target configuration for commands.
type TargetConfig struct {
	Type     string
	Database string
	Host     string
	Port     int
	User     string
	Password string
	Schema   string
	Options  map[string]string
}

func createEngine(cfg *Config) (*engine.Engine, error) {
	// Ensure state directory exists
	stateDir := filepath.Dir(cfg.StatePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0750); err != nil {
			return nil, fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	// Build target info for template rendering
	var targetInfo *starctx.TargetInfo
	var adapterConfig *adapter.Config

	if cfg.Target != nil {
		targetInfo = &starctx.TargetInfo{
			Type:     cfg.Target.Type,
			Schema:   cfg.Target.Schema,
			Database: cfg.Target.Database,
		}
		adapterConfig = &adapter.Config{
			Type:     cfg.Target.Type,
			Path:     cfg.Target.Database,
			Database: cfg.Target.Database,
			Schema:   cfg.Target.Schema,
			Host:     cfg.Target.Host,
			Port:     cfg.Target.Port,
			Username: cfg.Target.User,
			Password: cfg.Target.Password,
			Options:  cfg.Target.Options,
		}
	}

	engineCfg := engine.Config{
		ModelsDir:     cfg.ModelsDir,
		SeedsDir:      cfg.SeedsDir,
		MacrosDir:     cfg.MacrosDir,
		DatabasePath:  cfg.DatabasePath,
		StatePath:     cfg.StatePath,
		Environment:   cfg.Environment,
		Target:        targetInfo,
		AdapterConfig: adapterConfig,
	}

	return engine.New(engineCfg)
}
