package commands

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/spf13/cobra"
)

// NewSeedCommand creates the seed command.
func NewSeedCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "seed",
		Short: "Load seed data from CSV files",
		Long: `Load seed data from CSV files in the seeds directory into the database.

Seeds are typically used for reference data like country codes, status enums,
or small lookup tables that don't change frequently.

Output adapts to environment:
  - Terminal: Styled, colored output
  - Piped/Scripted: Markdown format (agent-friendly)
  
Use --output to override: auto, text, markdown, json`,
		Example: `  # Load all seeds (auto-detect output format)
  leapsql seed

  # Load seeds as JSON
  leapsql seed --output json

  # Load seeds from a specific directory
  leapsql seed --seeds-dir ./data/seeds`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSeed(cmd)
		},
	}

	return cmd
}

func runSeed(cmd *cobra.Command) error {
	cfg := getConfig()

	// Create renderer based on output format
	mode := output.Mode(cfg.OutputFormat)
	r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()

	ctx := context.Background()

	effectiveMode := r.EffectiveMode()

	// Get list of seed files before loading
	seedFiles, err := getSeedFiles(cfg.SeedsDir)
	if err != nil {
		return err
	}

	if len(seedFiles) == 0 {
		switch effectiveMode {
		case output.ModeJSON:
			return r.JSON(output.SeedOutput{
				Seeds:   []output.SeedInfo{},
				Summary: output.SeedSummary{TotalSeeds: 0, TotalRows: 0},
			})
		case output.ModeMarkdown:
			r.Println(output.FormatHeader(1, "Seeds"))
			r.Println("")
			r.Println("No seed files found in " + cfg.SeedsDir)
		default:
			r.Header(1, "Seeds")
			r.Muted("No seed files found in " + cfg.SeedsDir)
		}
		return nil
	}

	// Show spinner for TTY mode
	var spinner *output.Spinner
	if effectiveMode == output.ModeText {
		spinner = r.NewSpinner("Loading seeds...")
		spinner.Start()
	}

	if err := eng.LoadSeeds(ctx); err != nil {
		if spinner != nil {
			spinner.Fail("Failed to load seeds")
		}
		return err
	}

	if spinner != nil {
		spinner.Success("Seeds loaded successfully")
	}

	// Output based on mode
	switch effectiveMode {
	case output.ModeJSON:
		return seedJSON(r, cfg.SeedsDir, seedFiles)
	case output.ModeMarkdown:
		return seedMarkdown(r, cfg.SeedsDir, seedFiles)
	default:
		return seedText(r, cfg.SeedsDir, seedFiles)
	}
}

// getSeedFiles returns a list of CSV files in the seeds directory.
func getSeedFiles(seedsDir string) ([]string, error) {
	if seedsDir == "" {
		return nil, nil
	}

	entries, err := os.ReadDir(seedsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		files = append(files, entry.Name())
	}
	return files, nil
}

// seedText outputs seed results in styled text format.
func seedText(r *output.Renderer, seedsDir string, files []string) error {
	r.Println("")
	r.Header(2, "Loaded Seeds")

	for i, file := range files {
		tableName := strings.TrimSuffix(file, ".csv")
		r.StatusLine(tableName, "success", file)
		_ = i // suppress unused variable warning
	}

	r.Println("")
	r.Muted("Source: " + seedsDir)
	return nil
}

// seedMarkdown outputs seed results in markdown format.
func seedMarkdown(r *output.Renderer, seedsDir string, files []string) error {
	r.Println(output.FormatHeader(1, "Seeds Loaded"))
	r.Println("")

	for _, file := range files {
		tableName := strings.TrimSuffix(file, ".csv")
		r.Println(output.FormatKeyValue("Table", tableName))
		r.Println(output.FormatKeyValue("File", file))
		r.Println("")
	}

	r.Println(output.FormatKeyValue("Source Directory", seedsDir))
	r.Printf("**Total Seeds:** %d\n", len(files))
	return nil
}

// seedJSON outputs seed results in JSON format.
func seedJSON(r *output.Renderer, seedsDir string, files []string) error {
	seeds := make([]output.SeedInfo, 0, len(files))
	for _, file := range files {
		tableName := strings.TrimSuffix(file, ".csv")
		absPath, _ := filepath.Abs(filepath.Join(seedsDir, file))
		seeds = append(seeds, output.SeedInfo{
			Name:     tableName,
			FilePath: absPath,
			Rows:     -1, // Unknown without querying the database
		})
	}

	return r.JSON(output.SeedOutput{
		Seeds: seeds,
		Summary: output.SeedSummary{
			TotalSeeds: len(seeds),
			TotalRows:  -1, // Unknown without querying
		},
	})
}
