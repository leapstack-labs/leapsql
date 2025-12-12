// Package main provides the CLI for LeapSQL data transformation engine.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/docs"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/lsp"
	"github.com/leapstack-labs/leapsql/internal/macro"
	"github.com/leapstack-labs/leapsql/internal/state"
)

const (
	defaultModelsDir = "models"
	defaultSeedsDir  = "seeds"
	defaultMacrosDir = "macros"
	defaultStateFile = ".leapsql/state.db"
)

// Command represents a CLI command.
type Command struct {
	Name        string
	Description string
	Run         func(args []string) error
}

var (
	// Global flags
	modelsDir    string
	seedsDir     string
	macrosDir    string
	databasePath string
	statePath    string
	env          string
	verbose      bool
)

func main() {
	commands := map[string]*Command{
		"run": {
			Name:        "run",
			Description: "Run all models or specific models",
			Run:         runCmd,
		},
		"build": {
			Name:        "build",
			Description: "Build models (alias for run)",
			Run:         runCmd,
		},
		"list": {
			Name:        "list",
			Description: "List all models and their dependencies",
			Run:         listCmd,
		},
		"seed": {
			Name:        "seed",
			Description: "Load seed data from CSV files",
			Run:         seedCmd,
		},
		"dag": {
			Name:        "dag",
			Description: "Show the dependency graph",
			Run:         dagCmd,
		},
		"docs": {
			Name:        "docs",
			Description: "Generate and serve documentation site",
			Run:         docsCmd,
		},
		"version": {
			Name:        "version",
			Description: "Show version information",
			Run:         versionCmd,
		},
		"lsp": {
			Name:        "lsp",
			Description: "Start the Language Server Protocol server",
			Run:         lspCmd,
		},
		"discover": {
			Name:        "discover",
			Description: "Index macros and models for IDE features",
			Run:         discoverCmd,
		},
	}

	if len(os.Args) < 2 {
		printUsage(commands)
		os.Exit(0)
	}

	cmdName := os.Args[1]

	// Handle help
	if cmdName == "help" || cmdName == "-h" || cmdName == "--help" {
		printUsage(commands)
		os.Exit(0)
	}

	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmdName)
		printUsage(commands)
		os.Exit(1)
	}

	if err := cmd.Run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage(commands map[string]*Command) {
	fmt.Println("LeapSQL - Data Transformation Engine")
	fmt.Println()
	fmt.Println("Usage: leapsql <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	for _, cmd := range []string{"run", "build", "list", "seed", "dag", "docs", "discover", "lsp", "version"} {
		if c, ok := commands[cmd]; ok {
			fmt.Printf("  %-12s %s\n", c.Name, c.Description)
		}
	}
	fmt.Println()
	fmt.Println("Run 'leapsql <command> -h' for help on a specific command.")
}

func setupFlags(fs *flag.FlagSet) {
	fs.StringVar(&modelsDir, "models", defaultModelsDir, "Path to models directory")
	fs.StringVar(&seedsDir, "seeds", defaultSeedsDir, "Path to seeds directory")
	fs.StringVar(&macrosDir, "macros", defaultMacrosDir, "Path to macros directory")
	fs.StringVar(&databasePath, "database", "", "Path to DuckDB database (empty for in-memory)")
	fs.StringVar(&statePath, "state", defaultStateFile, "Path to state database")
	fs.StringVar(&env, "env", "dev", "Environment name")
	fs.BoolVar(&verbose, "v", false, "Verbose output")
}

func createEngine() (*engine.Engine, error) {
	// Ensure state directory exists
	stateDir := filepath.Dir(statePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	cfg := engine.Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		MacrosDir:    macrosDir,
		DatabasePath: databasePath,
		StatePath:    statePath,
	}

	return engine.New(cfg)
}

// runCmd executes the run/build command.
func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	setupFlags(fs)
	select_ := fs.String("select", "", "Comma-separated list of models to run")
	downstream := fs.Bool("downstream", false, "Include downstream dependents when using -select")
	fs.Parse(args)

	eng, err := createEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	ctx := context.Background()
	startTime := time.Now()

	// Load seeds
	if verbose {
		fmt.Println("Loading seeds...")
	}
	if err := eng.LoadSeeds(ctx); err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	// Discover models
	if verbose {
		fmt.Println("Discovering models...")
	}
	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	models := eng.GetModels()
	fmt.Printf("Found %d models\n", len(models))

	// Run models
	var run interface{ ID() string }
	if *select_ != "" {
		// Run selected models
		selected := strings.Split(*select_, ",")
		for i := range selected {
			selected[i] = strings.TrimSpace(selected[i])
		}
		downstreamStr := ""
		if *downstream {
			downstreamStr = " (+ downstream)"
		}
		fmt.Printf("Running %d selected models%s...\n", len(selected), downstreamStr)
		result, err := eng.RunSelected(ctx, env, selected, *downstream)
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
		result, err := eng.Run(ctx, env)
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

	_ = run // Silence unused variable warning

	return nil
}

// listCmd lists all models.
func listCmd(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	setupFlags(fs)
	fs.Parse(args)

	eng, err := createEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	// Discover models
	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	models := eng.GetModels()
	graph := eng.GetGraph()

	fmt.Printf("Models (%d total):\n\n", len(models))

	// Get execution order
	sorted, err := graph.TopologicalSort()
	if err != nil {
		return fmt.Errorf("failed to sort models: %w", err)
	}

	for i, node := range sorted {
		m := models[node.ID]
		if m == nil {
			continue
		}

		deps := graph.GetParents(node.ID)
		depStr := ""
		if len(deps) > 0 {
			depStr = fmt.Sprintf(" <- %s", strings.Join(deps, ", "))
		}

		fmt.Printf("  %2d. %-35s [%s]%s\n", i+1, m.Path, m.Materialized, depStr)
	}

	return nil
}

// seedCmd loads seed data.
func seedCmd(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	setupFlags(fs)
	fs.Parse(args)

	eng, err := createEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	ctx := context.Background()

	fmt.Printf("Loading seeds from %s...\n", seedsDir)
	if err := eng.LoadSeeds(ctx); err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	fmt.Println("Seeds loaded successfully")
	return nil
}

// dagCmd shows the dependency graph.
func dagCmd(args []string) error {
	fs := flag.NewFlagSet("dag", flag.ExitOnError)
	setupFlags(fs)
	fs.Parse(args)

	eng, err := createEngine()
	if err != nil {
		return err
	}
	defer eng.Close()

	// Discover models
	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	graph := eng.GetGraph()

	// Get execution levels
	levels, err := graph.GetExecutionLevels()
	if err != nil {
		return fmt.Errorf("failed to get execution levels: %w", err)
	}

	fmt.Println("Dependency Graph (execution levels):")
	fmt.Println()

	for i, level := range levels {
		fmt.Printf("Level %d:\n", i)
		for _, model := range level {
			deps := graph.GetParents(model)
			children := graph.GetChildren(model)

			fmt.Printf("  %s\n", model)
			if len(deps) > 0 {
				fmt.Printf("    depends on: %s\n", strings.Join(deps, ", "))
			}
			if len(children) > 0 {
				fmt.Printf("    used by: %s\n", strings.Join(children, ", "))
			}
		}
		fmt.Println()
	}

	fmt.Printf("Total: %d models, %d dependencies\n", graph.NodeCount(), graph.EdgeCount())

	return nil
}

// versionCmd shows version information.
func versionCmd(args []string) error {
	fmt.Println("LeapSQL v0.1.0")
	fmt.Println("Data Transformation Engine built with Go and DuckDB")
	return nil
}

// lspCmd starts the Language Server Protocol server.
func lspCmd(args []string) error {
	fs := flag.NewFlagSet("lsp", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println("Usage: leapsql lsp")
		fmt.Println()
		fmt.Println("Starts the LSP server for IDE integration.")
		fmt.Println("The server communicates over stdin/stdout using JSON-RPC.")
		fmt.Println()
		fmt.Println("The project root and state database are determined by the")
		fmt.Println("client's initialization request (rootUri parameter).")
	}
	fs.Parse(args)

	server := lsp.NewServer(os.Stdin, os.Stdout)
	return server.Run()
}

// discoverCmd indexes macros and models for IDE features.
func discoverCmd(args []string) error {
	fs := flag.NewFlagSet("discover", flag.ExitOnError)
	setupFlags(fs)
	fs.Usage = func() {
		fmt.Println("Usage: leapsql discover [options]")
		fmt.Println()
		fmt.Println("Indexes macros and models into the SQLite state database")
		fmt.Println("for use by the LSP server and other IDE features.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	// Ensure state directory exists
	stateDir := filepath.Dir(statePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0755); err != nil {
			return fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	// Open or create SQLite database
	store := state.NewSQLiteStore()
	if err := store.Open(statePath); err != nil {
		return fmt.Errorf("failed to open state database: %w", err)
	}
	defer store.Close()

	// Initialize schema (creates tables if they don't exist)
	if err := store.InitSchema(); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Discover macros
	macroCount, funcCount, err := discoverMacros(store, macrosDir)
	if err != nil {
		return fmt.Errorf("failed to discover macros: %w", err)
	}

	fmt.Printf("Discovered %d macro namespaces with %d functions\n", macroCount, funcCount)
	fmt.Printf("State saved to %s\n", statePath)

	return nil
}

// discoverMacros scans the macros directory and indexes all .star files.
func discoverMacros(store state.StateStore, dir string) (int, int, error) {
	// Check if macros directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		fmt.Printf("Macros directory not found: %s (skipping)\n", dir)
		return 0, 0, nil
	}

	var namespaceCount, functionCount int

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-.star files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".star") {
			return nil
		}

		// Read file content
		content, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("Warning: failed to read %s: %v\n", path, err)
			return nil
		}

		// Parse the .star file
		absPath, _ := filepath.Abs(path)
		parsed, err := macro.ParseStarlarkFile(absPath, content)
		if err != nil {
			fmt.Printf("Warning: failed to parse %s: %v\n", path, err)
			return nil
		}

		// Convert to state types
		ns := &state.MacroNamespace{
			Name:     parsed.Name,
			FilePath: parsed.FilePath,
			Package:  parsed.Package,
		}

		var funcs []*state.MacroFunction
		for _, f := range parsed.Functions {
			funcs = append(funcs, &state.MacroFunction{
				Namespace: parsed.Name,
				Name:      f.Name,
				Args:      f.Args,
				Docstring: f.Docstring,
				Line:      f.Line,
			})
		}

		// Save to database
		if err := store.SaveMacroNamespace(ns, funcs); err != nil {
			fmt.Printf("Warning: failed to save %s: %v\n", parsed.Name, err)
			return nil
		}

		if verbose {
			fmt.Printf("  Indexed: %s (%d functions)\n", parsed.Name, len(funcs))
		}

		namespaceCount++
		functionCount += len(funcs)

		return nil
	})

	return namespaceCount, functionCount, err
}

// docsCmd handles the docs subcommands.
func docsCmd(args []string) error {
	if len(args) < 1 {
		fmt.Println("Usage: leapsql docs <build|serve> [options]")
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  build    Generate static documentation site")
		fmt.Println("  serve    Build and serve documentation locally")
		return nil
	}

	switch args[0] {
	case "build":
		return docsBuildCmd(args[1:])
	case "serve":
		return docsServeCmd(args[1:])
	default:
		return fmt.Errorf("unknown docs subcommand: %s", args[0])
	}
}

// docsBuildCmd generates the static documentation site.
func docsBuildCmd(args []string) error {
	fs := flag.NewFlagSet("docs build", flag.ExitOnError)
	modelsPath := fs.String("models", modelsDir, "Path to models directory")
	outputPath := fs.String("output", "./docs-site", "Output directory for generated site")
	projectName := fs.String("project", "LeapSQL Project", "Project name for documentation")

	fs.Usage = func() {
		fmt.Println("Usage: leapsql docs build [options]")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate models directory exists
	if _, err := os.Stat(*modelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", *modelsPath)
	}

	fmt.Printf("Building documentation...\n")
	fmt.Printf("  Models:  %s\n", *modelsPath)
	fmt.Printf("  Output:  %s\n", *outputPath)
	fmt.Printf("  Project: %s\n", *projectName)
	fmt.Println()

	gen := docs.NewGenerator(*projectName)

	if err := gen.LoadModels(*modelsPath); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	if err := gen.Build(*outputPath); err != nil {
		return fmt.Errorf("failed to build docs: %w", err)
	}

	fmt.Printf("Documentation generated successfully!\n")
	fmt.Printf("Open %s/index.html in your browser\n", *outputPath)

	return nil
}

// docsServeCmd builds and serves the documentation locally.
func docsServeCmd(args []string) error {
	fs := flag.NewFlagSet("docs serve", flag.ExitOnError)
	modelsPath := fs.String("models", modelsDir, "Path to models directory")
	outputPath := fs.String("output", "./.leapsql-docs", "Output directory for generated site")
	projectName := fs.String("project", "LeapSQL Project", "Project name for documentation")
	port := fs.Int("port", 8080, "Port to serve on")

	fs.Usage = func() {
		fmt.Println("Usage: leapsql docs serve [options]")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate models directory exists
	if _, err := os.Stat(*modelsPath); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", *modelsPath)
	}

	fmt.Printf("Building documentation...\n")
	fmt.Printf("  Models:  %s\n", *modelsPath)
	fmt.Printf("  Project: %s\n", *projectName)
	fmt.Println()

	gen := docs.NewGenerator(*projectName)

	if err := gen.LoadModels(*modelsPath); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	if err := gen.Serve(*outputPath, *port); err != nil {
		return fmt.Errorf("failed to serve docs: %w", err)
	}

	return nil
}
