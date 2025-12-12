package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/macro"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// NewDiscoverCommand creates the discover command.
func NewDiscoverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Index macros and models for IDE features",
		Long: `Index macros and models into the SQLite state database.

This enables IDE features like autocomplete, hover documentation,
and go-to-definition through the LSP server.`,
		Example: `  # Discover all macros and models
  leapsql discover

  # Discover with custom macros directory
  leapsql discover --macros-dir ./custom-macros`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiscover(cmd)
		},
	}

	return cmd
}

func runDiscover(cmd *cobra.Command) error {
	cfg := getConfig()
	verbose := viper.GetBool("verbose")

	// Ensure state directory exists
	stateDir := filepath.Dir(cfg.StatePath)
	if stateDir != "." && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0755); err != nil {
			return fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	// Open or create SQLite database
	store := state.NewSQLiteStore()
	if err := store.Open(cfg.StatePath); err != nil {
		return fmt.Errorf("failed to open state database: %w", err)
	}
	defer store.Close()

	// Initialize schema (creates tables if they don't exist)
	if err := store.InitSchema(); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Discover macros
	macroCount, funcCount, err := discoverMacros(store, cfg.MacrosDir, verbose)
	if err != nil {
		return fmt.Errorf("failed to discover macros: %w", err)
	}

	fmt.Printf("Discovered %d macro namespaces with %d functions\n", macroCount, funcCount)
	fmt.Printf("State saved to %s\n", cfg.StatePath)

	return nil
}

// discoverMacros scans the macros directory and indexes all .star files.
func discoverMacros(store state.StateStore, dir string, verbose bool) (int, int, error) {
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
