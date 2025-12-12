package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
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
and go-to-definition through the LSP server.

Output adapts to environment:
  - Terminal: Styled summary with success indicator
  - Piped/Scripted: Markdown format (agent-friendly)`,
		Example: `  # Discover all macros and models
  leapsql discover

  # Discover with custom macros directory
  leapsql discover --macros-dir ./custom-macros

  # Output as JSON
  leapsql discover --output json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiscover(cmd)
		},
	}

	return cmd
}

func runDiscover(cmd *cobra.Command) error {
	cfg := getConfig()
	verbose := viper.GetBool("verbose")

	// Create renderer
	mode := output.OutputMode(cfg.OutputFormat)
	r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

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
	namespaces, macroCount, funcCount, err := discoverMacrosWithDetails(store, cfg.MacrosDir, verbose)
	if err != nil {
		return fmt.Errorf("failed to discover macros: %w", err)
	}

	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		return discoverJSON(r, namespaces, macroCount, funcCount, cfg.StatePath)
	case output.ModeMarkdown:
		return discoverMarkdown(r, namespaces, macroCount, funcCount, cfg.StatePath)
	default:
		return discoverText(r, namespaces, macroCount, funcCount, cfg.StatePath)
	}
}

// discoverText outputs discovery results in styled text format.
func discoverText(r *output.Renderer, namespaces []output.DiscoverNamespace, macroCount, funcCount int, statePath string) error {
	r.Success(fmt.Sprintf("Discovered %d macro namespaces with %d functions", macroCount, funcCount))
	r.Muted(fmt.Sprintf("State saved to %s", statePath))

	if len(namespaces) > 0 && r.IsTTY() {
		r.Println("")
		r.Header(2, "Namespaces")
		for _, ns := range namespaces {
			r.Printf("  - %s (%d functions)\n", ns.Name, len(ns.Functions))
		}
	}

	return nil
}

// discoverMarkdown outputs discovery results in markdown format.
func discoverMarkdown(r *output.Renderer, namespaces []output.DiscoverNamespace, macroCount, funcCount int, statePath string) error {
	r.Println(output.FormatHeader(1, "Discovery Results"))
	r.Println("")
	r.Println(output.FormatKeyValue("Macro Namespaces", fmt.Sprintf("%d", macroCount)))
	r.Println(output.FormatKeyValue("Total Functions", fmt.Sprintf("%d", funcCount)))
	r.Println(output.FormatKeyValue("State Path", statePath))
	r.Println("")

	if len(namespaces) > 0 {
		r.Println(output.FormatHeader(2, "Namespaces"))
		for _, ns := range namespaces {
			r.Printf("- %s (%d functions)\n", ns.Name, len(ns.Functions))
		}
	}

	return nil
}

// discoverJSON outputs discovery results in JSON format.
func discoverJSON(r *output.Renderer, namespaces []output.DiscoverNamespace, macroCount, funcCount int, statePath string) error {
	discoverOutput := output.DiscoverOutput{
		Namespaces: namespaces,
		Summary: output.DiscoverSummary{
			TotalNamespaces: macroCount,
			TotalFunctions:  funcCount,
			StatePath:       statePath,
		},
	}

	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(discoverOutput)
}

// discoverMacrosWithDetails scans the macros directory and indexes all .star files.
// Returns namespace details for output formatting.
func discoverMacrosWithDetails(store state.StateStore, dir string, verbose bool) ([]output.DiscoverNamespace, int, int, error) {
	// Check if macros directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		fmt.Printf("Macros directory not found: %s (skipping)\n", dir)
		return nil, 0, 0, nil
	}

	var namespaces []output.DiscoverNamespace
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
		var funcNames []string
		for _, f := range parsed.Functions {
			funcs = append(funcs, &state.MacroFunction{
				Namespace: parsed.Name,
				Name:      f.Name,
				Args:      f.Args,
				Docstring: f.Docstring,
				Line:      f.Line,
			})
			funcNames = append(funcNames, f.Name)
		}

		// Save to database
		if err := store.SaveMacroNamespace(ns, funcs); err != nil {
			fmt.Printf("Warning: failed to save %s: %v\n", parsed.Name, err)
			return nil
		}

		if verbose {
			fmt.Printf("  Indexed: %s (%d functions)\n", parsed.Name, len(funcs))
		}

		namespaces = append(namespaces, output.DiscoverNamespace{
			Name:      parsed.Name,
			FilePath:  absPath,
			Functions: funcNames,
		})
		namespaceCount++
		functionCount += len(funcs)

		return nil
	})

	return namespaces, namespaceCount, functionCount, err
}
