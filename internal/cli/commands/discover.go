package commands

import (
	"encoding/json"
	"fmt"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/spf13/cobra"
)

// NewDiscoverCommand creates the discover command.
func NewDiscoverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Index macros and models for IDE features",
		Long: `Index macros and models into the SQLite state database.

This enables IDE features like autocomplete, hover documentation,
and go-to-definition through the LSP server.

Discovery is incremental - only changed files are re-parsed.
Use --force to re-parse all files regardless of content hash.

Output adapts to environment:
  - Terminal: Styled summary with success indicator
  - Piped/Scripted: Markdown format (agent-friendly)`,
		Example: `  # Discover all macros and models
  leapsql discover

  # Force full refresh (ignore content hashes)
  leapsql discover --force

  # Output as JSON
  leapsql discover --output json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiscover(cmd)
		},
	}

	cmd.Flags().BoolP("force", "f", false, "Force full refresh, ignore content hashes")

	return cmd
}

func runDiscover(cmd *cobra.Command) error {
	cfg := getConfig()

	// Create renderer
	mode := output.Mode(cfg.OutputFormat)
	r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

	// Create engine (no DB connection needed for discovery)
	eng, err := createEngine(cfg)
	if err != nil {
		return fmt.Errorf("failed to create engine: %w", err)
	}
	defer func() { _ = eng.Close() }()

	// Run unified discovery
	force, _ := cmd.Flags().GetBool("force")
	opts := engine.DiscoveryOptions{
		ForceFullRefresh: force,
	}

	result, err := eng.Discover(opts)
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}

	// Render results based on output mode
	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		return discoverJSON(r, result, cfg.StatePath)
	case output.ModeMarkdown:
		return discoverMarkdown(r, result, cfg.StatePath)
	default:
		return discoverText(r, result, cfg.StatePath)
	}
}

// discoverText outputs discovery results in styled text format.
func discoverText(r *output.Renderer, result *engine.DiscoveryResult, statePath string) error {
	// Main summary
	r.Success(result.Summary())
	r.Muted(fmt.Sprintf("State saved to %s", statePath))

	// Show details in TTY mode
	if r.IsTTY() {
		r.Println("")

		// Models section
		if result.ModelsTotal > 0 {
			r.Header(2, "Models")
			if result.ModelsChanged > 0 {
				r.Printf("  Changed: %d\n", result.ModelsChanged)
			}
			if result.ModelsSkipped > 0 {
				r.Printf("  Skipped (unchanged): %d\n", result.ModelsSkipped)
			}
			if result.ModelsDeleted > 0 {
				r.Printf("  Deleted: %d\n", result.ModelsDeleted)
			}
			r.Println("")
		}

		// Macros section
		if result.MacrosTotal > 0 {
			r.Header(2, "Macros")
			if result.MacrosChanged > 0 {
				r.Printf("  Changed: %d\n", result.MacrosChanged)
			}
			if result.MacrosSkipped > 0 {
				r.Printf("  Skipped (unchanged): %d\n", result.MacrosSkipped)
			}
			if result.MacrosDeleted > 0 {
				r.Printf("  Deleted: %d\n", result.MacrosDeleted)
			}
			r.Println("")
		}

		// Seeds section
		if result.SeedsValidated > 0 || len(result.SeedsMissing) > 0 {
			r.Header(2, "Seeds")
			if result.SeedsValidated > 0 {
				r.Printf("  Validated: %d\n", result.SeedsValidated)
			}
			if len(result.SeedsMissing) > 0 {
				r.Warning(fmt.Sprintf("  Missing seeds: %v", result.SeedsMissing))
			}
			r.Println("")
		}

		// Errors section
		if result.HasErrors() {
			r.Header(2, "Errors")
			for _, e := range result.Errors {
				r.Error(fmt.Sprintf("  [%s] %s: %s", e.Type, e.Path, e.Message))
			}
		}
	}

	return nil
}

// discoverMarkdown outputs discovery results in markdown format.
func discoverMarkdown(r *output.Renderer, result *engine.DiscoveryResult, statePath string) error {
	r.Println(output.FormatHeader(1, "Discovery Results"))
	r.Println("")

	// Summary
	r.Println(output.FormatHeader(2, "Summary"))
	r.Println(output.FormatKeyValue("Models", fmt.Sprintf("%d total (%d changed, %d skipped, %d deleted)",
		result.ModelsTotal, result.ModelsChanged, result.ModelsSkipped, result.ModelsDeleted)))
	r.Println(output.FormatKeyValue("Macros", fmt.Sprintf("%d total (%d changed, %d skipped, %d deleted)",
		result.MacrosTotal, result.MacrosChanged, result.MacrosSkipped, result.MacrosDeleted)))
	r.Println(output.FormatKeyValue("Seeds Validated", fmt.Sprintf("%d", result.SeedsValidated)))
	r.Println(output.FormatKeyValue("Duration", result.Duration.String()))
	r.Println(output.FormatKeyValue("State Path", statePath))
	r.Println("")

	// Missing seeds
	if len(result.SeedsMissing) > 0 {
		r.Println(output.FormatHeader(2, "Missing Seeds"))
		for _, seed := range result.SeedsMissing {
			r.Printf("- %s\n", seed)
		}
		r.Println("")
	}

	// Errors
	if result.HasErrors() {
		r.Println(output.FormatHeader(2, "Errors"))
		for _, e := range result.Errors {
			r.Printf("- **[%s]** `%s`: %s\n", e.Type, e.Path, e.Message)
		}
	}

	return nil
}

// discoverJSON outputs discovery results in JSON format.
func discoverJSON(r *output.Renderer, result *engine.DiscoveryResult, statePath string) error {
	discoverOutput := output.DiscoverOutput{
		Namespaces: []output.DiscoverNamespace{}, // Could be populated from state if needed
		Models:     []output.DiscoverModel{},     // Could be populated from state if needed
		Summary: output.DiscoverSummary{
			ModelsTotal:    result.ModelsTotal,
			ModelsChanged:  result.ModelsChanged,
			ModelsSkipped:  result.ModelsSkipped,
			ModelsDeleted:  result.ModelsDeleted,
			MacrosTotal:    result.MacrosTotal,
			MacrosChanged:  result.MacrosChanged,
			MacrosSkipped:  result.MacrosSkipped,
			MacrosDeleted:  result.MacrosDeleted,
			SeedsValidated: result.SeedsValidated,
			SeedsMissing:   result.SeedsMissing,
			ErrorCount:     len(result.Errors),
			StatePath:      statePath,
			DurationMS:     result.Duration.Milliseconds(),
		},
	}

	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(discoverOutput)
}
