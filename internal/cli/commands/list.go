package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/spf13/cobra"
)

// ListOptions holds options for the list command.
type ListOptions struct {
	OutputFormat string
}

// NewListCommand creates the list command.
func NewListCommand() *cobra.Command {
	opts := &ListOptions{}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all models and their dependencies",
		Long: `List all discovered models with their metadata, dependencies, and last run status.

The output includes model paths, materialization types, dependencies,
and execution history from the state database.`,
		Example: `  # List all models in text format
  leapsql list

  # List models as JSON
  leapsql list --output json

  # List models with verbose output
  leapsql list -v`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.OutputFormat, "output", "o", "text", "Output format (text|json)")

	// Register completion for output flag
	cmd.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"text", "json"}, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}

func runList(cmd *cobra.Command, opts *ListOptions) error {
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	// Discover models
	if err := eng.Discover(); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	w := cmd.OutOrStdout()
	if opts.OutputFormat == "json" {
		return listJSON(eng, w)
	}
	return listText(eng, w)
}

// listText outputs models in text format.
func listText(eng *engine.Engine, w io.Writer) error {
	models := eng.GetModels()
	graph := eng.GetGraph()

	fmt.Fprintf(w, "Models (%d total):\n\n", len(models))

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

		fmt.Fprintf(w, "  %2d. %-35s [%s]%s\n", i+1, m.Path, m.Materialized, depStr)
	}

	return nil
}

// listJSON outputs models and macros in JSON format.
func listJSON(eng *engine.Engine, w io.Writer) error {
	models := eng.GetModels()
	graph := eng.GetGraph()
	store := eng.GetStateStore()

	listOutput := output.ListOutput{
		Models: make([]output.ModelInfo, 0, len(models)),
		Macros: []output.MacroInfo{},
		Summary: output.ListSummary{
			TotalModels: len(models),
			ByStatus:    make(map[string]int),
			StaleCount:  0,
		},
	}

	// Build model info
	for path, m := range models {
		absPath, _ := filepath.Abs(m.FilePath)

		// Get dependencies and dependents from graph
		deps := graph.GetParents(path)
		dependents := graph.GetChildren(path)

		// Extract schema from path
		schema := ""
		parts := strings.Split(path, ".")
		if len(parts) > 1 {
			schema = parts[0]
		}

		modelInfo := output.ModelInfo{
			Path:         path,
			Name:         m.Name,
			Materialized: m.Materialized,
			FilePath:     absPath,
			Schema:       schema,
			Owner:        m.Owner,
			Tags:         m.Tags,
			Dependencies: deps,
			Dependents:   dependents,
			IsStale:      false,
		}

		// Get last run info from state store
		if store != nil {
			if stateModel, err := store.GetModelByPath(path); err == nil && stateModel != nil {
				modelInfo.ContentHash = stateModel.ContentHash

				if lastRun, err := store.GetLatestModelRun(stateModel.ID); err == nil && lastRun != nil {
					var errPtr *string
					if lastRun.Error != "" {
						errPtr = &lastRun.Error
					}
					completedAt := ""
					if lastRun.CompletedAt != nil {
						completedAt = lastRun.CompletedAt.Format(time.RFC3339)
					}
					modelInfo.LastRun = &output.LastRunInfo{
						Status:       string(lastRun.Status),
						RowsAffected: lastRun.RowsAffected,
						ExecutionMS:  lastRun.ExecutionMS,
						CompletedAt:  completedAt,
						Error:        errPtr,
					}

					// Update summary stats
					listOutput.Summary.ByStatus[string(lastRun.Status)]++

					// Check if stale (content hash changed since last run)
					if lastRun.Status == state.ModelRunStatusFailed {
						modelInfo.IsStale = true
						listOutput.Summary.StaleCount++
					}
				} else {
					listOutput.Summary.ByStatus["never_run"]++
				}
			}
		}

		listOutput.Models = append(listOutput.Models, modelInfo)
	}

	// Get macro info from state store
	if store != nil {
		namespaces, err := store.GetMacroNamespaces()
		if err == nil {
			for _, ns := range namespaces {
				macroInfo := output.MacroInfo{
					Namespace: ns.Name,
					FilePath:  ns.FilePath,
					Package:   ns.Package,
					Functions: []output.FunctionInfo{},
				}

				funcs, err := store.GetMacroFunctions(ns.Name)
				if err == nil {
					for _, f := range funcs {
						macroInfo.Functions = append(macroInfo.Functions, output.FunctionInfo{
							Name: f.Name,
							Args: f.Args,
							Line: f.Line,
						})
					}
				}

				listOutput.Macros = append(listOutput.Macros, macroInfo)
			}
		}
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(listOutput)
}

// Unused type aliases kept for reference
var _ = (*parser.ModelConfig)(nil)
var _ = (*dag.Graph)(nil)
