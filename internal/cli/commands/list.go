package commands

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/spf13/cobra"
)

// NewListCommand creates the list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all models and their dependencies",
		Long: `List all discovered models with their metadata, dependencies, and last run status.

Output adapts to environment:
  - Terminal: Styled, colored output
  - Piped/Scripted: Markdown format (agent-friendly)
  
Use --output to override: auto, text, markdown, json`,
		Example: `  # List all models (auto-detect output format)
  leapsql list

  # List models as JSON
  leapsql list --output json

  # List models as Markdown (for agents/scripts)
  leapsql list --output markdown

  # List models with verbose output
  leapsql list -v`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd)
		},
	}

	return cmd
}

func runList(cmd *cobra.Command) error {
	cmdCtx, cleanup, err := NewCommandContext(cmd)
	if err != nil {
		return err
	}
	defer cleanup()

	eng := cmdCtx.Engine
	r := cmdCtx.Renderer

	// Discover models
	if _, err := eng.Discover(engine.DiscoveryOptions{}); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		return listJSON(eng, r)
	case output.ModeMarkdown:
		return listMarkdown(eng, r)
	default:
		return listText(eng, r)
	}
}

// listText outputs models in styled text format.
func listText(eng *engine.Engine, r *output.Renderer) error {
	models := eng.GetModels()
	graph := eng.GetGraph()

	r.Header(1, fmt.Sprintf("Models (%d total)", len(models)))

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
		r.ModelLine(i+1, m.Path, m.Materialized, deps)
	}

	return nil
}

// listMarkdown outputs models in markdown format.
func listMarkdown(eng *engine.Engine, r *output.Renderer) error {
	models := eng.GetModels()
	graph := eng.GetGraph()
	store := eng.GetStateStore()

	r.Println(output.FormatHeader(1, fmt.Sprintf("Models (%d total)", len(models))))
	r.Println("")

	// Get execution order
	sorted, err := graph.TopologicalSort()
	if err != nil {
		return fmt.Errorf("failed to sort models: %w", err)
	}

	for _, node := range sorted {
		m := models[node.ID]
		if m == nil {
			continue
		}

		r.Println(output.FormatHeader(2, m.Path))

		r.Println(output.FormatKeyValue("Materialized", m.Materialized))
		r.Println(output.FormatKeyValue("File", m.FilePath))

		deps := graph.GetParents(node.ID)
		if len(deps) > 0 {
			r.Println(output.FormatKeyValue("Dependencies", strings.Join(deps, ", ")))
		}

		dependents := graph.GetChildren(node.ID)
		if len(dependents) > 0 {
			r.Println(output.FormatKeyValue("Dependents", strings.Join(dependents, ", ")))
		}

		// Add last run info if available
		if store != nil {
			if stateModel, err := store.GetModelByPath(m.Path); err == nil && stateModel != nil {
				if lastRun, err := store.GetLatestModelRun(stateModel.ID); err == nil && lastRun != nil {
					r.Println(output.FormatKeyValue("Last Run", string(lastRun.Status)))
					if lastRun.RowsAffected > 0 {
						r.Println(output.FormatKeyValue("Rows", fmt.Sprintf("%d", lastRun.RowsAffected)))
					}
				}
			}
		}

		r.Println("")
	}

	return nil
}

// listJSON outputs models and macros in JSON format.
func listJSON(eng *engine.Engine, r *output.Renderer) error {
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
					if lastRun.Status == core.ModelRunStatusFailed {
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

	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(listOutput)
}

// Unused type aliases kept for reference
var _ = (*core.Model)(nil)
var _ = (*dag.Graph)(nil)
