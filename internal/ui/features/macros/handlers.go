package macros

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the macros feature.
type Handlers struct {
	engine       *engine.Engine
	store        core.Store
	sessionStore sessions.Store
	notifier     *notifier.Notifier
	isDev        bool
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
	notify *notifier.Notifier,
	isDev bool,
) *Handlers {
	return &Handlers{
		engine:       eng,
		store:        store,
		sessionStore: sessionStore,
		notifier:     notify,
		isDev:        isDev,
	}
}

// HandleMacrosPage renders the macros catalog page with full content.
// Handles both /macros (no selection) and /macros/{namespace}/{function} (with selection).
func (h *Handlers) HandleMacrosPage(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")

	sidebar, macrosData, err := h.buildMacrosDataWithSelection(namespace, function)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Title and SSE path depend on whether viewing a specific function
	title := "Macros"
	sseUpdatePath := "/macros/updates"
	if function != "" {
		title = fmt.Sprintf("%s.%s", namespace, function)
		sseUpdatePath = fmt.Sprintf("/macros/%s/%s/updates", namespace, function)
	}

	if err := MacrosPage(title, h.isDev, sidebar, macrosData, sseUpdatePath).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// MacrosPageUpdates is the long-lived SSE endpoint for the macros page.
// Handles both /macros/updates (no selection) and /macros/{ns}/{fn}/updates (with selection).
// Pushes full AppShell on every update so both tree and detail stay in sync.
func (h *Handlers) MacrosPageUpdates(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	sse := datastar.NewSSE(w, r)

	// Subscribe to updates
	updates := h.notifier.Subscribe()
	defer h.notifier.Unsubscribe(updates)

	// Wait for updates (no initial send - content is already rendered)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-updates:
			sidebar, macrosData, err := h.buildMacrosDataWithSelection(namespace, function)
			if err != nil {
				_ = sse.ConsoleError(err)
				continue
			}
			if err := sse.PatchElementTempl(MacrosAppShell(sidebar, macrosData)); err != nil {
				_ = sse.ConsoleError(err)
			}
		}
	}
}

// buildMacrosDataWithSelection assembles all data needed for the macros view with optional selected function.
func (h *Handlers) buildMacrosDataWithSelection(selectedNs, selectedFn string) (common.SidebarData, *ViewData, error) {
	sidebar := common.SidebarData{
		CurrentPath: "/macros",
		FullWidth:   true,
	}

	if selectedFn != "" {
		sidebar.CurrentPath = fmt.Sprintf("/macros/%s/%s", selectedNs, selectedFn)
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return sidebar, nil, err
	}
	sidebar.ExplorerTree = common.BuildExplorerTree(models)

	// Build macros view data
	macrosData, err := h.buildMacrosViewData(selectedNs, selectedFn)
	if err != nil {
		return sidebar, nil, err
	}

	return sidebar, macrosData, nil
}

// buildMacrosViewData builds the macros-specific view data.
func (h *Handlers) buildMacrosViewData(selectedNs, selectedFn string) (*ViewData, error) {
	viewData := &ViewData{}

	// Get all namespaces
	namespaces, err := h.store.GetMacroNamespaces()
	if err != nil {
		return viewData, err
	}

	// Build namespace data with functions
	for _, ns := range namespaces {
		nsData := NamespaceData{
			Name:     ns.Name,
			FilePath: ns.FilePath,
		}

		// Get functions for this namespace
		functions, err := h.store.GetMacroFunctions(ns.Name)
		if err != nil {
			continue // Skip namespace on error
		}

		for _, fn := range functions {
			sig := buildSignature(fn.Name, fn.Args)
			nsData.Functions = append(nsData.Functions, FunctionSummary{
				Name:      fn.Name,
				Signature: sig,
				HasDoc:    fn.Docstring != "",
			})
		}

		viewData.Namespaces = append(viewData.Namespaces, nsData)
	}

	// If a function is selected, load its detail
	if selectedNs != "" && selectedFn != "" {
		detail, err := h.buildFunctionDetail(selectedNs, selectedFn)
		if err == nil {
			viewData.SelectedFunction = detail
		}
		// If error, just don't show the detail (function might have been deleted)
	}

	return viewData, nil
}

// buildFunctionDetail loads full function details including source code.
func (h *Handlers) buildFunctionDetail(namespace, name string) (*FunctionDetail, error) {
	// Get function from store
	fn, err := h.store.GetMacroFunction(namespace, name)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, fmt.Errorf("function not found: %s.%s", namespace, name)
	}

	// Get namespace for file path
	ns, err := h.store.GetMacroNamespace(namespace)
	if err != nil {
		return nil, err
	}

	// Extract source code
	sourceCode, err := extractFunctionSource(ns.FilePath, fn.Line)
	if err != nil {
		sourceCode = "// Source code unavailable"
	}

	// Build detail
	detail := &FunctionDetail{
		Namespace:  namespace,
		Name:       fn.Name,
		Signature:  buildSignature(fn.Name, fn.Args),
		Docstring:  fn.Docstring,
		SourceCode: sourceCode,
		FilePath:   ns.FilePath,
		Line:       fn.Line,
	}

	// Parse arguments
	for _, arg := range fn.Args {
		argDetail := parseArgument(arg)
		detail.Args = append(detail.Args, argDetail)
	}

	return detail, nil
}

// buildSignature creates a function signature string.
func buildSignature(name string, args []string) string {
	return fmt.Sprintf("%s(%s)", name, strings.Join(args, ", "))
}

// parseArgument parses an argument string like "x" or "x=default".
func parseArgument(arg string) ArgDetail {
	if idx := strings.Index(arg, "="); idx != -1 {
		return ArgDetail{
			Name:     arg[:idx],
			Default:  arg[idx+1:],
			Required: false,
		}
	}
	return ArgDetail{
		Name:     arg,
		Required: true,
	}
}

// extractFunctionSource extracts function source code from a .star file.
// Uses simple line-based extraction: starts at startLine, ends at next "def " or EOF.
func extractFunctionSource(filePath string, startLine int) (string, error) {
	file, err := os.Open(filePath) //nolint:gosec // filePath comes from trusted internal store
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

	var lines []string
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip lines before our function
		if lineNum < startLine {
			continue
		}

		// Stop at next top-level def (not indented)
		if lineNum > startLine && strings.HasPrefix(line, "def ") {
			break
		}

		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	// Trim trailing empty lines
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}

	return strings.Join(lines, "\n"), nil
}
