package lsp

import (
	"encoding/json"
	"sync"

	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// fixCache stores fixes for diagnostics, keyed by URI and rule ID.
type fixCache struct {
	mu    sync.RWMutex
	fixes map[string]map[string][]lint.Fix // URI -> RuleID -> []Fix
}

var globalFixCache = &fixCache{
	fixes: make(map[string]map[string][]lint.Fix),
}

// cacheFixes stores fixes for a URI and rule ID.
func (c *fixCache) cacheFixes(uri string, ruleID string, fixes []lint.Fix) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fixes[uri] == nil {
		c.fixes[uri] = make(map[string][]lint.Fix)
	}
	c.fixes[uri][ruleID] = fixes
}

// getFixes retrieves fixes for a URI and rule ID.
func (c *fixCache) getFixes(uri string, ruleID string) []lint.Fix {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.fixes[uri] == nil {
		return nil
	}
	return c.fixes[uri][ruleID]
}

// clearURI removes all cached fixes for a URI.
func (c *fixCache) clearURI(uri string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.fixes, uri)
}

// handleCodeAction handles the textDocument/codeAction request.
func (s *Server) handleCodeAction(msg *JSONRPCMessage) error {
	var params CodeActionParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		s.sendResponse(msg.ID, nil, &JSONRPCError{Code: -32602, Message: err.Error()})
		return err
	}

	actions := s.getCodeActions(params)
	s.sendResponse(msg.ID, actions, nil)
	return nil
}

// getCodeActions returns code actions for the given parameters.
func (s *Server) getCodeActions(params CodeActionParams) []CodeAction {
	var actions []CodeAction

	// Filter to only quickfix if requested
	onlyQuickFix := false
	for _, kind := range params.Context.Only {
		if kind == CodeActionKindQuickFix {
			onlyQuickFix = true
			break
		}
	}

	for _, diag := range params.Context.Diagnostics {
		// Get cached fixes for this diagnostic
		fixes := globalFixCache.getFixes(params.TextDocument.URI, diag.Code)

		if len(fixes) == 0 {
			continue
		}

		for _, fix := range fixes {
			// Skip if not a quickfix and only quickfix was requested
			if onlyQuickFix {
				// All our fixes are quickfixes for now
			}

			// Convert lint.Fix to LSP CodeAction
			action := CodeAction{
				Title:       fix.Description,
				Kind:        CodeActionKindQuickFix,
				Diagnostics: []Diagnostic{diag},
				IsPreferred: len(fixes) == 1, // Single fix is preferred
				Edit: &WorkspaceEdit{
					Changes: map[string][]TextEdit{
						params.TextDocument.URI: convertTextEdits(fix.TextEdits),
					},
				},
			}

			actions = append(actions, action)
		}
	}

	return actions
}

// convertTextEdits converts lint.TextEdit to LSP TextEdit.
func convertTextEdits(edits []lint.TextEdit) []TextEdit {
	result := make([]TextEdit, len(edits))
	for i, edit := range edits {
		result[i] = TextEdit{
			Range: Range{
				Start: Position{
					Line:      uint32(max(0, edit.Pos.Line-1)),   //nolint:gosec // G115: line is always non-negative
					Character: uint32(max(0, edit.Pos.Column-1)), //nolint:gosec // G115: column is always non-negative
				},
				End: Position{
					Line:      uint32(max(0, edit.EndPos.Line-1)),   //nolint:gosec // G115: line is always non-negative
					Character: uint32(max(0, edit.EndPos.Column-1)), //nolint:gosec // G115: column is always non-negative
				},
			},
			NewText: edit.NewText,
		}
	}
	return result
}

// cacheDiagnosticFixes stores fixes from lint diagnostics for later retrieval.
func (s *Server) cacheDiagnosticFixes(uri string, diagnostics []lint.Diagnostic) {
	// Clear existing fixes for this URI
	globalFixCache.clearURI(uri)

	// Cache new fixes
	for _, diag := range diagnostics {
		if len(diag.Fixes) > 0 {
			globalFixCache.cacheFixes(uri, diag.RuleID, diag.Fixes)
		}
	}
}
