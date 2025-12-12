// Package macro provides functionality for loading and managing Starlark macros.
// This file contains static parsing functions that extract metadata without execution.

package macro

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"go.starlark.net/syntax"
)

// ParsedFunction represents a function extracted from a .star file.
type ParsedFunction struct {
	Name      string   `json:"name"`      // Function name
	Args      []string `json:"args"`      // Argument names (with defaults like "x=None")
	Docstring string   `json:"docstring"` // Docstring if present
	Line      int      `json:"line"`      // Line number for go-to-definition
}

// ParsedNamespace represents a parsed .star file.
type ParsedNamespace struct {
	Name      string            `json:"name"`      // Namespace name (filename without .star)
	FilePath  string            `json:"file_path"` // Absolute path to .star file
	Package   string            `json:"package"`   // "" for local, package name for vendor
	Functions []*ParsedFunction `json:"functions"`
}

// ParseStarlarkFile statically parses a .star file and extracts function metadata.
// This does NOT execute the file - it only analyzes the AST.
func ParseStarlarkFile(filename string, content []byte) (*ParsedNamespace, error) {
	f, err := syntax.Parse(filename, content, 0)
	if err != nil {
		return nil, &ParseError{
			File:    filename,
			Message: err.Error(),
		}
	}

	ns := &ParsedNamespace{
		Name:     strings.TrimSuffix(filepath.Base(filename), ".star"),
		FilePath: filename,
	}

	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if !ok {
			continue
		}

		// Skip private functions (start with _)
		if strings.HasPrefix(def.Name.Name, "_") {
			continue
		}

		fn := &ParsedFunction{
			Name: def.Name.Name,
			Line: int(def.Name.NamePos.Line),
			Args: extractArgs(def.Params),
		}

		// Extract docstring (first statement if it's a string literal)
		fn.Docstring = extractDocstring(def.Body)

		ns.Functions = append(ns.Functions, fn)
	}

	return ns, nil
}

// extractArgs converts syntax parameters to string representations.
func extractArgs(params []syntax.Expr) []string {
	var args []string
	for _, param := range params {
		switch p := param.(type) {
		case *syntax.Ident:
			// Simple parameter: def foo(x)
			args = append(args, p.Name)
		case *syntax.BinaryExpr:
			// Default parameter: def foo(x=1)
			if p.Op == syntax.EQ {
				if ident, ok := p.X.(*syntax.Ident); ok {
					args = append(args, ident.Name+"="+exprToString(p.Y))
				}
			}
		case *syntax.UnaryExpr:
			// *args or **kwargs
			if ident, ok := p.X.(*syntax.Ident); ok {
				prefix := ""
				if p.Op == syntax.STAR {
					prefix = "*"
				} else if p.Op == syntax.STARSTAR {
					prefix = "**"
				}
				args = append(args, prefix+ident.Name)
			}
		}
	}
	return args
}

// extractDocstring gets the docstring from function body if present.
func extractDocstring(body []syntax.Stmt) string {
	if len(body) == 0 {
		return ""
	}

	// Check if first statement is an expression statement with a string literal
	exprStmt, ok := body[0].(*syntax.ExprStmt)
	if !ok {
		return ""
	}

	lit, ok := exprStmt.X.(*syntax.Literal)
	if !ok || lit.Token != syntax.STRING {
		return ""
	}

	// Return the string value (unquoted)
	s, ok := lit.Value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}

// exprToString converts a syntax expression to a string representation.
func exprToString(expr syntax.Expr) string {
	switch e := expr.(type) {
	case *syntax.Literal:
		return e.Raw
	case *syntax.Ident:
		return e.Name
	case *syntax.ListExpr:
		return "[]"
	case *syntax.DictExpr:
		return "{}"
	case *syntax.TupleExpr:
		return "()"
	case *syntax.UnaryExpr:
		if e.Op == syntax.MINUS {
			return "-" + exprToString(e.X)
		}
		return exprToString(e.X)
	default:
		return "..."
	}
}

// ArgsToJSON converts args slice to JSON for SQLite storage.
func ArgsToJSON(args []string) string {
	if args == nil {
		args = []string{}
	}
	b, _ := json.Marshal(args)
	return string(b)
}

// ArgsFromJSON parses args from JSON stored in SQLite.
func ArgsFromJSON(s string) []string {
	var args []string
	if s == "" {
		return args
	}
	json.Unmarshal([]byte(s), &args)
	return args
}

// ParseError represents an error during static parsing.
type ParseError struct {
	File    string
	Message string
}

func (e *ParseError) Error() string {
	return "parse " + filepath.Base(e.File) + ": " + e.Message
}

// FunctionInfo returns a human-readable signature for a function.
func (f *ParsedFunction) Signature() string {
	return f.Name + "(" + strings.Join(f.Args, ", ") + ")"
}

// HasDocstring returns true if the function has a docstring.
func (f *ParsedFunction) HasDocstring() bool {
	return f.Docstring != ""
}
