// Package macros provides the macros catalog feature for the UI.
package macros

// ViewData holds data for the macros catalog page.
type ViewData struct {
	Namespaces       []NamespaceData
	SelectedFunction *FunctionDetail
}

// NamespaceData represents a macro namespace (one .star file).
type NamespaceData struct {
	Name      string            // Namespace name (filename without .star)
	FilePath  string            // Absolute path to .star file
	Functions []FunctionSummary // Exported functions
}

// FunctionSummary is a compact function representation for the tree view.
type FunctionSummary struct {
	Name      string // Function name
	Signature string // "name(arg1, arg2=default)"
	HasDoc    bool   // True if function has docstring
}

// FunctionDetail holds full function information for the detail panel.
type FunctionDetail struct {
	Namespace  string      // Parent namespace name
	Name       string      // Function name
	Signature  string      // Full signature with args
	Args       []ArgDetail // Parsed arguments
	Docstring  string      // Function docstring (may be empty)
	SourceCode string      // Extracted function source code
	FilePath   string      // Path to .star file
	Line       int         // Line number where function is defined
}

// ArgDetail represents a function argument.
type ArgDetail struct {
	Name     string // Argument name
	Default  string // Default value (empty if required)
	Required bool   // True if no default value
}
