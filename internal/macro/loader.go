// Package macro provides functionality for loading and managing Starlark macros.
// Macros are loaded from .star files and auto-namespaced based on filename.
package macro

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.starlark.net/starlark"
)

// Loader scans a directory for .star files and loads them as Starlark modules.
type Loader struct {
	dir string
}

// NewLoader creates a new macro loader for the specified directory.
func NewLoader(dir string) *Loader {
	return &Loader{dir: dir}
}

// LoadedModule represents a parsed Starlark macro file.
type LoadedModule struct {
	// Namespace is derived from filename (e.g., "datetime" from "datetime.star")
	Namespace string

	// Path is the absolute path to the .star file
	Path string

	// Exports contains all exported functions/values (names not starting with _)
	Exports starlark.StringDict
}

// Load scans the macro directory and loads all .star files.
// Returns a slice of loaded modules and any errors encountered.
func (l *Loader) Load() ([]*LoadedModule, error) {
	// Check if directory exists
	info, err := os.Stat(l.dir)
	if err != nil {
		if os.IsNotExist(err) {
			// No macros directory is fine - return empty slice
			return nil, nil
		}
		return nil, fmt.Errorf("failed to access macros directory: %w", err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("macros path is not a directory: %s", l.dir)
	}

	// Find all .star files
	pattern := filepath.Join(l.dir, "*.star")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to scan macros directory: %w", err)
	}

	var modules []*LoadedModule

	for _, file := range files {
		module, err := l.loadFile(file)
		if err != nil {
			return nil, err
		}
		modules = append(modules, module)
	}

	return modules, nil
}

// loadFile loads a single .star file and extracts its exports.
func (l *Loader) loadFile(path string) (*LoadedModule, error) {
	// Read file content
	content, err := os.ReadFile(path) //nolint:gosec // G304: path comes from filepath.Walk within macros directory
	if err != nil {
		return nil, &LoadError{
			File:    path,
			Message: fmt.Sprintf("failed to read file: %v", err),
		}
	}

	// Derive namespace from filename
	base := filepath.Base(path)
	namespace := strings.TrimSuffix(base, ".star")

	// Validate namespace name
	if err := validateNamespace(namespace); err != nil {
		return nil, &LoadError{
			File:    path,
			Message: err.Error(),
		}
	}

	// Create a new Starlark thread for execution
	thread := &starlark.Thread{
		Name: fmt.Sprintf("load:%s", namespace),
		Print: func(_ *starlark.Thread, _ string) {
			// Ignore prints during macro loading
		},
	}

	// Execute the Starlark file
	globals, err := starlark.ExecFile(thread, path, content, nil) //nolint:staticcheck // SA1019: will migrate to ExecFileOptions later
	if err != nil {
		return nil, &LoadError{
			File:    path,
			Message: fmt.Sprintf("Starlark execution error: %v", err),
		}
	}

	// Filter exports (exclude names starting with _)
	exports := make(starlark.StringDict)
	for name, value := range globals {
		if !strings.HasPrefix(name, "_") {
			exports[name] = value
		}
	}

	return &LoadedModule{
		Namespace: namespace,
		Path:      path,
		Exports:   exports,
	}, nil
}

// validateNamespace checks if a namespace name is valid.
func validateNamespace(name string) error {
	if name == "" {
		return fmt.Errorf("namespace cannot be empty")
	}

	// Check for valid identifier
	for i, r := range name {
		if i == 0 {
			if !isLetter(r) && r != '_' {
				return fmt.Errorf("namespace must start with letter or underscore: %s", name)
			}
		} else {
			if !isLetter(r) && !isDigit(r) && r != '_' {
				return fmt.Errorf("namespace contains invalid character: %s", name)
			}
		}
	}

	return nil
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

// LoadError represents an error loading a macro file.
type LoadError struct {
	File    string
	Message string
}

func (e *LoadError) Error() string {
	return fmt.Sprintf("macros/%s: %s", filepath.Base(e.File), e.Message)
}
