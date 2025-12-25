package core_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCoreImportsOnly verifies pkg/core only imports allowed packages.
// The Golden Rule: pkg/core imports ONLY pkg/token and stdlib.
func TestCoreImportsOnly(t *testing.T) {
	// Allowed imports for pkg/core
	// NOTE: pkg/spi is NOT allowed - it's a Mechanism Contract, not Domain Data
	allowedExternal := map[string]bool{
		"github.com/leapstack-labs/leapsql/pkg/token": true,
	}

	fset := token.NewFileSet()

	// Find the core package directory relative to test location
	coreDir := "."

	entries, err := os.ReadDir(coreDir)
	if err != nil {
		t.Fatalf("Failed to read core directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		// Skip test files
		if strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		path := filepath.Join(coreDir, entry.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", path, err)
			continue
		}

		for _, imp := range f.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)

			// Allow stdlib (no dots in path, except for test packages)
			if !strings.Contains(importPath, ".") {
				continue
			}

			// Check if external import is allowed
			if !allowedExternal[importPath] {
				t.Errorf("%s imports forbidden package: %s", entry.Name(), importPath)
			}
		}
	}
}

// TestCoreDoesNotImportInternal verifies pkg/core doesn't import any internal packages.
func TestCoreDoesNotImportInternal(t *testing.T) {
	fset := token.NewFileSet()
	coreDir := "."

	entries, err := os.ReadDir(coreDir)
	if err != nil {
		t.Fatalf("Failed to read core directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		if strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		path := filepath.Join(coreDir, entry.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", path, err)
			continue
		}

		for _, imp := range f.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)

			if strings.Contains(importPath, "/internal/") {
				t.Errorf("%s imports internal package: %s (core must not import internal packages)", entry.Name(), importPath)
			}
		}
	}
}

// TestCoreDoesNotImportSPI verifies pkg/core doesn't import pkg/spi.
// pkg/spi contains mechanism contracts, not domain data.
func TestCoreDoesNotImportSPI(t *testing.T) {
	fset := token.NewFileSet()
	coreDir := "."

	entries, err := os.ReadDir(coreDir)
	if err != nil {
		t.Fatalf("Failed to read core directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		if strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		path := filepath.Join(coreDir, entry.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", path, err)
			continue
		}

		for _, imp := range f.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)

			if strings.Contains(importPath, "leapsql/pkg/spi") {
				t.Errorf("%s imports pkg/spi: %s (core must not import spi - it's a mechanism contract)", entry.Name(), importPath)
			}
		}
	}
}
