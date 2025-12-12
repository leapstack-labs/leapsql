// Package testutil provides test utilities for CLI testing.
package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// SetupTestProject creates a temporary project with test models.
func SetupTestProject(t *testing.T) string {
	t.Helper()

	tmpDir := t.TempDir()

	// Create directories
	dirs := []string{
		filepath.Join(tmpDir, "models", "staging"),
		filepath.Join(tmpDir, "models", "marts"),
		filepath.Join(tmpDir, "seeds"),
		filepath.Join(tmpDir, "macros"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create directory %s: %v", dir, err)
		}
	}

	// Create test model
	stgCustomers := `---
materialized: table
---
SELECT 
    id AS customer_id,
    name AS customer_name
FROM raw_customers`
	if err := os.WriteFile(filepath.Join(tmpDir, "models", "staging", "stg_customers.sql"),
		[]byte(stgCustomers), 0644); err != nil {
		t.Fatalf("failed to create stg_customers.sql: %v", err)
	}

	// Create seed file
	rawCustomers := `id,name
1,Alice
2,Bob`
	if err := os.WriteFile(filepath.Join(tmpDir, "seeds", "raw_customers.csv"),
		[]byte(rawCustomers), 0644); err != nil {
		t.Fatalf("failed to create raw_customers.csv: %v", err)
	}

	return tmpDir
}

// GetTestdataDir returns the path to the testdata directory.
func GetTestdataDir(t *testing.T) string {
	t.Helper()

	// Get the absolute path to testdata directory
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	// Try different relative paths based on where tests are run from
	candidates := []string{
		filepath.Join(wd, "testdata"),
		filepath.Join(wd, "..", "testdata"),
		filepath.Join(wd, "..", "..", "testdata"),
		filepath.Join(wd, "..", "..", "..", "testdata"),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	t.Fatalf("testdata directory not found, tried: %v", candidates)
	return ""
}

// CaptureOutput is a helper that can be used to capture stdout during tests.
// Returns a cleanup function that should be deferred.
func CaptureOutput(t *testing.T) (output *os.File, cleanup func() string) {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w

	return w, func() string {
		w.Close()
		os.Stdout = old
		buf := make([]byte, 4096)
		n, _ := r.Read(buf)
		return string(buf[:n])
	}
}
