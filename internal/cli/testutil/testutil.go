// Package testutil provides test utilities for CLI testing.
package testutil

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
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

// TestRenderer wraps a Renderer for testing with captured output buffers.
type TestRenderer struct {
	*output.Renderer
	Out    *bytes.Buffer
	ErrOut *bytes.Buffer
}

// NewTestRenderer creates a new test renderer with the specified mode and TTY state.
// Output is captured in buffers for inspection.
func NewTestRenderer(mode output.OutputMode, isTTY bool) *TestRenderer {
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	return &TestRenderer{
		Renderer: output.NewRendererWithTTY(out, errOut, isTTY, mode),
		Out:      out,
		ErrOut:   errOut,
	}
}

// NewTestRendererAuto creates a new test renderer with auto mode detection.
// In tests, non-TTY defaults to markdown output.
func NewTestRendererAuto() *TestRenderer {
	return NewTestRenderer(output.ModeAuto, false)
}

// NewTestRendererText creates a new test renderer in text mode (simulated TTY).
func NewTestRendererText() *TestRenderer {
	return NewTestRenderer(output.ModeText, true)
}

// NewTestRendererMarkdown creates a new test renderer in markdown mode.
func NewTestRendererMarkdown() *TestRenderer {
	return NewTestRenderer(output.ModeMarkdown, false)
}

// NewTestRendererJSON creates a new test renderer in JSON mode.
func NewTestRendererJSON() *TestRenderer {
	return NewTestRenderer(output.ModeJSON, false)
}

// Output returns the combined stdout output as a string.
func (tr *TestRenderer) Output() string {
	return tr.Out.String()
}

// ErrorOutput returns the stderr output as a string.
func (tr *TestRenderer) ErrorOutput() string {
	return tr.ErrOut.String()
}

// Reset clears both output buffers.
func (tr *TestRenderer) Reset() {
	tr.Out.Reset()
	tr.ErrOut.Reset()
}

// ansiPattern matches ANSI escape codes.
var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// AssertNoANSI checks that a string contains no ANSI escape codes.
func AssertNoANSI(t *testing.T, s string) {
	t.Helper()
	if ansiPattern.MatchString(s) {
		t.Errorf("string contains ANSI escape codes: %q", s)
	}
}

// AssertContains checks that the string contains the expected substring.
func AssertContains(t *testing.T, s, expected string) {
	t.Helper()
	if !strings.Contains(s, expected) {
		t.Errorf("string %q does not contain expected %q", s, expected)
	}
}

// AssertNotContains checks that the string does not contain the substring.
func AssertNotContains(t *testing.T, s, unexpected string) {
	t.Helper()
	if strings.Contains(s, unexpected) {
		t.Errorf("string %q unexpectedly contains %q", s, unexpected)
	}
}

// AssertValidMarkdown performs basic markdown validation.
// It checks for unclosed code fences and basic structure.
func AssertValidMarkdown(t *testing.T, md string) {
	t.Helper()

	// Check for balanced code fences
	fenceCount := strings.Count(md, "```")
	if fenceCount%2 != 0 {
		t.Errorf("unbalanced code fences in markdown: found %d occurrences", fenceCount)
	}

	// Check that headers have content
	lines := strings.Split(md, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") && strings.TrimLeft(trimmed, "# ") == "" {
			t.Errorf("empty header at line %d: %q", i+1, line)
		}
	}
}

// AssertOutputMode checks that the renderer output matches expected mode characteristics.
func AssertOutputMode(t *testing.T, tr *TestRenderer, expectedMode output.OutputMode) {
	t.Helper()

	combinedOutput := tr.Output() + tr.ErrorOutput()

	switch expectedMode {
	case output.ModeMarkdown:
		AssertNoANSI(t, combinedOutput)
		// Markdown mode should not contain ANSI codes
	case output.ModeText:
		// Text mode may contain ANSI codes if TTY
		// No specific assertion needed
	case output.ModeJSON:
		AssertNoANSI(t, combinedOutput)
		// JSON mode should not contain ANSI codes
	}
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
func CaptureOutput(t *testing.T) (file *os.File, cleanup func() string) {
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
