package core_test

import (
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const modulePath = "github.com/leapstack-labs/leapsql"

// architectureRules defines allowed imports for each package prefix.
// Keys are package prefixes (relative to module path).
// Values are allowed import prefixes (relative to module path).
//
// Rules:
// - Self-imports are always allowed (pkg/lint can import pkg/lint/sql)
// - Stdlib imports are always allowed
// - Only project imports (github.com/leapstack-labs/leapsql/*) are checked
//
// The most specific matching rule wins (pkg/lint/sql uses pkg/lint rule, not pkg rule).
var architectureRules = map[string][]string{
	// ============================================
	// Foundation Layer
	// ============================================

	// pkg/token: No project imports allowed (foundational)
	"pkg/token": {},

	// pkg/core: Only imports pkg/token (the hub)
	"pkg/core": {"pkg/token"},

	// ============================================
	// Libraries Layer
	// ============================================

	// pkg/spi: Interfaces using Core AST types
	"pkg/spi": {"pkg/core", "pkg/token"},

	// pkg/dialect: The framework for building dialects
	"pkg/dialect": {"pkg/core", "pkg/spi", "pkg/token"},

	// pkg/parser: The parser engine
	"pkg/parser": {"pkg/core", "pkg/dialect", "pkg/dialects", "pkg/spi", "pkg/token"},

	// pkg/format: AST formatter
	"pkg/format": {"pkg/core", "pkg/dialect", "pkg/parser", "pkg/spi", "pkg/token"},

	// ============================================
	// Lint Layer
	// ============================================

	// pkg/lint: Linting framework and rules
	// Needs parser for AST analysis, core for resource types
	"pkg/lint": {"pkg/core", "pkg/lint", "pkg/parser", "pkg/spi", "pkg/token"},

	// ============================================
	// Dialects Layer (Language Libraries)
	// ============================================

	// pkg/dialects/*: Dialect-specific configurations
	// NOT allowed to import pkg/parser (would create circular risk)
	"pkg/dialects": {"pkg/core", "pkg/dialect", "pkg/spi", "pkg/token"},

	// ============================================
	// Microkernel Layer
	// ============================================

	// pkg/adapter: Registry only - STRICT
	// Must NOT import pkg/dialect (heavy framework)
	"pkg/adapter": {"pkg/core"},

	// pkg/adapters/*: Database drivers
	"pkg/adapters": {"pkg/adapter", "pkg/core", "pkg/dialect", "pkg/dialects"},
}

// knownViolations lists packages with known architecture violations
// that are documented and scheduled to be fixed.
// Key: package path (relative to module), Value: forbidden import (relative to module)
//
// NOTE: These violations FAIL the test intentionally. Fix them or remove the rule.
//
//nolint:unused // Kept for documentation; uncomment violations during migrations
var knownViolations = map[string]string{
	// TODO: Phase 0 - Move DuckDB AST types (ListLiteral, StructLiteral, IndexExpr, LambdaExpr)
	// from pkg/parser to pkg/core/ast_extensions.go
	// "pkg/dialects/duckdb": "pkg/parser",  // UNCOMMENT TO TEMPORARILY ALLOW
}

// TestArchitectureAllowlist validates that all pkg/* packages only import
// allowed packages according to architectureRules.
//
// This enforces the Golden Rule: Dependencies must flow inwards toward pkg/core.
func TestArchitectureAllowlist(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
	}

	pkgs, err := packages.Load(cfg, modulePath+"/pkg/...")
	if err != nil {
		t.Fatalf("failed to load packages: %v", err)
	}

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			// Skip packages with load errors (may be due to build tags)
			continue
		}

		relPath := strings.TrimPrefix(pkg.PkgPath, modulePath+"/")
		allowed := findAllowedImports(relPath)
		if allowed == nil {
			// No rule for this package prefix - this is a bug in the test
			t.Errorf("no architecture rule defined for %s", relPath)
			continue
		}

		for imp := range pkg.Imports {
			// Only check project imports
			if !strings.HasPrefix(imp, modulePath) {
				continue
			}
			relImp := strings.TrimPrefix(imp, modulePath+"/")

			// Self-imports always allowed (pkg/lint can import pkg/lint/sql)
			if strings.HasPrefix(relImp, relPath) || strings.HasPrefix(relPath, relImp) {
				continue
			}

			if !isAllowed(relImp, allowed) {
				t.Errorf("ARCHITECTURE VIOLATION: %s imports %s\n"+
					"  Allowed imports: %v\n"+
					"  See spec/architecture.md for dependency rules",
					relPath, relImp, allowed)
			}
		}
	}
}

// TestPkgDoesNotImportInternal ensures no pkg/* package imports any internal/* package.
// This is a strict rule: libraries must never depend on application code.
func TestPkgDoesNotImportInternal(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
	}

	pkgs, err := packages.Load(cfg, modulePath+"/pkg/...")
	if err != nil {
		t.Fatalf("failed to load packages: %v", err)
	}

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			continue
		}

		relPath := strings.TrimPrefix(pkg.PkgPath, modulePath+"/")

		for imp := range pkg.Imports {
			if strings.Contains(imp, modulePath+"/internal/") {
				t.Errorf("ARCHITECTURE VIOLATION: %s imports internal package %s\n"+
					"  pkg/* must never import internal/*\n"+
					"  See spec/architecture.md for dependency rules",
					relPath, imp)
			}
		}
	}
}

// TestCoreOnlyImportsToken is a focused test for the most critical rule.
// pkg/core is the hub and must only import pkg/token.
func TestCoreOnlyImportsToken(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
	}

	pkgs, err := packages.Load(cfg, modulePath+"/pkg/core")
	if err != nil {
		t.Fatalf("failed to load pkg/core: %v", err)
	}

	if len(pkgs) == 0 {
		t.Fatal("pkg/core not found")
	}

	pkg := pkgs[0]
	allowedExternal := modulePath + "/pkg/token"

	for imp := range pkg.Imports {
		if !strings.HasPrefix(imp, modulePath) {
			continue // stdlib or external, OK
		}

		if imp != allowedExternal {
			t.Errorf("GOLDEN RULE VIOLATION: pkg/core imports %s\n"+
				"  pkg/core may ONLY import pkg/token (and stdlib)\n"+
				"  This is the foundation of the architecture",
				imp)
		}
	}
}

// TestAdapterDoesNotImportDialect is a focused test for a critical microkernel rule.
// pkg/adapter (the registry) must not import pkg/dialect (the framework).
func TestAdapterDoesNotImportDialect(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
	}

	pkgs, err := packages.Load(cfg, modulePath+"/pkg/adapter")
	if err != nil {
		t.Fatalf("failed to load pkg/adapter: %v", err)
	}

	if len(pkgs) == 0 {
		t.Fatal("pkg/adapter not found")
	}

	pkg := pkgs[0]
	forbidden := modulePath + "/pkg/dialect"

	for imp := range pkg.Imports {
		if imp == forbidden {
			t.Errorf("MICROKERNEL VIOLATION: pkg/adapter imports pkg/dialect\n" +
				"  The adapter registry must only know about core.Adapter and core.DialectConfig\n" +
				"  It must NOT import the heavy dialect framework\n" +
				"  Fix: Change Adapter.Dialect() to return *core.DialectConfig")
		}
	}
}

// TestDialectsDoNotImportParser ensures dialect definitions don't create
// circular dependency risk with the parser.
//
// This is a strict test - violations FAIL. Fix them by moving AST types to pkg/core.
func TestDialectsDoNotImportParser(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
	}

	pkgs, err := packages.Load(cfg, modulePath+"/pkg/dialects/...")
	if err != nil {
		t.Fatalf("failed to load pkg/dialects: %v", err)
	}

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			continue
		}

		relPath := strings.TrimPrefix(pkg.PkgPath, modulePath+"/")
		forbidden := modulePath + "/pkg/parser"

		for imp := range pkg.Imports {
			if imp == forbidden {
				t.Errorf("CIRCULAR DEPENDENCY RISK: %s imports pkg/parser\n"+
					"  Dialect definitions must not import the parser.\n"+
					"  Fix: Move required AST types (ListLiteral, StructLiteral, etc.) to pkg/core/ast_extensions.go",
					relPath)
			}
		}
	}
}

// findAllowedImports returns the allowed imports for a package path.
// It finds the most specific matching rule.
func findAllowedImports(pkgPath string) []string {
	var bestMatch string
	var allowed []string

	for prefix, imports := range architectureRules {
		if strings.HasPrefix(pkgPath, prefix) {
			if len(prefix) > len(bestMatch) {
				bestMatch = prefix
				allowed = imports
			}
		}
	}

	if bestMatch == "" {
		return nil
	}
	return allowed
}

// isAllowed checks if an import is in the allowed list.
// It uses prefix matching (pkg/dialects allows pkg/dialects/duckdb).
func isAllowed(imp string, allowed []string) bool {
	for _, prefix := range allowed {
		if strings.HasPrefix(imp, prefix) {
			return true
		}
	}
	return false
}
