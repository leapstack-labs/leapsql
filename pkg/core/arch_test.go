package core_test

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const modulePath = "github.com/leapstack-labs/leapsql"

// =============================================================================
// NO TYPE ALIAS RE-EXPORTS TEST
// =============================================================================

// TestArchitecture_NoTypeAliasReexports ensures no package re-exports core types via aliases.
// This prevents "type smuggling" where multiple packages appear to provide the same type.
func TestArchitecture_NoTypeAliasReexports(t *testing.T) {
	projectRoot := findProjectRoot(t)

	dirsToScan := []string{
		filepath.Join(projectRoot, "pkg"),
		filepath.Join(projectRoot, "internal"),
	}

	for _, root := range dirsToScan {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}

			// Skip pkg/core itself - it's the canonical source
			relPath, _ := filepath.Rel(projectRoot, path)
			if relPath == "pkg/core" || strings.HasPrefix(relPath, "pkg/core/") {
				return nil
			}

			checkNoCoreReexports(t, path, relPath)
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to walk directory %s: %v", root, err)
		}
	}
}

// checkNoCoreReexports scans a directory for forbidden core type aliases and re-exports.
func checkNoCoreReexports(t *testing.T, dir, relPath string) {
	t.Helper()

	fset := gotoken.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		// Include all .go files (including _test.go and generated)
		return strings.HasSuffix(fi.Name(), ".go")
	}, parser.ParseComments)

	if err != nil {
		// Skip directories that can't be parsed (e.g., no Go files)
		return
	}

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			// Find local name for core package in this file's imports
			coreLocalName := findCoreImportName(file)
			if coreLocalName == "" {
				continue // File doesn't import core, skip
			}

			ast.Inspect(file, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.TypeSpec:
					// Catch: type T = core.T
					if x.Assign.IsValid() && isCoreReference(x.Type, coreLocalName) {
						typeName := extractTypeName(x.Type)
						t.Errorf("ALIAS VIOLATION: %s\n"+
							"    Type alias '%s = %s.%s' re-exports a core type.\n"+
							"    pkg/core is the canonical source for domain types.\n"+
							"    Fix: Delete this alias and use 'core.%s' directly in consuming packages.",
							fset.Position(x.Pos()), x.Name.Name, coreLocalName, typeName, typeName)
					}

				case *ast.ValueSpec:
					// Catch: var V = core.V or const C = core.C
					for i, val := range x.Values {
						if isCoreReference(val, coreLocalName) {
							varName := ""
							if i < len(x.Names) {
								varName = x.Names[i].Name
							}
							typeName := extractTypeName(val)
							t.Errorf("ALIAS VIOLATION: %s\n"+
								"    Re-export '%s = %s.%s' smuggles a core value.\n"+
								"    pkg/core is the canonical source for domain values.\n"+
								"    Fix: Delete this re-export and use 'core.%s' directly.",
								fset.Position(val.Pos()), varName, coreLocalName, typeName, typeName)
						}
					}
				}
				return true
			})
		}
	}
}

// findCoreImportName returns the local name used for the core package in this file.
// Returns "core" for `import "...pkg/core"` or the alias for `import c "...pkg/core"`.
// Returns "" if core is not imported.
func findCoreImportName(file *ast.File) string {
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if strings.HasSuffix(path, "pkg/core") {
			if imp.Name != nil {
				return imp.Name.Name
			}
			return "core" // Default import name
		}
	}
	return ""
}

// isCoreReference checks if an expression is a selector like `core.X` (or `c.X` if aliased).
func isCoreReference(expr ast.Expr, coreLocalName string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == coreLocalName
}

// extractTypeName extracts the type/value name from a core.X selector expression.
func extractTypeName(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return "?"
	}
	return sel.Sel.Name
}

// findProjectRoot locates the project root by looking for go.mod.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	// Start from the test file location and walk up
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Could not find project root (no go.mod found)")
		}
		dir = parent
	}
}

// =============================================================================
// PKG LAYER CLASSIFICATION
// =============================================================================

// namespacePackages are directories that group distinct components (plugins).
// Subpackages under these are treated as separate components.
// e.g., pkg/dialects/duckdb and pkg/dialects/postgres are different components.
var namespacePackages = map[string]bool{
	"pkg/dialects": true,
	"pkg/adapters": true,
}

// getTopLevelComponent returns the logical component for a pkg/* path.
// For namespace packages (like pkg/dialects), the component is one level deeper.
// e.g., "pkg/lint/sql/rules" -> "pkg/lint"
//
//	"pkg/dialects/duckdb/types" -> "pkg/dialects/duckdb"
func getTopLevelComponent(path string) string {
	if !strings.HasPrefix(path, "pkg/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "pkg/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 {
		return path
	}
	candidate := "pkg/" + parts[0]
	if namespacePackages[candidate] {
		if len(parts) > 1 {
			return candidate + "/" + parts[1]
		}
		return candidate
	}
	return candidate
}

// isSameComponent checks if two paths belong to the same logical component.
func isSameComponent(path1, path2 string) bool {
	c1 := getTopLevelComponent(path1)
	c2 := getTopLevelComponent(path2)
	return c1 != "" && c1 == c2
}

// isFoundation returns true for hub packages that any component can import.
func isFoundation(path string) bool {
	foundationPkgs := map[string]bool{
		"pkg/core":    true,
		"pkg/token":   true,
		"pkg/spi":     true,
		"pkg/adapter": true,
	}
	if foundationPkgs[path] {
		return true
	}
	for f := range foundationPkgs {
		if strings.HasPrefix(path, f+"/") {
			return true
		}
	}
	return false
}

// isInfrastructure returns true for adapter implementations (outer layer).
func isInfrastructure(path string) bool {
	return strings.HasPrefix(path, "pkg/adapters/")
}

// isPkgComponent returns true for pure logic packages (spokes) in pkg/.
func isPkgComponent(path string) bool {
	return strings.HasPrefix(path, "pkg/") &&
		!isFoundation(path) &&
		!isInfrastructure(path)
}

// =============================================================================
// INTERNAL LAYER CLASSIFICATION
// =============================================================================

// isEntrypoint returns true for top-level application entry points.
func isEntrypoint(path string) bool {
	entrypoints := map[string]bool{
		"internal/cli":          true,
		"internal/cli/commands": true,
		"internal/cli/output":   true,
		"internal/cli/testutil": true,
	}
	return entrypoints[path]
}

// isOrchestrator returns true for business logic coordinators.
func isOrchestrator(path string) bool {
	orchestrators := map[string]bool{
		"internal/engine":   true,
		"internal/lsp":      true,
		"internal/provider": true,
		"internal/docs":     true,
	}
	return orchestrators[path]
}

// =============================================================================
// UTILITY TIER CLASSIFICATION
// =============================================================================

// isSharedUtility returns true for foundational utility packages that provide
// core mechanisms used across multiple entry points (CLI, LSP, Engine).
// These packages must remain decoupled from application-specific concerns.
func isSharedUtility(path string) bool {
	shared := map[string]bool{
		"internal/config":        true, // Shared config loading/defaults
		"internal/loader":        true, // Model/seed loading
		"internal/lineage":       true, // SQL lineage extraction
		"internal/state":         true, // State persistence
		"internal/state/sqlcgen": true, // Generated SQL code
		"internal/dag":           true, // Dependency graph
		"internal/template":      true, // Template rendering
		"internal/starlark":      true, // Starlark execution
		"internal/macro":         true, // Macro definitions
		"internal/registry":      true, // Generic registry
		"internal/testutil":      true, // Test utilities
	}
	return shared[path]
}

// isContextualUtility returns true for application entry-point specific
// utilities that extend shared utilities with context (CLI flags, etc.).
// Contextual utilities are allowed to import Shared utilities.
func isContextualUtility(path string) bool {
	contextual := map[string]bool{
		"internal/cli/config": true, // CLI-specific config (flags, env vars)
	}
	return contextual[path]
}

// isUtility returns true for any utility package (shared or contextual).
func isUtility(path string) bool {
	return isSharedUtility(path) || isContextualUtility(path)
}

// isAllowedUtilityException returns true for explicitly allowed utility-to-utility imports.
func isAllowedUtilityException(from, to string) bool {
	exceptions := map[string]map[string]bool{
		// template -> starlark: Composition - template system is Starlark-based
		"internal/template": {"internal/starlark": true},
	}
	if allowed, exists := exceptions[from]; exists {
		return allowed[to]
	}
	return false
}

// =============================================================================
// STAR TOPOLOGY TEST - pkg/* Components
// =============================================================================

// TestArchitecture_StarTopology enforces that pkg components (spokes) only import
// foundation packages (hub), never each other. This dynamically forces shared
// types to migrate to pkg/core.
func TestArchitecture_StarTopology(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(cfg, modulePath+"/pkg/...")
	if err != nil {
		t.Fatalf("Failed to load packages: %v", err)
	}

	base := modulePath + "/"

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			continue
		}

		pkgPath := strings.TrimPrefix(pkg.PkgPath, base)

		if strings.HasSuffix(pkgPath, "_test") {
			continue
		}

		switch {
		case isFoundation(pkgPath):
			checkFoundationImports(t, pkg, pkgPath, base)
		case isInfrastructure(pkgPath):
			checkInfrastructureImports(t, pkg, pkgPath, base)
		case isPkgComponent(pkgPath):
			checkPkgComponentImports(t, pkg, pkgPath, base)
		}
	}
}

// checkFoundationImports ensures foundation packages follow strict rules.
func checkFoundationImports(t *testing.T, pkg *packages.Package, pkgPath, base string) {
	t.Helper()

	foundationRules := map[string][]string{
		"pkg/token":   {},
		"pkg/core":    {"pkg/token"},
		"pkg/spi":     {"pkg/core", "pkg/token"},
		"pkg/adapter": {"pkg/core"},
	}

	allowed := foundationRules[pkgPath]
	if allowed == nil {
		for parent, rules := range foundationRules {
			if strings.HasPrefix(pkgPath, parent+"/") {
				allowed = rules
				break
			}
		}
	}

	for imp := range pkg.Imports {
		if !strings.HasPrefix(imp, base) {
			continue
		}
		depPath := strings.TrimPrefix(imp, base)

		if strings.HasPrefix(depPath, pkgPath) || strings.HasPrefix(pkgPath, depPath) {
			continue
		}

		if !isAllowedImport(depPath, allowed) {
			t.Errorf("FOUNDATION VIOLATION: '%s' imports '%s'.\n"+
				"   Foundation packages have strict import rules.\n"+
				"   Allowed: %v",
				pkgPath, depPath, allowed)
		}
	}
}

// checkPkgComponentImports enforces Star Topology - no horizontal dependencies.
func checkPkgComponentImports(t *testing.T, pkg *packages.Package, pkgPath, base string) {
	t.Helper()

	for imp := range pkg.Imports {
		if !strings.HasPrefix(imp, base) {
			continue
		}
		depPath := strings.TrimPrefix(imp, base)

		// Self-imports OK (same package or subpackages)
		if strings.HasPrefix(depPath, pkgPath) || strings.HasPrefix(pkgPath, depPath) {
			continue
		}

		// Same component OK (e.g., pkg/lint/sql/rules can import pkg/lint/sql/internal/ast)
		if isSameComponent(pkgPath, depPath) {
			continue
		}

		// Foundation OK
		if isFoundation(depPath) {
			continue
		}

		// Internal packages NOT OK
		if strings.HasPrefix(depPath, "internal/") {
			t.Errorf("BOUNDARY VIOLATION: Component '%s' imports internal '%s'.\n"+
				"   pkg/* must never import internal/*.",
				pkgPath, depPath)
			continue
		}

		// Infrastructure NOT OK
		if isInfrastructure(depPath) {
			t.Errorf("LAYER VIOLATION: Component '%s' imports infrastructure '%s'.\n"+
				"   Components cannot depend on adapter implementations.",
				pkgPath, depPath)
			continue
		}

		// Another component - HORIZONTAL VIOLATION
		if isPkgComponent(depPath) {
			t.Errorf("HORIZONTAL VIOLATION: Component '%s' imports peer component '%s'.\n"+
				"   Components (spokes) cannot import each other.\n"+
				"   Fix: Move shared types/contracts from '%s' to pkg/core.",
				pkgPath, depPath, depPath)
		}
	}
}

// checkInfrastructureImports enforces microkernel rules for adapters.
func checkInfrastructureImports(t *testing.T, pkg *packages.Package, pkgPath, base string) {
	t.Helper()

	forbiddenForAdapters := map[string]bool{
		"pkg/parser": true,
		"pkg/format": true,
		"pkg/lint":   true,
	}

	for imp := range pkg.Imports {
		if !strings.HasPrefix(imp, base) {
			continue
		}
		depPath := strings.TrimPrefix(imp, base)

		if strings.HasPrefix(depPath, pkgPath) || strings.HasPrefix(pkgPath, depPath) {
			continue
		}

		for forbidden := range forbiddenForAdapters {
			if depPath == forbidden || strings.HasPrefix(depPath, forbidden+"/") {
				t.Errorf("MICROKERNEL VIOLATION: Adapter '%s' imports heavy library '%s'.\n"+
					"   Adapters should only use pkg/core interfaces and pkg/dialects config.\n"+
					"   Fix: Move required functionality to pkg/core or use dependency injection.",
					pkgPath, depPath)
			}
		}
	}
}

// =============================================================================
// INTERNAL TIERS TEST
// =============================================================================

// TestArchitecture_InternalTiers enforces the 3-layer internal architecture:
// Entrypoints -> Orchestrators -> Utilities
func TestArchitecture_InternalTiers(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(cfg, modulePath+"/internal/...")
	if err != nil {
		t.Fatalf("Failed to load packages: %v", err)
	}

	base := modulePath + "/"

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			continue
		}

		pkgPath := strings.TrimPrefix(pkg.PkgPath, base)

		if strings.HasSuffix(pkgPath, "_test") {
			continue
		}

		for imp := range pkg.Imports {
			if !strings.HasPrefix(imp, base) {
				continue
			}
			depPath := strings.TrimPrefix(imp, base)

			// Self-imports OK (including subpackages)
			if strings.HasPrefix(depPath, pkgPath+"/") || strings.HasPrefix(pkgPath, depPath+"/") {
				continue
			}
			if depPath == pkgPath {
				continue
			}

			// pkg/* imports always OK for internal packages
			if strings.HasPrefix(depPath, "pkg/") {
				continue
			}

			// Check internal -> internal dependencies based on tiers
			checkInternalTierImport(t, pkgPath, depPath)
		}
	}
}

func checkInternalTierImport(t *testing.T, from, to string) {
	t.Helper()

	// Entrypoints can import anything
	if isEntrypoint(from) {
		return
	}

	// Orchestrators can import utilities and other orchestrators
	if isOrchestrator(from) {
		if isEntrypoint(to) {
			t.Errorf("INTERNAL LAYER VIOLATION: Orchestrator '%s' imports Entrypoint '%s'.\n"+
				"   Orchestrators cannot depend on CLI/entrypoint code.\n"+
				"   Fix: Extract shared logic to a utility or pkg/*.",
				from, to)
		}
		return
	}

	// Utilities have strict rules
	if isUtility(from) {
		// Contextual utilities MAY import shared utilities
		// This is the "Three-Tier" model: Core → Shared → Contextual
		if isContextualUtility(from) && isSharedUtility(to) {
			return // Allowed: cli/config → config
		}

		// Check for allowed exceptions
		if isAllowedUtilityException(from, to) {
			return
		}

		if isEntrypoint(to) {
			t.Errorf("INTERNAL LAYER VIOLATION: Utility '%s' imports Entrypoint '%s'.\n"+
				"   Utilities cannot depend on CLI/entrypoint code.",
				from, to)
			return
		}

		if isOrchestrator(to) {
			t.Errorf("INTERNAL LAYER VIOLATION: Utility '%s' imports Orchestrator '%s'.\n"+
				"   Utilities cannot depend on orchestrators.\n"+
				"   Fix: Move orchestration logic to internal/engine.",
				from, to)
			return
		}

		if isUtility(to) {
			t.Errorf("INTERNAL LAYER VIOLATION: Utility '%s' imports peer Utility '%s'.\n"+
				"   Utilities cannot import other utilities (horizontal dependency).\n"+
				"   Fix: Move orchestration logic to internal/engine, or extract shared code to pkg/*.",
				from, to)
			return
		}
	}
}

// =============================================================================
// FOCUSED TESTS
// =============================================================================

// TestArchitecture_CoreOnlyImportsToken is a focused test for the most critical rule.
func TestArchitecture_CoreOnlyImportsToken(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(cfg, modulePath+"/pkg/core")
	if err != nil {
		t.Fatalf("Failed to load pkg/core: %v", err)
	}

	if len(pkgs) == 0 {
		t.Fatal("pkg/core not found")
	}

	pkg := pkgs[0]
	base := modulePath + "/"
	allowed := modulePath + "/pkg/token"

	for imp := range pkg.Imports {
		if !strings.HasPrefix(imp, base) {
			continue
		}
		if imp != allowed {
			t.Errorf("GOLDEN RULE VIOLATION: pkg/core imports %s.\n"+
				"   pkg/core may ONLY import pkg/token (and stdlib).\n"+
				"   This is the foundation of the architecture.",
				strings.TrimPrefix(imp, base))
		}
	}
}

// TestArchitecture_PkgDoesNotImportInternal ensures libraries don't depend on application code.
func TestArchitecture_PkgDoesNotImportInternal(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(cfg, modulePath+"/pkg/...")
	if err != nil {
		t.Fatalf("Failed to load packages: %v", err)
	}

	base := modulePath + "/"

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			continue
		}

		pkgPath := strings.TrimPrefix(pkg.PkgPath, base)

		for imp := range pkg.Imports {
			if strings.Contains(imp, modulePath+"/internal/") {
				t.Errorf("BOUNDARY VIOLATION: '%s' imports internal package '%s'.\n"+
					"   pkg/* must never import internal/*.\n"+
					"   Fix: Move shared code to pkg/core or use interfaces.",
					pkgPath, strings.TrimPrefix(imp, base))
			}
		}
	}
}

// =============================================================================
// HELPERS
// =============================================================================

func isAllowedImport(imp string, allowed []string) bool {
	for _, prefix := range allowed {
		if imp == prefix || strings.HasPrefix(imp, prefix+"/") {
			return true
		}
	}
	return false
}
