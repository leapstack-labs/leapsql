//go:build governance

package core_test

import (
	"go/types"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// =============================================================================
// COHESION TEST - Core types must be shared by multiple packages
// =============================================================================

// TestGovernance_CoreCohesion verifies that types in pkg/core are genuinely
// shared across multiple packages. Single-use types should be moved to their
// sole consumer to maintain cohesion.
func TestGovernance_CoreCohesion(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedDeps,
	}
	pkgs, err := packages.Load(cfg, modulePath+"/...")
	if err != nil {
		t.Fatalf("Failed to load packages: %v", err)
	}

	// Find pkg/core and collect exported types
	coreDefs := make(map[types.Object]string)
	var corePkg *packages.Package

	for _, p := range pkgs {
		if p.PkgPath == modulePath+"/pkg/core" {
			corePkg = p
			scope := p.Types.Scope()
			for _, name := range scope.Names() {
				obj := scope.Lookup(name)
				if obj.Exported() {
					coreDefs[obj] = name
				}
			}
			break
		}
	}

	if corePkg == nil {
		t.Fatal("Could not find pkg/core")
	}

	// Count usages: CoreTypeName -> set of importing packages
	usageMap := make(map[string]map[string]bool)
	for _, name := range coreDefs {
		usageMap[name] = make(map[string]bool)
	}

	base := modulePath + "/"

	for _, p := range pkgs {
		// Skip core itself and test packages
		if p.PkgPath == corePkg.PkgPath || strings.HasSuffix(p.PkgPath, "_test") {
			continue
		}
		if p.TypesInfo == nil {
			continue
		}

		for _, info := range p.TypesInfo.Uses {
			if name, exists := coreDefs[info]; exists {
				importer := strings.TrimPrefix(p.PkgPath, base)
				usageMap[name][importer] = true
			}
		}
	}

	// Report violations
	for typeName, importers := range usageMap {
		if isCohesionAllowlisted(typeName) {
			continue
		}

		if len(importers) == 0 {
			t.Logf("WARNING: Unused Core Type: %s (consider deleting)", typeName)
		} else if len(importers) == 1 {
			var user string
			for k := range importers {
				user = k
			}
			t.Errorf("COHESION VIOLATION: 'core.%s' is used ONLY by '%s'.\n"+
				"   Fix: Move type from pkg/core to %s.",
				typeName, user, user)
		}
	}
}

// isCohesionAllowlisted returns true for types allowed to have single usage.
func isCohesionAllowlisted(name string) bool {
	allowlist := map[string]bool{
		"Adapter":       true, // Interface - implementations may be in one place
		"DialectConfig": true, // Config struct for extension point
	}
	return allowlist[name]
}

// =============================================================================
// PURITY TEST - No type alias re-exports from non-core packages
// =============================================================================

// TestGovernance_NoTypeAliasReexports ensures packages don't re-export core
// types as aliases. This prevents the pattern that was fixed in the
// "remove-parser-type-aliases" migration.
func TestGovernance_NoTypeAliasReexports(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedTypes,
	}
	pkgs, err := packages.Load(cfg, modulePath+"/pkg/...")
	if err != nil {
		t.Fatalf("Failed to load packages: %v", err)
	}

	// Packages that should NOT re-export core AST types as aliases
	// These patterns were the source of the problem fixed in the migration
	forbiddenAliasPatterns := map[string][]string{
		modulePath + "/pkg/parser": {
			// AST types that were previously aliased and removed
			"SelectStmt", "InsertStmt", "UpdateStmt", "DeleteStmt",
			"CreateTableStmt", "DropTableStmt", "AlterTableStmt",
			"Expr", "BinaryExpr", "UnaryExpr", "ColumnRef", "TableRef",
			"FunctionCall", "CaseExpr", "SubqueryExpr", "ExistsExpr",
			"LiteralString", "LiteralNumber", "LiteralBool", "LiteralNull",
			"InExpr", "BetweenExpr", "LikeExpr", "IsNullExpr",
			"CTE", "WithClause", "JoinClause", "OrderByItem", "WindowSpec",
			"Node", "Statement",
		},
	}

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			continue
		}

		forbidden, isForbiddenPkg := forbiddenAliasPatterns[pkg.PkgPath]
		if !isForbiddenPkg {
			continue
		}

		forbiddenSet := make(map[string]bool)
		for _, name := range forbidden {
			forbiddenSet[name] = true
		}

		scope := pkg.Types.Scope()
		for _, name := range scope.Names() {
			obj := scope.Lookup(name)
			if !obj.Exported() {
				continue
			}

			if typeName, ok := obj.(*types.TypeName); ok {
				if typeName.IsAlias() && forbiddenSet[name] {
					t.Errorf("PURITY VIOLATION: Package '%s' re-exports type alias '%s'.\n"+
						"   This pattern was deprecated in the AST Core Migration.\n"+
						"   Fix: Remove the alias. Consumers should use core.%s directly.",
						strings.TrimPrefix(pkg.PkgPath, modulePath+"/"), name, name)
				}
			}
		}
	}
}
