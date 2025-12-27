// Package lint provides a unified SQL and project linting framework.
//
// # Architecture
//
// The lint package follows a modular architecture with three layers:
//
//  1. Root package (pkg/lint/): Contains shared contracts, interfaces, and the unified registry
//  2. SQL subsystem (pkg/lint/sql/): SQL statement analysis with dialect-aware rules
//  3. Project subsystem (pkg/lint/project/): Project-level analysis (DAG, naming, architecture)
//
// # Rule Registration
//
// Rules are automatically registered via init() functions when their packages are imported:
//
//	// Import SQL rules
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules"
//
//	// Import project rules
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules"
//
// # Rule Categories
//
// SQL Rules (statement-level):
//   - AL (Aliasing): Rules about alias usage and naming
//   - AM (Ambiguous): Rules about ambiguous SQL constructs
//   - CV (Convention): Rules about SQL coding conventions
//   - RF (References): Rules about column and table references
//   - ST (Structure): Rules about SQL query structure
//
// Project Rules (architecture-level):
//   - PL (Lineage): Rules about data lineage and dependencies
//   - PM (Modeling): Rules about model structure and organization
//   - PS (Structure): Rules about project structure and naming
//
// # Using the Registry
//
// Query all registered rules:
//
//	rules := lint.AllRules()
//	sqlRules := lint.GetAllSQLRules()
//	projectRules := lint.GetAllProjectRules()
//
// Query rules by ID, group, or dialect:
//
//	rule, ok := lint.GetRuleByID("AM01")
//	sqlRules := lint.GetSQLRulesByDialect("postgres")
//	groupRules := lint.GetSQLRulesByGroup("ambiguous")
//
// # Configuration
//
// Use Config to control which rules are enabled and their severity:
//
//	config := lint.NewConfig()
//	config.Disable("AM01")
//	config.SetSeverity("CV05", core.SeverityError)
//	config.SetRuleOptions("AL06", map[string]any{"min_length": 3})
//
// # Creating Custom Rules
//
// For SQL rules, implement the SQLRule interface or use RuleDef:
//
//	var MyRule = sql.RuleDef{
//		ID:          "MY01",
//		Name:        "my.custom_rule",
//		Group:       "custom",
//		Description: "My custom rule description",
//		Severity:    core.SeverityWarning,
//		Check:       checkMyRule,
//	}
//
//	func init() {
//		sql.Register(MyRule)
//	}
//
// For project rules, implement the ProjectRule interface in the project package.
package lint
