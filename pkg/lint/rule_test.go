package lint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSQLRule implements SQLRule for testing
type mockSQLRule struct {
	id          string
	name        string
	group       string
	description string
	severity    Severity
	configKeys  []string
	dialects    []string
}

func (m *mockSQLRule) ID() string                { return m.id }
func (m *mockSQLRule) Name() string              { return m.name }
func (m *mockSQLRule) Group() string             { return m.group }
func (m *mockSQLRule) Description() string       { return m.description }
func (m *mockSQLRule) DefaultSeverity() Severity { return m.severity }
func (m *mockSQLRule) ConfigKeys() []string      { return m.configKeys }
func (m *mockSQLRule) Dialects() []string        { return m.dialects }

// Documentation methods (return empty for mocks)
func (m *mockSQLRule) Rationale() string   { return "" }
func (m *mockSQLRule) BadExample() string  { return "" }
func (m *mockSQLRule) GoodExample() string { return "" }
func (m *mockSQLRule) Fix() string         { return "" }

func (m *mockSQLRule) CheckSQL(_ any, _ DialectInfo, _ map[string]any) []Diagnostic {
	return nil
}

// mockProjectRule implements ProjectRule for testing
type mockProjectRule struct {
	id          string
	name        string
	group       string
	description string
	severity    Severity
	configKeys  []string
}

func (m *mockProjectRule) ID() string                { return m.id }
func (m *mockProjectRule) Name() string              { return m.name }
func (m *mockProjectRule) Group() string             { return m.group }
func (m *mockProjectRule) Description() string       { return m.description }
func (m *mockProjectRule) DefaultSeverity() Severity { return m.severity }
func (m *mockProjectRule) ConfigKeys() []string      { return m.configKeys }

// Documentation methods (return empty for mocks)
func (m *mockProjectRule) Rationale() string   { return "" }
func (m *mockProjectRule) BadExample() string  { return "" }
func (m *mockProjectRule) GoodExample() string { return "" }
func (m *mockProjectRule) Fix() string         { return "" }

func (m *mockProjectRule) CheckProject(_ ProjectContext) []Diagnostic {
	return nil
}

func TestSQLRuleInterface(t *testing.T) {
	rule := &mockSQLRule{
		id:          "TST01",
		name:        "test-rule",
		group:       "testing",
		description: "A test SQL rule",
		severity:    SeverityWarning,
		configKeys:  []string{"max_count"},
		dialects:    []string{"postgres", "duckdb"},
	}

	// Verify it implements Rule interface
	var _ Rule = rule

	// Verify it implements SQLRule interface
	var _ SQLRule = rule

	// Test all interface methods
	assert.Equal(t, "TST01", rule.ID())
	assert.Equal(t, "test-rule", rule.Name())
	assert.Equal(t, "testing", rule.Group())
	assert.Equal(t, "A test SQL rule", rule.Description())
	assert.Equal(t, SeverityWarning, rule.DefaultSeverity())
	assert.Equal(t, []string{"max_count"}, rule.ConfigKeys())
	assert.Equal(t, []string{"postgres", "duckdb"}, rule.Dialects())

	// Test CheckSQL returns empty diagnostics
	diags := rule.CheckSQL(nil, nil, nil)
	assert.Empty(t, diags)
}

func TestProjectRuleInterface(t *testing.T) {
	rule := &mockProjectRule{
		id:          "PM01",
		name:        "project-rule",
		group:       "modeling",
		description: "A test project rule",
		severity:    SeverityError,
		configKeys:  []string{"threshold"},
	}

	// Verify it implements Rule interface
	var _ Rule = rule

	// Verify it implements ProjectRule interface
	var _ ProjectRule = rule

	// Test all interface methods
	assert.Equal(t, "PM01", rule.ID())
	assert.Equal(t, "project-rule", rule.Name())
	assert.Equal(t, "modeling", rule.Group())
	assert.Equal(t, "A test project rule", rule.Description())
	assert.Equal(t, SeverityError, rule.DefaultSeverity())
	assert.Equal(t, []string{"threshold"}, rule.ConfigKeys())

	// Test CheckProject returns empty diagnostics
	diags := rule.CheckProject(nil)
	assert.Empty(t, diags)
}

func TestGetRuleInfo_SQLRule(t *testing.T) {
	rule := &mockSQLRule{
		id:          "TST01",
		name:        "test-rule",
		group:       "testing",
		description: "A test SQL rule",
		severity:    SeverityWarning,
		configKeys:  []string{"opt1"},
		dialects:    []string{"postgres"},
	}

	info := GetRuleInfo(rule)

	assert.Equal(t, "TST01", info.ID)
	assert.Equal(t, "test-rule", info.Name)
	assert.Equal(t, "testing", info.Group)
	assert.Equal(t, "A test SQL rule", info.Description)
	assert.Equal(t, SeverityWarning, info.DefaultSeverity)
	assert.Equal(t, []string{"opt1"}, info.ConfigKeys)
	assert.Equal(t, []string{"postgres"}, info.Dialects)
	assert.Equal(t, "sql", info.Type)
}

func TestGetRuleInfo_ProjectRule(t *testing.T) {
	rule := &mockProjectRule{
		id:          "PM01",
		name:        "project-rule",
		group:       "modeling",
		description: "A test project rule",
		severity:    SeverityError,
		configKeys:  []string{"threshold"},
	}

	info := GetRuleInfo(rule)

	assert.Equal(t, "PM01", info.ID)
	assert.Equal(t, "project-rule", info.Name)
	assert.Equal(t, "modeling", info.Group)
	assert.Equal(t, "A test project rule", info.Description)
	assert.Equal(t, SeverityError, info.DefaultSeverity)
	assert.Equal(t, []string{"threshold"}, info.ConfigKeys)
	assert.Nil(t, info.Dialects) // Project rules don't have dialects
	assert.Equal(t, "project", info.Type)
}

func TestWrapRuleDef(t *testing.T) {
	def := RuleDef{
		ID:          "WRAP01",
		Name:        "wrapped-rule",
		Group:       "wrapper",
		Description: "A wrapped rule",
		Severity:    SeverityInfo,
		ConfigKeys:  []string{"key1", "key2"},
		Dialects:    []string{"ansi"},
		Check: func(_ any, _ DialectInfo, _ map[string]any) []Diagnostic {
			return []Diagnostic{{RuleID: "WRAP01", Message: "test"}}
		},
	}

	wrapped := WrapRuleDef(def)

	// Verify it implements SQLRule
	var _ SQLRule = wrapped //nolint:staticcheck // explicit type check for interface compliance

	// Test all methods
	assert.Equal(t, "WRAP01", wrapped.ID())
	assert.Equal(t, "wrapped-rule", wrapped.Name())
	assert.Equal(t, "wrapper", wrapped.Group())
	assert.Equal(t, "A wrapped rule", wrapped.Description())
	assert.Equal(t, SeverityInfo, wrapped.DefaultSeverity())
	assert.Equal(t, []string{"key1", "key2"}, wrapped.ConfigKeys())
	assert.Equal(t, []string{"ansi"}, wrapped.Dialects())

	// Test CheckSQL delegates to the wrapped function
	diags := wrapped.CheckSQL(nil, nil, nil)
	require.Len(t, diags, 1)
	assert.Equal(t, "WRAP01", diags[0].RuleID)
	assert.Equal(t, "test", diags[0].Message)
}

func TestWrapRuleDef_Unwrap(t *testing.T) {
	def := RuleDef{
		ID:   "UNWRAP01",
		Name: "unwrap-test",
	}

	wrapped := WrapRuleDef(def)

	// Cast to access Unwrap method
	w, ok := wrapped.(*wrappedRuleDef)
	require.True(t, ok)

	unwrapped := w.Unwrap()
	assert.Equal(t, "UNWRAP01", unwrapped.ID)
	assert.Equal(t, "unwrap-test", unwrapped.Name)
}

func TestUnifiedRegistry_SQLRules(t *testing.T) {
	// Clear registry before test
	Clear()

	rule := &mockSQLRule{
		id:    "REG01",
		name:  "registry-test",
		group: "testing",
	}

	RegisterSQLRule(rule)

	// Test GetAllSQLRules
	rules := GetAllSQLRules()
	require.Len(t, rules, 1)
	assert.Equal(t, "REG01", rules[0].ID())

	// Test GetSQLRuleByID
	found, ok := GetSQLRuleByID("REG01")
	require.True(t, ok)
	assert.Equal(t, "REG01", found.ID())

	// Test GetRuleByID (generic)
	genericFound, ok := GetRuleByID("REG01")
	require.True(t, ok)
	assert.Equal(t, "REG01", genericFound.ID())

	// Test CountSQLRules
	assert.Equal(t, 1, CountSQLRules())

	// Test not found
	_, ok = GetSQLRuleByID("NOTEXIST")
	assert.False(t, ok)
}

func TestUnifiedRegistry_ProjectRules(t *testing.T) {
	// Clear registry before test
	Clear()

	rule := &mockProjectRule{
		id:    "PREG01",
		name:  "project-registry-test",
		group: "modeling",
	}

	RegisterProjectRule(rule)

	// Test GetAllProjectRules
	rules := GetAllProjectRules()
	require.Len(t, rules, 1)
	assert.Equal(t, "PREG01", rules[0].ID())

	// Test GetProjectRuleByID
	found, ok := GetProjectRuleByID("PREG01")
	require.True(t, ok)
	assert.Equal(t, "PREG01", found.ID())

	// Test GetRuleByID (generic)
	genericFound, ok := GetRuleByID("PREG01")
	require.True(t, ok)
	assert.Equal(t, "PREG01", genericFound.ID())

	// Test CountProjectRules
	assert.Equal(t, 1, CountProjectRules())
}

func TestAllRules(t *testing.T) {
	// Clear registry before test
	Clear()

	sqlRule := &mockSQLRule{
		id:       "SQL01",
		name:     "sql-test",
		group:    "sql",
		severity: SeverityWarning,
		dialects: []string{"postgres"},
	}

	projectRule := &mockProjectRule{
		id:       "PRJ01",
		name:     "project-test",
		group:    "project",
		severity: SeverityError,
	}

	RegisterSQLRule(sqlRule)
	RegisterProjectRule(projectRule)

	// Test AllRules returns both
	allRules := AllRules()
	require.Len(t, allRules, 2)

	// Find by type
	var sqlInfo, projectInfo *RuleInfo
	for i := range allRules {
		switch allRules[i].Type {
		case "sql":
			sqlInfo = &allRules[i]
		case "project":
			projectInfo = &allRules[i]
		}
	}

	require.NotNil(t, sqlInfo)
	require.NotNil(t, projectInfo)

	assert.Equal(t, "SQL01", sqlInfo.ID)
	assert.Equal(t, "sql", sqlInfo.Type)
	assert.Equal(t, []string{"postgres"}, sqlInfo.Dialects)

	assert.Equal(t, "PRJ01", projectInfo.ID)
	assert.Equal(t, "project", projectInfo.Type)
	assert.Nil(t, projectInfo.Dialects)
}

func TestGetSQLRulesByDialect(t *testing.T) {
	// Clear registry before test
	Clear()

	postgresRule := &mockSQLRule{
		id:       "PG01",
		dialects: []string{"postgres"},
	}
	duckdbRule := &mockSQLRule{
		id:       "DDB01",
		dialects: []string{"duckdb"},
	}
	universalRule := &mockSQLRule{
		id:       "UNI01",
		dialects: nil, // applies to all
	}

	RegisterSQLRule(postgresRule)
	RegisterSQLRule(duckdbRule)
	RegisterSQLRule(universalRule)

	// Test postgres filter
	pgRules := GetSQLRulesByDialect("postgres")
	require.Len(t, pgRules, 2) // PG01 + UNI01
	ids := make(map[string]bool)
	for _, r := range pgRules {
		ids[r.ID()] = true
	}
	assert.True(t, ids["PG01"])
	assert.True(t, ids["UNI01"])
	assert.False(t, ids["DDB01"])

	// Test duckdb filter
	ddbRules := GetSQLRulesByDialect("duckdb")
	require.Len(t, ddbRules, 2) // DDB01 + UNI01
}

func TestGetSQLRulesByGroup(t *testing.T) {
	// Clear registry before test
	Clear()

	rule1 := &mockSQLRule{id: "GRP01", group: "ambiguous"}
	rule2 := &mockSQLRule{id: "GRP02", group: "ambiguous"}
	rule3 := &mockSQLRule{id: "GRP03", group: "structure"}

	RegisterSQLRule(rule1)
	RegisterSQLRule(rule2)
	RegisterSQLRule(rule3)

	ambiguousRules := GetSQLRulesByGroup("ambiguous")
	require.Len(t, ambiguousRules, 2)

	structureRules := GetSQLRulesByGroup("structure")
	require.Len(t, structureRules, 1)
	assert.Equal(t, "GRP03", structureRules[0].ID())
}

func TestGetProjectRulesByGroup(t *testing.T) {
	// Clear registry before test
	Clear()

	rule1 := &mockProjectRule{id: "PGRP01", group: "modeling"}
	rule2 := &mockProjectRule{id: "PGRP02", group: "modeling"}
	rule3 := &mockProjectRule{id: "PGRP03", group: "lineage"}

	RegisterProjectRule(rule1)
	RegisterProjectRule(rule2)
	RegisterProjectRule(rule3)

	modelingRules := GetProjectRulesByGroup("modeling")
	require.Len(t, modelingRules, 2)

	lineageRules := GetProjectRulesByGroup("lineage")
	require.Len(t, lineageRules, 1)
	assert.Equal(t, "PGRP03", lineageRules[0].ID())
}
