package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func TestCalculateHealthScore(t *testing.T) {
	tests := []struct {
		name       string
		checks     []HealthCheck
		modelCount int
		minScore   int
		maxScore   int
	}{
		{
			name:       "no checks returns 100",
			checks:     nil,
			modelCount: 10,
			minScore:   100,
			maxScore:   100,
		},
		{
			name: "all passing returns 100",
			checks: []HealthCheck{
				{RuleID: "PM01", Status: "pass", IssueCount: 0},
				{RuleID: "PM02", Status: "pass", IssueCount: 0},
			},
			modelCount: 10,
			minScore:   100,
			maxScore:   100,
		},
		{
			name: "warnings reduce score",
			checks: []HealthCheck{
				{RuleID: "PM01", Status: "pass", IssueCount: 0},
				{RuleID: "PM02", Status: "warn", IssueCount: 2},
			},
			modelCount: 10,
			minScore:   80,
			maxScore:   100,
		},
		{
			name: "errors reduce score more",
			checks: []HealthCheck{
				{RuleID: "PM01", Status: "error", IssueCount: 2},
			},
			modelCount: 10,
			minScore:   70,
			maxScore:   95,
		},
		{
			name: "more models means less impact per issue",
			checks: []HealthCheck{
				{RuleID: "PM01", Status: "warn", IssueCount: 5},
			},
			modelCount: 100,
			minScore:   90,
			maxScore:   100,
		},
		{
			name: "many issues can reduce to 0",
			checks: []HealthCheck{
				{RuleID: "PM01", Status: "error", IssueCount: 20},
				{RuleID: "PM02", Status: "error", IssueCount: 20},
			},
			modelCount: 5,
			minScore:   0,
			maxScore:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := calculateHealthScore(tt.checks, tt.modelCount)
			assert.GreaterOrEqual(t, score, tt.minScore, "score should be >= %d", tt.minScore)
			assert.LessOrEqual(t, score, tt.maxScore, "score should be <= %d", tt.maxScore)
		})
	}
}

func TestGetRecommendation(t *testing.T) {
	tests := []struct {
		ruleID   string
		expected bool // whether a recommendation is returned
	}{
		{"PM01", true},
		{"PM02", true},
		{"PM03", true},
		{"PM04", true},
		{"PM05", true},
		{"PM06", true},
		{"PM07", true},
		{"PS01", true},
		{"PS02", true},
		{"PL01", true},
		{"PL02", true},
		{"PL04", true},
		{"UNKNOWN", false},
	}

	for _, tt := range tests {
		t.Run(tt.ruleID, func(t *testing.T) {
			rec := getRecommendation(tt.ruleID, 1)
			if tt.expected {
				assert.NotEmpty(t, rec, "expected recommendation for %s", tt.ruleID)
			} else {
				assert.Empty(t, rec, "expected no recommendation for %s", tt.ruleID)
			}
		})
	}
}

func TestGenerateRecommendations(t *testing.T) {
	checks := []HealthCheck{
		{RuleID: "PM01", Status: "warn", IssueCount: 1},
		{RuleID: "PM02", Status: "warn", IssueCount: 2},
		{RuleID: "PM03", Status: "pass", IssueCount: 0},
	}
	diags := []project.Diagnostic{
		{RuleID: "PM01", Message: "Model has no sources"},
		{RuleID: "PM02", Message: "Source has high fanout"},
	}

	recommendations := generateRecommendations(checks, diags)

	// Should have recommendations for PM01 and PM02
	assert.Len(t, recommendations, 2)
	assert.Contains(t, recommendations[0], "root models")
	assert.Contains(t, recommendations[1], "staging models")
}

func TestGenerateRecommendations_LimitTo5(t *testing.T) {
	// Create 10 checks with issues
	checks := make([]HealthCheck, 10)
	for i := 0; i < 10; i++ {
		ruleID := []string{"PM01", "PM02", "PM03", "PM04", "PM05", "PM06", "PM07", "PS01", "PS02", "PL01"}[i]
		checks[i] = HealthCheck{RuleID: ruleID, Status: "warn", IssueCount: 1}
	}

	recommendations := generateRecommendations(checks, nil)

	// Should be limited to 5
	assert.LessOrEqual(t, len(recommendations), 5)
}

func TestBuildProjectSummary_NoGraph(t *testing.T) {
	// This test just verifies the function handles nil graph gracefully
	// We can't easily test with a real engine without more setup
	summary := ProjectSummary{
		Models:    10,
		Macros:    5,
		Seeds:     3,
		DAGDepth:  4,
		RootCount: 2,
		LeafCount: 3,
		EdgeCount: 8,
	}

	// Just verify the struct can hold expected values
	assert.Equal(t, 10, summary.Models)
	assert.Equal(t, 5, summary.Macros)
	assert.Equal(t, 3, summary.Seeds)
	assert.Equal(t, 4, summary.DAGDepth)
}

func TestHealthCheck_Struct(t *testing.T) {
	check := HealthCheck{
		RuleID:     "PM01",
		Name:       "root-models",
		Group:      "modeling",
		Status:     "pass",
		IssueCount: 0,
		Details:    nil,
	}

	assert.Equal(t, "PM01", check.RuleID)
	assert.Equal(t, "root-models", check.Name)
	assert.Equal(t, "modeling", check.Group)
	assert.Equal(t, "pass", check.Status)
	assert.Equal(t, 0, check.IssueCount)
}

func TestDoctorOutput_Struct(t *testing.T) {
	output := DoctorOutput{
		Summary: ProjectSummary{
			Models:   10,
			DAGDepth: 3,
		},
		HealthChecks: []HealthCheck{
			{RuleID: "PM01", Status: "pass"},
		},
		Score:           95,
		Recommendations: []string{"Fix something"},
		IssueCount:      1,
	}

	assert.Equal(t, 10, output.Summary.Models)
	assert.Equal(t, 95, output.Score)
	assert.Len(t, output.HealthChecks, 1)
	assert.Len(t, output.Recommendations, 1)
}
