package project

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// InferModelType determines the model type using hybrid logic:
//  1. Check frontmatter `type:` override (highest priority)
//  2. Check if path contains model type directory
//  3. Check if name has model type prefix
func InferModelType(model *ModelInfo) lint.ModelType {
	// 1. Frontmatter override (if present in Meta)
	if model.Meta != nil {
		if typeVal, ok := model.Meta["type"].(string); ok {
			switch strings.ToLower(typeVal) {
			case "staging":
				return lint.ModelTypeStaging
			case "intermediate":
				return lint.ModelTypeIntermediate
			case "marts":
				return lint.ModelTypeMarts
			}
		}
	}

	// 2. Path-based detection
	pathLower := strings.ToLower(model.FilePath)
	if strings.Contains(pathLower, "/staging/") {
		return lint.ModelTypeStaging
	}
	if strings.Contains(pathLower, "/intermediate/") {
		return lint.ModelTypeIntermediate
	}
	if strings.Contains(pathLower, "/marts/") {
		return lint.ModelTypeMarts
	}

	// 3. Prefix-based detection
	nameLower := strings.ToLower(model.Name)
	if strings.HasPrefix(nameLower, "stg_") {
		return lint.ModelTypeStaging
	}
	if strings.HasPrefix(nameLower, "int_") {
		return lint.ModelTypeIntermediate
	}
	if strings.HasPrefix(nameLower, "fct_") || strings.HasPrefix(nameLower, "dim_") {
		return lint.ModelTypeMarts
	}

	return lint.ModelTypeOther
}

// InferAndSetTypes infers and sets the Type field for all models in the context.
func InferAndSetTypes(models map[string]*ModelInfo) {
	for _, m := range models {
		m.Type = InferModelType(m)
	}
}
