// Package project provides project-level linting for LeapSQL.
//
// Unlike SQL-level linting which analyzes individual statements, project-level
// linting examines the entire DAG structure, naming conventions, and cross-model
// relationships to detect architectural issues.
//
// # Rule Categories
//
// Rules are organized into categories:
//
//   - modeling (PM*): DAG structure rules like root models, fanout, dependencies
//   - structure (PS*): Naming conventions and directory organization
//   - lineage (PL*): Column-level lineage analysis like passthrough detection
//
// # Model Type Inference
//
// Models are classified into semantic types (staging, intermediate, marts)
// using a hybrid approach:
//
//  1. Frontmatter override (meta.type field)
//  2. Path-based detection (/staging/, /intermediate/, /marts/)
//  3. Prefix-based detection (stg_, int_, fct_, dim_)
//
// This allows flexibility for teams using either folder-based or prefix-based
// conventions.
//
// # Usage
//
// Create a Context from your discovered models and run the analyzer:
//
//	ctx := project.NewContext(models, graph)
//	analyzer := project.NewAnalyzer(config)
//	diagnostics := analyzer.Analyze(ctx)
package project
